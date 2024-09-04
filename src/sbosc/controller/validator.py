import concurrent.futures
import time
from datetime import datetime, timedelta
from queue import Queue, Empty

import MySQLdb
from MySQLdb.cursors import Cursor

from typing import TYPE_CHECKING, Generator, List

from modules.db import Database
from sbosc.exceptions import StopFlagSet
from sbosc.operations.operation import MigrationOperation

if TYPE_CHECKING:
    from sbosc.controller import Controller
from config import config
from modules.redis import RedisData
from sbosc.const import UnmatchType


class DataValidator:
    def __init__(self, controller: 'Controller'):
        self.migration_id = controller.migration_id
        self.bulk_import_batch_size = config.BULK_IMPORT_VALIDATION_BATCH_SIZE
        self.apply_dml_events_batch_size = config.APPLY_DML_EVENTS_VALIDATION_BATCH_SIZE
        self.full_dml_event_validation_interval = config.FULL_DML_EVENT_VALIDATION_INTERVAL_IN_HOURS
        self.thread_count = config.VALIDATION_THREAD_COUNT
        self.db = Database()
        self.redis_data = RedisData(self.migration_id)
        self.migration_operation: MigrationOperation = config.operation_class(self.migration_id)
        self.logger = controller.logger

        self.source_conn_pool = self.db.get_reader_connection_pool(self.thread_count)
        self.dest_conn_pool = self.db.get_reader_connection_pool(self.thread_count, host='dest')

        self.stop_flag = False

    def set_stop_flag(self):
        self.stop_flag = True

    def __handle_operational_error(self, e, range_queue, start_range, end_range):
        if e.args[0] == 2013:
            self.logger.warning("Query timeout. Retry with smaller batch size")
            range_queue.put((start_range, start_range + (end_range - start_range) // 2))
            range_queue.put((start_range + (end_range - start_range) // 2 + 1, end_range))
            time.sleep(0.1)
        else:
            self.logger.error(f"Error occurred during validation. Error: {e}")
            range_queue.put((start_range, end_range))
            time.sleep(3)

    def __validate_bulk_import_batch(self, range_queue: Queue, failed_pks):
        with self.source_conn_pool.get_connection() as source_conn, self.dest_conn_pool.get_connection() as dest_conn:
            while not range_queue.empty():
                if len(failed_pks) > 0:
                    return False

                if self.stop_flag:
                    raise StopFlagSet()

                with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
                    try:
                        batch_start_pk, batch_end_pk = range_queue.get_nowait()
                        not_imported_pks = self.migration_operation.get_not_imported_pks(
                            source_cursor, dest_cursor, batch_start_pk, batch_end_pk)
                        if not_imported_pks:
                            failed_pks.extend(not_imported_pks)
                            return False
                    except MySQLdb.OperationalError as e:
                        self.__handle_operational_error(e, range_queue, batch_start_pk, batch_end_pk)
                        source_conn.ping(True)
                        dest_conn.ping(True)
                        continue
                    except Empty:
                        self.logger.warning("Range queue is empty")
                        continue
                    self.logger.info(f"Validation succeeded for range {batch_start_pk} - {batch_end_pk}")
            return True

    def bulk_import_validation(self):
        self.logger.info("Start bulk import validation")
        metadata = self.redis_data.metadata
        range_queue = Queue()
        start_pk = 0
        while start_pk <= metadata.max_pk:
            range_queue.put((start_pk, min(start_pk + self.bulk_import_batch_size, metadata.max_pk)))
            start_pk += self.bulk_import_batch_size + 1
        failed_pks = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count) as executor:
            threads = []
            for _ in range(self.thread_count):
                threads.append(executor.submit(self.__validate_bulk_import_batch, range_queue, failed_pks))
            is_valid = all([thread.result() for thread in threads])
            if not is_valid:
                self.logger.critical(f"Failed to validate bulk import. Failed pks: {failed_pks}")
            else:
                self.logger.info("Bulk import validation succeeded")
            return is_valid

    def __get_timestamp_range(self):
        start_timestamp = None
        end_timestamp = None
        with self.db.cursor() as cursor:
            cursor: Cursor

            # Get last validated event timestamp
            cursor.execute(f'''
                SELECT last_validated_timestamp FROM {config.SBOSC_DB}.apply_dml_events_validation_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')
            if cursor.rowcount > 0:
                start_timestamp = cursor.fetchone()[0]
            else:
                cursor.execute(f'''
                    SELECT MIN(event_timestamps.min_ts) FROM (
                        SELECT MIN(event_timestamp) AS min_ts
                        FROM {config.SBOSC_DB}.inserted_pk_{self.migration_id} UNION
                        SELECT MIN(event_timestamp) AS min_ts
                        FROM {config.SBOSC_DB}.updated_pk_{self.migration_id} UNION
                        SELECT MIN(event_timestamp) AS min_ts
                        FROM {config.SBOSC_DB}.deleted_pk_{self.migration_id}
                    ) AS event_timestamps;
                ''')
                if cursor.rowcount > 0:
                    start_timestamp = cursor.fetchone()[0]

            # This ensures that all events up to the last event timestamp are all saved in the event tables
            # save_current_binlog_position are called after save_events_to_db
            cursor.execute(f'''
                SELECT last_event_timestamp FROM {config.SBOSC_DB}.event_handler_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')
            if cursor.rowcount > 0:
                end_timestamp = cursor.fetchone()[0]
        return start_timestamp, end_timestamp

    def __execute_apply_dml_events_validation_query(
            self, source_cursor, dest_cursor, table, event_pks: list, unmatched_pks: list):
        metadata = self.redis_data.metadata
        if table == 'inserted_pk':
            not_inserted_pks = self.migration_operation.get_not_inserted_pks(source_cursor, dest_cursor, event_pks)
            if not_inserted_pks:
                self.logger.warning(f"Found {len(not_inserted_pks)} unmatched inserted pks: {not_inserted_pks}")
                unmatched_pks.extend([(pk, UnmatchType.NOT_UPDATED) for pk in not_inserted_pks])
        elif table == 'updated_pk':
            not_updated_pks = self.migration_operation.get_not_updated_pks(source_cursor, dest_cursor, event_pks)
            if not_updated_pks:
                self.logger.warning(f"Found {len(not_updated_pks)} unmatched updated pks: {not_updated_pks}")
                unmatched_pks.extend([(pk, UnmatchType.NOT_UPDATED) for pk in not_updated_pks])
        elif table == 'deleted_pk':
            if event_pks:
                event_pks_str = ','.join([str(pk) for pk in event_pks])
                dest_cursor.execute(f'''
                    SELECT {metadata.pk_column} FROM {metadata.destination_db}.{metadata.destination_table} WHERE {metadata.pk_column} IN ({event_pks_str})
                ''')
                not_deleted_pks = set([row[0] for row in dest_cursor.fetchall()])
                if dest_cursor.rowcount > 0:
                    # Check if deleted pks are reinserted
                    source_cursor.execute(f'''
                        SELECT {metadata.pk_column} FROM {metadata.source_db}.{metadata.source_table} WHERE {metadata.pk_column} IN ({event_pks_str})
                    ''')
                    reinserted_pks = set([row[0] for row in source_cursor.fetchall()])
                    if reinserted_pks:
                        not_deleted_pks = not_deleted_pks - reinserted_pks
                        self.logger.warning(f"Found {len(reinserted_pks)} reinserted pks: {reinserted_pks}")
                    self.logger.warning(f"Found {len(not_deleted_pks)} unmatched deleted pks: {not_deleted_pks}")
                    unmatched_pks.extend([(pk, UnmatchType.NOT_REMOVED) for pk in not_deleted_pks])

    def __get_event_pk_batch(self, cursor, table, start_timestamp, end_timestamp) -> Generator[List[int], None, None]:
        cursor.execute(f'''
            SELECT source_pk FROM {config.SBOSC_DB}.{table}_{self.migration_id}
            WHERE event_timestamp BETWEEN {start_timestamp} AND {end_timestamp}
        ''')
        event_pks = [row[0] for row in cursor.fetchall()]
        while event_pks:
            yield event_pks[:self.apply_dml_events_batch_size]
            event_pks = event_pks[self.apply_dml_events_batch_size:]

    def __validate_apply_dml_events_batch(self, table, range_queue: Queue, unmatched_pks):
        with self.source_conn_pool.get_connection() as source_conn, self.dest_conn_pool.get_connection() as dest_conn:
            while not range_queue.empty():
                if self.stop_flag:
                    raise StopFlagSet()

                try:
                    batch_start_timestamp, batch_end_timestamp = range_queue.get_nowait()
                    self.logger.info(f"Validating {table} from {batch_start_timestamp} to {batch_end_timestamp}")
                except Empty:
                    self.logger.warning("Range queue is empty")
                    continue

                with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
                    source_cursor: Cursor
                    dest_cursor: Cursor
                    source_cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
                    dest_cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

                    source_cursor.execute(f'''
                        SELECT COUNT(1) FROM {config.SBOSC_DB}.{table}_{self.migration_id}
                        WHERE event_timestamp BETWEEN {batch_start_timestamp} AND {batch_end_timestamp}
                    ''')
                    event_count = source_cursor.fetchone()[0]
                    if event_count > self.apply_dml_events_batch_size and batch_end_timestamp > batch_start_timestamp:
                        range_queue.put((
                            batch_start_timestamp,
                            batch_start_timestamp + (batch_end_timestamp - batch_start_timestamp) // 2
                        ))
                        range_queue.put((
                            batch_start_timestamp + (batch_end_timestamp - batch_start_timestamp) // 2 + 1,
                            batch_end_timestamp
                        ))
                        continue

                    else:
                        try:
                            event_pk_batch = self.__get_event_pk_batch(
                                source_cursor, table, batch_start_timestamp, batch_end_timestamp
                            )
                            while event_pks := next(event_pk_batch, None):
                                self.__execute_apply_dml_events_validation_query(
                                    source_cursor, dest_cursor, table, event_pks, unmatched_pks
                                )
                        except MySQLdb.OperationalError as e:
                            self.__handle_operational_error(e, range_queue, batch_start_timestamp, batch_end_timestamp)
                            source_conn.ping(True)
                            dest_conn.ping(True)
                            continue

    def __validate_unmatched_pks(self):
        self.logger.info("Validating unmatched pks")
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT source_pk, unmatch_type FROM {config.SBOSC_DB}.unmatched_rows
                WHERE migration_id = {self.migration_id} LIMIT {self.apply_dml_events_batch_size}
            ''')
            if cursor.rowcount > 0:
                not_updated_pks = set()
                not_removed_pks = set()
                for pk, unmatch_type in cursor.fetchall():
                    if unmatch_type == UnmatchType.NOT_UPDATED:
                        not_updated_pks.add(pk)
                    elif unmatch_type == UnmatchType.NOT_REMOVED:
                        not_removed_pks.add(pk)
                if len(not_updated_pks) > 0:
                    matched_pks = self.migration_operation.get_rematched_updated_pks(self.db, not_updated_pks)
                    if matched_pks:
                        not_updated_pks = not_updated_pks - matched_pks
                        matched_pks_str = ','.join([str(pk) for pk in matched_pks])
                        cursor.execute(f'''
                            DELETE FROM {config.SBOSC_DB}.unmatched_rows WHERE source_pk IN ({matched_pks_str})
                            AND unmatch_type = '{UnmatchType.NOT_UPDATED}' AND migration_id = {self.migration_id}
                        ''')
                if len(not_removed_pks) > 0:
                    matched_pks = self.migration_operation.get_rematched_removed_pks(self.db, not_removed_pks)
                    if matched_pks:
                        not_removed_pks = not_removed_pks - matched_pks
                        matched_pks_str = ','.join([str(pk) for pk in matched_pks])
                        cursor.execute(f'''
                            DELETE FROM {config.SBOSC_DB}.unmatched_rows WHERE source_pk IN ({matched_pks_str})
                            AND unmatch_type = '{UnmatchType.NOT_REMOVED}' AND migration_id = {self.migration_id}
                        ''')
                self.redis_data.updated_pk_set.add(not_updated_pks - not_removed_pks)
                self.redis_data.updated_pk_set.remove(not_removed_pks)
                self.redis_data.removed_pk_set.add(not_removed_pks)

    def validate_apply_dml_events(self, start_timestamp, end_timestamp):
        unmatched_pks = []
        with self.db.cursor() as cursor:
            cursor: Cursor

            if start_timestamp <= end_timestamp:
                self.logger.info(f"Start validating DML events from {start_timestamp} to {end_timestamp}")
                for table in ['inserted_pk', 'updated_pk', 'deleted_pk']:
                    cursor.execute(f'''
                        ANALYZE TABLE {config.SBOSC_DB}.{table}_{self.migration_id}
                    ''')
                    cursor.execute(f'''
                        SELECT TABLE_ROWS FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = '{config.SBOSC_DB}' AND TABLE_NAME = '{table}_{self.migration_id}'
                    ''')
                    table_rows = cursor.fetchone()[0]

                    if table_rows > 0:
                        range_queue = Queue()
                        batch_start_timestamp = start_timestamp
                        while batch_start_timestamp <= end_timestamp:
                            batch_duration = \
                                (end_timestamp - start_timestamp) * self.apply_dml_events_batch_size // table_rows
                            batch_end_timestamp = min(batch_start_timestamp + batch_duration, end_timestamp)
                            range_queue.put((batch_start_timestamp, batch_end_timestamp))
                            batch_start_timestamp = batch_end_timestamp + 1

                        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count) as executor:
                            threads = []
                            for _ in range(self.thread_count):
                                threads.append(executor.submit(
                                    self.__validate_apply_dml_events_batch, table, range_queue, unmatched_pks))
                            for thread in threads:
                                thread.result()

            cursor.executemany(f'''
                INSERT IGNORE INTO {config.SBOSC_DB}.unmatched_rows (source_pk, migration_id, unmatch_type)
                VALUES (%s, {self.migration_id}, %s)
            ''', unmatched_pks)
            self.__validate_unmatched_pks()
            cursor.execute(
                f"SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows WHERE migration_id = {self.migration_id}")
            unmatched_rows = cursor.fetchone()[0]

        # Even though validation logic is based on data in tables following valid condition can be achieved.
        # All events are being pushed to redis in validation stage.
        return unmatched_rows == 0 and not self.stop_flag

    def apply_dml_events_validation(self):
        self.logger.info("Start apply DML events validation")

        start_timestamp, end_timestamp = self.__get_timestamp_range()
        if start_timestamp is None:
            self.logger.warning("No events found. Skipping apply DML events validation")
            return True
        elif end_timestamp is None:
            self.logger.warning("Failed to get valid end_timestamp")
            return False

        is_valid = self.validate_apply_dml_events(start_timestamp, end_timestamp)

        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                INSERT INTO {config.SBOSC_DB}.apply_dml_events_validation_status
                (migration_id, last_validated_timestamp, is_valid, created_at)
                VALUES ({self.migration_id}, {end_timestamp}, {is_valid}, NOW())
            ''')

        return is_valid

    def full_dml_event_validation(self):
        """
        :return: True if validation ran, False if validation skipped
        """
        if self.full_dml_event_validation_interval == 0:
            self.logger.info("Full DML event validation is disabled")
            return False

        self.logger.info("Start full DML event validation")

        with self.db.cursor(role='reader') as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT created_at FROM {config.SBOSC_DB}.full_dml_event_validation_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')

            if cursor.rowcount > 0:
                last_validation_time = cursor.fetchone()[0]
                if datetime.now() - last_validation_time < timedelta(hours=self.full_dml_event_validation_interval):
                    self.logger.info(
                        f"Last validation was done less than {self.full_dml_event_validation_interval} hour ago. "
                        f"Skipping full DML event validation"
                    )
                    return False

            cursor.execute(f'''
                SELECT MIN(event_timestamps.min_ts) FROM (
                    SELECT MIN(event_timestamp) AS min_ts FROM {config.SBOSC_DB}.inserted_pk_{self.migration_id} UNION
                    SELECT MIN(event_timestamp) AS min_ts FROM {config.SBOSC_DB}.updated_pk_{self.migration_id} UNION
                    SELECT MIN(event_timestamp) AS min_ts FROM {config.SBOSC_DB}.deleted_pk_{self.migration_id}
                ) AS event_timestamps;
            ''')
            if cursor.rowcount > 0:
                start_timestamp = cursor.fetchone()[0]
                if start_timestamp is None:
                    self.logger.warning("No events found. Skipping full DML event validation")
                    return False

            cursor.execute(f'''
                SELECT last_event_timestamp FROM {config.SBOSC_DB}.event_handler_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')
            if cursor.rowcount > 0:
                end_timestamp = cursor.fetchone()[0]
                if end_timestamp is None:
                    self.logger.warning("Failed to get valid end_timestamp")
                    return False

        is_valid = self.validate_apply_dml_events(start_timestamp, end_timestamp)

        with self.db.cursor() as cursor:
            cursor.execute(f'''
                INSERT INTO {config.SBOSC_DB}.full_dml_event_validation_status
                (migration_id, last_validated_timestamp, is_valid, created_at)
                VALUES ({self.migration_id}, {end_timestamp}, {is_valid}, NOW())
            ''')

        return True
