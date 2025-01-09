import time
import concurrent.futures

from datetime import datetime

from MySQLdb.cursors import Cursor

from config import config
from modules.slack import SlackClient
from sbosc.component import SBOSCComponent
from sbosc.const import Stage, ChunkStatus
from sbosc.controller.initializer import Initializer
from sbosc.controller.validator import DataValidator
from sbosc.exceptions import StopFlagSet


class Controller(SBOSCComponent):
    def __init__(self):
        self.initializer = Initializer()
        super().__init__()
        self.slack = SlackClient('SB-OSC Controller', f'{config.SOURCE_CLUSTER_ID}, {self.migration_id}')
        self.validator: DataValidator = DataValidator(self)

        self.interval = 60

    def get_migration_id(self):
        migration_id = super().get_migration_id()
        if migration_id is None:
            migration_id = self.initializer.init_migration()
        return migration_id

    def set_stop_flag(self):
        self.logger.info("Stopping controller master...")
        self.stop_flag = True
        self.validator.set_stop_flag()

    def start(self):
        self.logger.info("Starting controller master")

        stage_actions = {
            Stage.BULK_IMPORT_CHUNK_CREATION: self.create_bulk_import_chunks,
            Stage.BULK_IMPORT_VALIDATION: self.validate_bulk_import,
            Stage.ADD_INDEX: self.add_index,
            Stage.APPLY_DML_EVENTS_VALIDATION: self.apply_dml_events_validation,
            Stage.SWAP_TABLES: self.swap_tables,
        }

        while not self.stop_flag:
            current_stage = self.redis_data.current_stage
            self.logger.info(f"Current stage: {current_stage}")
            action = stage_actions.get(current_stage)
            if action:
                action()

            time.sleep(self.interval)

        # Close db connection
        self.logger.info("Controller master stopped")

    def create_bulk_import_chunks(self):
        # Remove old chunks
        self.redis_data.remove_all_chunks()

        metadata = self.redis_data.metadata
        max_pk = metadata.max_pk

        # chunk_count is determined by min_chunk_size and max_chunk_count
        # Each chunk will have min_chunk_size rows and the number of chunks should not exceed max_chunk_count
        min_chunk_size = config.MIN_CHUNK_SIZE
        max_chunk_count = config.MAX_CHUNK_COUNT  # Number of chunks means max number of worker threads
        chunk_count = min(max_pk // min_chunk_size, max_chunk_count)
        chunk_size = max_pk // chunk_count

        # Create chunks
        # Each chunk will have a range of primary key values [start_pk, end_pk]
        chunks = []
        for i in range(chunk_count):
            start_pk = i * chunk_size + 1
            end_pk = (i + 1) * chunk_size
            if i == chunk_count - 1:
                end_pk = max_pk

            chunk_id = f"{self.migration_id}-{i}"
            chunk_info = self.redis_data.get_chunk_info(chunk_id)
            chunk_info.set({
                'start_pk': start_pk,
                'end_pk': end_pk,
                'status': ChunkStatus.NOT_STARTED,
            })
            chunks.append((self.migration_id, chunk_id, start_pk, end_pk))
            self.redis_data.push_chunk(chunk_id)

        self.logger.info("Bulk import chunks created")

        # Set initial worker config
        self.redis_data.worker_config.set({
            'batch_size': config.MIN_BATCH_SIZE,
            'thread_count': config.MIN_THREAD_COUNT,
            'commit_interval': config.COMMIT_INTERVAL_IN_SECONDS,
            'revision': 0,
        })

        # Save chunk info to database
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.executemany(f'''
                INSERT INTO {config.SBOSC_DB}.chunk_info (migration_id, chunk_id, start_pk, end_pk, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            ''', chunks)

        self.redis_data.set_current_stage(Stage.BULK_IMPORT)
        self.slack.send_message(
            subtitle="Bulk import started",
            message=f"Max PK: {max_pk}\n"
                    f"Chunk count: {chunk_count}\n"
                    f"Chunk size: {chunk_size}\n"
                    f"Batch size: {config.MIN_BATCH_SIZE}\n"
                    f"Thread count: {config.MIN_THREAD_COUNT}\n"
                    f"Commit interval: {config.COMMIT_INTERVAL_IN_SECONDS}"
        )

    def validate_bulk_import(self):
        # Check if all chunks are completed
        incomplete_chunks = []

        # Restore missing chunk from database
        self.logger.info("Restoring missing chunks from database")
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT chunk_id FROM {config.SBOSC_DB}.chunk_info
                WHERE migration_id = %s
            ''', [self.migration_id])
            for chunk_id, in cursor.fetchall():
                if chunk_id not in self.redis_data.chunk_set:
                    incomplete_chunks.append(chunk_id)

        # Check if all chunks are completed
        self.logger.info("Checking if all chunks are completed")
        for chunk_id in self.redis_data.chunk_set:
            chunk_info = self.redis_data.get_chunk_info(chunk_id)
            if chunk_info.status != ChunkStatus.DONE or chunk_info.last_pk_inserted != chunk_info.end_pk:
                incomplete_chunks.append(chunk_id)

        if len(incomplete_chunks) > 0:
            self.logger.warning(f"Found incomplete chunks: {incomplete_chunks}")
            self.redis_data.push_chunk(*incomplete_chunks)
            self.redis_data.set_current_stage(Stage.BULK_IMPORT)
        else:
            # Analyze destination table
            metadata = self.redis_data.metadata
            with self.db.cursor(host='dest') as cursor:
                cursor: Cursor
                cursor.execute(f"ANALYZE TABLE {metadata.destination_db}.{metadata.destination_table}")
            self.logger.info("Finished ANALYZE TABLE on destination table")
            while not self.is_preferred_window():
                if self.stop_flag:
                    return
                self.logger.info("Waiting for preferred window")
                time.sleep(300)
            self.slack.send_message("Start validating bulk import")
            try:
                is_valid = self.validator.bulk_import_validation()
                if not is_valid:
                    self.redis_data.set_current_stage(Stage.BULK_IMPORT_VALIDATION_FAILED)
                    self.slack.send_message(message="Bulk import validation failed", color="danger")
                else:
                    if not config.DISABLE_EVENTHANDLER:
                        self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)
                    else:
                        self.redis_data.set_current_stage(Stage.DONE)
                    self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)
                    self.slack.send_message(message="Bulk import validation succeeded", color="good")
            except StopFlagSet:
                return

    def apply_dml_events_validation(self):
        self.interval = config.APPLY_DML_EVENTS_VALIDATION_INTERVAL_IN_SECONDS

        try:
            is_valid = self.validator.apply_dml_events_validation()

            if is_valid:
                full_dml_event_validation_executed = self.validator.full_dml_event_validation()
                if full_dml_event_validation_executed:  # Validation did not skip
                    # Returning will call apply_dml_events_validation again
                    # full_dml_event_validation may take a long time
                    # So, apply_dml_events_validation needs to be called again to validate the latest DML events
                    return

                if not self.is_preferred_window():
                    self.logger.info("Waiting for preferred window")
                    time.sleep(300)
                    return
                if not config.AUTO_SWAP:
                    self.logger.info("Auto swap is disabled")
                    time.sleep(config.WAIT_INTERVAL_UNTIL_AUTO_SWAP_IN_SECONDS)
                    return

                # Analyze table
                with self.db.cursor(host='dest') as cursor:
                    cursor: Cursor
                    metadata = self.redis_data.metadata
                    cursor.execute(f"ANALYZE TABLE {metadata.destination_db}.{metadata.destination_table}")
                self.logger.info("Finished ANALYZE TABLE on destination table")

                self.redis_data.set_current_stage(Stage.SWAP_TABLES)
                self.interval = 1
        except StopFlagSet:
            return

    def add_index(self):
        self.logger.info("Start creating indexes")
        metadata = self.redis_data.metadata

        finished_all_creation = False
        while not self.stop_flag:
            finished_creation = False
            with self.db.cursor() as cursor:
                cursor: Cursor

                index_info = None
                cursor.execute(f'''
                    SELECT index_name FROM {config.SBOSC_DB}.index_creation_status
                    WHERE migration_id = %s AND ended_at IS NULL AND started_at IS NOT NULL
                ''', (self.migration_id,))

                if cursor.rowcount > 0:
                    index_names = [row[0] for row in cursor.fetchall()]
                    self.slack.send_message(
                        subtitle="Found unfinished index creation", message=f"Indexes: {index_names}", color="warning")

                    while True:
                        if self.stop_flag:
                            return
                        cursor.execute(f'''
                            SELECT DISTINCT database_name, table_name, index_name FROM mysql.innodb_index_stats
                            WHERE database_name = %s AND table_name = %s
                            AND index_name IN ({','.join(['%s'] * len(index_names))})
                        ''', [metadata.destination_db, metadata.destination_table] + index_names)
                        if cursor.rowcount == len(index_names):
                            finished_creation = True
                            break
                        self.logger.info("Waiting for index creation to finish")
                        time.sleep(60)

                else:
                    cursor.execute(f'''
                        SELECT index_name, index_columns, is_unique FROM {config.SBOSC_DB}.index_creation_status
                        WHERE migration_id = %s AND ended_at IS NULL LIMIT {config.INDEX_CREATED_PER_QUERY}
                    ''', (self.migration_id,))

                    if cursor.rowcount == 0:
                        finished_all_creation = True
                        break

                    index_info = cursor.fetchall()
                    index_names = [index_name for index_name, *_ in index_info]

            if index_info and not finished_creation:
                self.logger.info(f"Creating indexes {index_names}")
                self.slack.send_message(subtitle="Start creating indexes", message=f"Indexes: {index_names}")

                # update ended_at
                started_at = datetime.now()
                with self.db.cursor() as cursor:
                    cursor: Cursor
                    cursor.executemany(f'''
                       UPDATE {config.SBOSC_DB}.index_creation_status SET started_at = %s
                       WHERE migration_id = %s AND index_name = %s
                   ''', [(started_at, self.migration_id, index_name) for index_name in index_names])

                # add index
                with self.db.cursor(host='dest') as cursor:
                    cursor: Cursor

                    # set session variables
                    if config.INNODB_DDL_BUFFER_SIZE is not None:
                        cursor.execute(f"SET SESSION innodb_ddl_buffer_size = {config.INNODB_DDL_BUFFER_SIZE}")
                        self.logger.info(f"Set innodb_ddl_buffer_size to {config.INNODB_DDL_BUFFER_SIZE}")
                    if config.INNODB_DDL_THREADS is not None:
                        cursor.execute(f"SET SESSION innodb_ddl_threads = {config.INNODB_DDL_THREADS}")
                        self.logger.info(f"Set innodb_ddl_threads to {config.INNODB_DDL_THREADS}")
                    if config.INNODB_PARALLEL_READ_THREADS is not None:
                        cursor.execute(
                            f"SET SESSION innodb_parallel_read_threads = {config.INNODB_PARALLEL_READ_THREADS}")
                        self.logger.info(f"Set innodb_parallel_read_threads to {config.INNODB_PARALLEL_READ_THREADS}")

                    cursor.execute(f'''
                        ALTER TABLE {metadata.destination_db}.{metadata.destination_table}
                        {', '.join([
                        f"ADD{' UNIQUE' if is_unique else ''} INDEX {index_name} ({index_columns})"
                        for index_name, index_columns, is_unique in index_info
                    ])}
                    ''')

                finished_creation = True

            if finished_creation:
                # update ended_at
                ended_at = datetime.now()
                with self.db.cursor() as cursor:
                    cursor: Cursor
                    cursor.executemany(f'''
                        UPDATE {config.SBOSC_DB}.index_creation_status SET ended_at = %s
                        WHERE migration_id = %s AND index_name = %s
                    ''', [(ended_at, self.migration_id, index_name) for index_name in index_names])

                self.logger.info(f"Finished creating index {index_names}")
                self.slack.send_message(
                    subtitle="Finished creating indexes", message=f"Indexes: {index_names}", color="good")

        if finished_all_creation:
            self.logger.info("Resetting worker config")
            self.redis_data.worker_config.set({
                'batch_size': config.MIN_BATCH_SIZE,
                'thread_count': 0,
                'revision': self.redis_data.worker_config.revision + 1
            })
            self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)

    def swap_tables(self):
        updated_pk_set = self.redis_data.updated_pk_set
        removed_pk_set = self.redis_data.removed_pk_set

        # Check if all updates are applied
        if len(updated_pk_set) > 0 or len(removed_pk_set) > 0 or \
                time.time() - self.redis_data.last_catchup_timestamp > 1:
            self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_VALIDATION)
            return

        self.slack.send_message("Start swapping tables")
        # Swap tables
        with self.db.cursor() as cursor:
            metadata = self.redis_data.metadata
            cursor.execute('''
                SELECT 1 FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ''', (metadata.source_db, metadata.source_table))
            destination_table_valid = False
            if cursor.rowcount > 0:
                source_table = f"{metadata.source_db}.{metadata.source_table}"
                destination_table = f"{metadata.destination_db}.{metadata.destination_table}"
                self.redis_data.set_old_source_table(
                    f"_{metadata.source_table}_old_{datetime.now().strftime('%Y%m%d')}")
                old_source_table = f"{metadata.source_db}.{self.redis_data.old_source_table}"
                cursor.execute(f"RENAME TABLE {source_table} TO {old_source_table}")
                after_rename_table_timestamp = time.time()
                cursor.execute(f"SELECT MAX({metadata.pk_column}) FROM {old_source_table}")
                final_max_id = cursor.fetchone()[0]

                with self.validator.migration_operation.override_source_table(self.redis_data.old_source_table):
                    retry_interval = 0.3
                    for _ in range(10):
                        time.sleep(retry_interval)
                        if len(updated_pk_set) == 0 and len(removed_pk_set) == 0 and \
                                self.redis_data.last_catchup_timestamp > after_rename_table_timestamp:
                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                                validation_thread = executor.submit(self.validator.apply_dml_events_validation)
                                try:
                                    destination_table_valid = validation_thread.result(timeout=3)
                                    if destination_table_valid:
                                        break
                                    else:
                                        self.logger.warning(
                                            f"Final validation failed. Retrying in {retry_interval} seconds")
                                except concurrent.futures.TimeoutError:
                                    destination_table_valid = False
                                    self.logger.warning("Final validation timed out. Downtime may be required.")
                        else:
                            self.logger.warning(f"Found unapplied DML event. Retrying in {retry_interval} seconds")

            # Source table does not exist or destination table is not valid
            if not destination_table_valid:
                self.logger.critical("Failed to validate destination table. Restoring source table name")
                cursor.execute(f"RENAME TABLE {old_source_table} TO {source_table}")
                self.redis_data.set_old_source_table(None)
                self.redis_data.set_current_stage(Stage.SWAP_TABLES_FAILED)
                self.slack.send_message("Failed to swap tables", color="danger")
                return
            else:
                cursor.execute(f"RENAME TABLE {destination_table} TO {source_table}")
                self.redis_data.set_current_stage(Stage.DONE)
                cursor.execute(f'''
                    UPDATE {config.SBOSC_DB}.migration_plan
                    SET ended_at = FROM_UNIXTIME(%s), final_max_id = %s WHERE id = %s
                ''', (after_rename_table_timestamp, final_max_id, self.migration_id))
                self.logger.info("Tables swapped")
                self.slack.send_message("Tables swapped", color="good")

        self.interval = 60
