import time

from MySQLdb.cursors import Cursor

from typing import TYPE_CHECKING

from modules.db import Database
from modules.redis import RedisData

if TYPE_CHECKING:
    from sbosc.eventhandler.eventhandler import EventHandler
from config import config


class EventLoader:
    def __init__(self, event_handler: 'EventHandler'):
        self.migration_id = event_handler.migration_id
        self.db = Database()
        self.redis_data = RedisData(self.migration_id)
        self.logger = event_handler.logger

        self.last_loaded_timestamp = 1
        self.batch_duration = config.EVENT_BATCH_DURATION

        self.stop_flag = False

    def set_stop_flag(self):
        self.logger.info("Stopping event loader")
        self.stop_flag = True

    def start(self):
        try:
            while not self.stop_flag:
                self.load_events_from_db()
                time.sleep(self.interval)
        except Exception as e:
            self.logger.error(f"Error in event loader: {e}")
            raise e

    @property
    def event_set_length(self):
        return len(self.redis_data.updated_pk_set) + len(self.redis_data.removed_pk_set)

    @property
    def interval(self):
        event_set_length = self.event_set_length
        if event_set_length > 100000:
            return 60
        elif event_set_length > 10000:
            return 10
        else:
            return 1

    def get_start_timestamp(self):
        with self.db.cursor() as cursor:
            cursor: Cursor

            # Get last loaded event timestamp
            cursor.execute(f'''
                SELECT last_loaded_timestamp FROM sbosc.apply_dml_events_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')
            if cursor.rowcount > 0:
                start_timestamp = cursor.fetchone()[0]
            else:
                cursor.execute(f'''
                    SELECT MIN(event_timestamps.min_ts) FROM (
                        SELECT MIN(event_timestamp) AS min_ts FROM sbosc.inserted_pk_{self.migration_id} UNION
                        SELECT MIN(event_timestamp) AS min_ts FROM sbosc.updated_pk_{self.migration_id} UNION
                        SELECT MIN(event_timestamp) AS min_ts FROM sbosc.deleted_pk_{self.migration_id}
                    ) AS event_timestamps;
                ''')
                if cursor.rowcount > 0:
                    start_timestamp = cursor.fetchone()[0] or 0
                else:
                    start_timestamp = 0
            return start_timestamp

    def get_max_timestamp(self):
        with self.db.cursor() as cursor:
            cursor: Cursor

            cursor.execute(f'''
                SELECT MAX(event_timestamps.max_ts) FROM (
                    SELECT MAX(event_timestamp) AS max_ts FROM sbosc.inserted_pk_{self.migration_id} UNION
                    SELECT MAX(event_timestamp) AS max_ts FROM sbosc.updated_pk_{self.migration_id} UNION
                    SELECT MAX(event_timestamp) AS max_ts FROM sbosc.deleted_pk_{self.migration_id}
                ) AS event_timestamps;
            ''')
            if cursor.rowcount > 0:
                max_timestamp = cursor.fetchone()[0] or 0
            else:
                max_timestamp = 0
            return max_timestamp

    def get_end_timestamp(self, start_timestamp):
        found_end_timestamp = False
        with self.db.cursor() as cursor:
            cursor: Cursor

            while not found_end_timestamp and self.batch_duration > 0 and not self.stop_flag:
                cursor.execute(f'''
                    SELECT COUNT(1) FROM sbosc.inserted_pk_{self.migration_id}
                    WHERE event_timestamp BETWEEN {start_timestamp} AND {start_timestamp + self.batch_duration}
                ''')
                inserted_count = cursor.fetchone()[0]
                cursor.execute(f'''
                    SELECT COUNT(1) FROM sbosc.updated_pk_{self.migration_id}
                    WHERE event_timestamp BETWEEN {start_timestamp} AND {start_timestamp + self.batch_duration}
                ''')
                updated_count = cursor.fetchone()[0]
                cursor.execute(f'''
                    SELECT COUNT(1) FROM sbosc.deleted_pk_{self.migration_id}
                    WHERE event_timestamp BETWEEN {start_timestamp} AND {start_timestamp + self.batch_duration}
                ''')
                deleted_count = cursor.fetchone()[0]

                max_event_count = max(inserted_count, updated_count, deleted_count)
                if max_event_count > config.PK_SET_MAX_SIZE:
                    self.batch_duration //= 2
                    self.logger.warning(f"Batch is too large, reducing duration to {self.batch_duration} seconds")
                elif max_event_count == 0:
                    self.batch_duration *= 2
                    self.logger.warning(
                        f"No events found in timestamp range, Batch duration increased to {self.batch_duration}")
                else:
                    found_end_timestamp = True

        return start_timestamp + self.batch_duration

    def get_pk_batch(self, start_timestamp, end_timestamp) -> (set, set, int):
        updated_pks = set()
        removed_pks = set()

        self.logger.info(
            f"Loading events from database. Start timestamp: {start_timestamp}, end timestamp: {end_timestamp}")
        with self.db.cursor() as cursor:
            cursor: Cursor

            # Updated pks
            cursor.execute(f'''
                SELECT updated_pks.source_pk, updated_pks.event_timestamp FROM (
                    SELECT source_pk, event_timestamp FROM sbosc.inserted_pk_{self.migration_id}
                    WHERE event_timestamp BETWEEN {start_timestamp} AND {end_timestamp}
                    UNION
                    SELECT source_pk, event_timestamp FROM sbosc.updated_pk_{self.migration_id}
                    WHERE event_timestamp BETWEEN {start_timestamp} AND {end_timestamp}
                ) AS updated_pks
            ''')
            max_timestamp = start_timestamp
            for source_pk, event_timestamp in cursor.fetchall():
                if event_timestamp > max_timestamp:
                    max_timestamp = event_timestamp
                updated_pks.add(source_pk)

            # Removed pks
            cursor.execute(f'''
                SELECT source_pk, event_timestamp FROM sbosc.deleted_pk_{self.migration_id}
                WHERE event_timestamp BETWEEN {start_timestamp} AND {end_timestamp}
            ''')
            for source_pk, event_timestamp in cursor.fetchall():
                if event_timestamp > max_timestamp:
                    max_timestamp = event_timestamp
                removed_pks.add(source_pk)

            return updated_pks, removed_pks, max_timestamp

    def load_events_from_db(self):
        updated_pk_set = self.redis_data.updated_pk_set
        removed_pk_set = self.redis_data.removed_pk_set
        if self.event_set_length < config.PK_SET_MAX_SIZE:
            # Load events from database

            start_timestamp = self.get_start_timestamp()
            max_timestamp = self.get_max_timestamp()
            if start_timestamp == 0 or start_timestamp > max_timestamp:
                self.logger.info("No events to load")
                if self.last_loaded_timestamp == 1:
                    # Set last loaded timestamp to initial timestamp
                    # By updating it here, eventhandler can move to next stage
                    # Also it will prevent eventhandler from moving to next stage to early even before loading events
                    with self.db.cursor(role='reader') as cursor:
                        cursor: Cursor
                        cursor.execute('''
                            SELECT last_event_timestamp FROM sbosc.event_handler_status
                            WHERE migration_id = %s ORDER BY id LIMIT 1
                        ''', (self.migration_id,))
                        if cursor.rowcount > 0:
                            self.last_loaded_timestamp = cursor.fetchone()[0]

                time.sleep(10)
                return

            next_timestamp = self.get_end_timestamp(start_timestamp)
            if start_timestamp == next_timestamp:
                self.logger.warning("Batch duration is 0, please resize PK_SET_MAX_SIZE")
                time.sleep(10)
                return

            updated_pks, removed_pks, max_timestamp = self.get_pk_batch(start_timestamp, next_timestamp)
            updated_pk_set.add(updated_pks - removed_pks)
            updated_pk_set.remove(removed_pks)
            removed_pk_set.add(removed_pks)
            # Save last loaded event timestamp
            with self.db.cursor() as cursor:
                cursor: Cursor
                cursor.execute('''
                    INSERT INTO sbosc.apply_dml_events_status (migration_id, last_loaded_timestamp, created_at)
                    VALUES (%s, %s, NOW())
                ''', (self.migration_id, max_timestamp))
                self.last_loaded_timestamp = max_timestamp
                self.logger.info(f"Loaded events from database. Last loaded timestamp: {self.last_loaded_timestamp}")
