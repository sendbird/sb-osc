from datetime import datetime

from MySQLdb.cursors import Cursor

from config import config
from modules.slack import SlackClient
from sbosc.const import Stage
from modules.db import Database
from modules.redis import RedisData
from modules.logger import get_logger

REQUIRED_TABLES = [
    # Controller
    "migration_plan", "chunk_info",
    "apply_dml_events_status", "index_creation_status",
    "apply_dml_events_validation_status", "full_dml_event_validation_status", "unmatched_rows",
    # EventHandler
    "event_handler_status"
]


class Initializer:
    def __init__(self):
        self.db = Database()
        self.logger = get_logger({"dbclusteridentifier": config.SOURCE_CLUSTER_ID})

    def check_database_setup(self):
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute("SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'sbosc'")
            if cursor.rowcount == 0:
                self.logger.info("SB-OSC database not found")
                return False
            cursor.execute('''
                   SELECT 1 FROM information_schema.TABLES
                   WHERE TABLE_SCHEMA = 'sbosc' AND TABLE_NAME IN (%s)
               ''' % ','.join(['%s'] * len(REQUIRED_TABLES)), REQUIRED_TABLES)
            if cursor.rowcount != len(REQUIRED_TABLES):
                self.logger.info("Required tables not found")
                return False
            return True

    def setup_database(self):
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute("CREATE DATABASE IF NOT EXISTS sbosc;")
            self.logger.info("Database created")

            # Controller tables
            cursor.execute("USE sbosc;")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS migration_plan (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    source_cluster_id varchar(128),
                    source_db varchar(128),
                    source_table varchar(128),
                    destination_cluster_id varchar(128),
                    destination_db varchar(128),
                    destination_table varchar(128),
                    detail text,
                    created_at datetime,
                    final_max_id bigint,
                    ended_at datetime
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Migration plan table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chunk_info (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    chunk_id varchar(128),
                    start_pk bigint,
                    end_pk bigint,
                    created_at datetime,
                    KEY `idx_chunk_info_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Chunk info table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS apply_dml_events_status (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    last_loaded_timestamp bigint,
                    created_at datetime,
                    KEY `idx_apply_dml_events_status_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Apply DML event status table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS index_creation_status (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    index_name varchar(128),
                    index_columns varchar(128),
                    is_unique bool,
                    started_at datetime,
                    ended_at datetime,
                    created_at datetime,
                    KEY `idx_index_creation_status_migration_id_ended_at` (`migration_id`, `ended_at`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Index creation status table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS apply_dml_events_validation_status (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    last_validated_timestamp bigint,
                    is_valid bool,
                    created_at datetime,
                    KEY `idx_apply_dml_events_validation_status_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Apply DML event validation status table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS full_dml_event_validation_status (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    last_validated_timestamp bigint,
                    is_valid bool,
                    created_at datetime,
                    KEY `idx_full_dml_event_validation_status_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Full DML event validation status table created")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS unmatched_rows (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    source_pk bigint,
                    migration_id int,
                    unmatch_type varchar(128),
                    KEY `idx_unmatched_rows_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Unmatched rows table created")

            # EventHandler tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS event_handler_status (
                    id int PRIMARY KEY AUTO_INCREMENT,
                    migration_id int,
                    log_file varchar(128),
                    log_pos bigint,
                    last_event_timestamp bigint,
                    created_at datetime,
                    KEY `idx_event_handler_status_migration_id` (`migration_id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            ''')
            self.logger.info("Event handler status table created")

    def fetch_metadata(self, migration_id):
        redis_data = RedisData(migration_id)
        metadata = redis_data.metadata

        # Config data
        metadata.set({
            "source_db": config.SOURCE_DB,
            "source_table": config.SOURCE_TABLE,
            "destination_db": config.DESTINATION_DB,
            "destination_table": config.DESTINATION_TABLE,
        })
        self.logger.info("Saved migration plan data to Redis")

        with self.db.cursor() as cursor:
            # Column schema
            cursor.execute('''
                SELECT GROUP_CONCAT('`', COLUMN_NAME, '`') FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ''', (metadata.source_db, metadata.source_table))
            metadata.source_columns = cursor.fetchone()[0]
            self.logger.info("Saved source column schema to Redis")

            # Get max id
            cursor.execute("SELECT MAX(id) FROM %s.%s" % (metadata.source_db, metadata.source_table))
            max_id = cursor.fetchone()[0]
            metadata.max_id = max_id
            self.logger.info("Saved total rows to Redis")

        metadata.start_datetime = datetime.now()
        redis_data.set_current_stage(Stage.START_EVENT_HANDLER)

    def init_migration(self):
        if not self.check_database_setup():
            self.setup_database()

        with self.db.cursor() as cursor:
            # Insert migration plan
            cursor: Cursor
            cursor.execute('''
                INSERT INTO sbosc.migration_plan
                (source_cluster_id, source_db, source_table,
                destination_cluster_id, destination_db, destination_table, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ''', (
                config.SOURCE_CLUSTER_ID,
                config.SOURCE_DB,
                config.SOURCE_TABLE,
                config.DESTINATION_CLUSTER_ID,
                config.DESTINATION_DB,
                config.DESTINATION_TABLE
            ))
            self.logger.info("Migration plan created")
            migration_id = cursor.lastrowid

            # Insert index creation status
            for index in config.INDEXES:
                cursor.execute('''
                    INSERT INTO sbosc.index_creation_status
                    (migration_id, index_name, index_columns, is_unique, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                ''', (
                    migration_id,
                    index.name,
                    index.columns,
                    index.unique
                ))

            # DML log tables
            dml_log_tables = [f'{table}_{migration_id}' for table in ['inserted_pk', 'updated_pk', 'deleted_pk']]
            for table in dml_log_tables:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS sbosc.{table} (
                        source_pk bigint PRIMARY KEY,
                        event_timestamp bigint,
                        KEY `idx_{table}_event_timestamp` (`event_timestamp`)
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                ''')
            self.logger.info("DML log tables created")

            # Fetch metadata
            self.fetch_metadata(migration_id)

            slack = SlackClient("SB-OSC Controller", f'{config.SOURCE_CLUSTER_ID}, {migration_id}')
            slack.send_message(
                subtitle=f"Finished initializing migration. Migration ID: {migration_id}",
                message=f"Source DB: {config.SOURCE_DB}\n"
                        f"Source table: {config.SOURCE_TABLE}\n"
                        f"Destination table: {config.DESTINATION_TABLE}",
                color="good"
            )

        return migration_id
