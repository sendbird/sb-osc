import signal
import time

import MySQLdb
from MySQLdb.cursors import Cursor

from config import config
from modules.db import Database
from modules.redis import RedisData
from modules.logger import get_logger


class SBOSCComponent:
    def __init__(self):
        self.db: Database = Database()

        self.migration_id = self.get_migration_id()
        if self.migration_id is None:
            print("Migration ID not found. Exiting.")
            time.sleep(60)
            return
        self.redis_data: RedisData = RedisData(self.migration_id)
        if self.redis_data.current_stage is None:
            print("Current stage not set. Exiting.")
            time.sleep(60)
            return
        self.logger = get_logger({
            "dbclusteridentifier": config.SOURCE_CLUSTER_ID,
            "migration_id": self.migration_id
        })

        self.stop_flag = False

    def set_stop_flag(self):
        self.stop_flag = True

    def is_preferred_window(self):
        pass

    def get_migration_id(self):
        if not config.SOURCE_TABLE or not config.DESTINATION_TABLE:
            raise Exception("Migration not configured")

        with self.db.cursor() as cursor:
            cursor: Cursor
            try:
                cursor.execute('''
                    SELECT id FROM sbosc.migration_plan
                    WHERE source_cluster_id = %s AND source_db = %s AND source_table = %s
                    AND destination_cluster_id = %s AND destination_db = %s AND destination_table = %s
                ''', (
                    config.SOURCE_CLUSTER_ID,
                    config.SOURCE_DB,
                    config.SOURCE_TABLE,
                    config.DESTINATION_CLUSTER_ID,
                    config.DESTINATION_DB,
                    config.DESTINATION_TABLE
                ))
                return cursor.fetchone()[0] if cursor.rowcount > 0 else None

            except MySQLdb.ProgrammingError as e:
                if e.args[0] == 1146:  # database or table doesn't exist
                    print(f"Table not found. {e.args[1]}")
                    return None
                raise e


def start_component(component_class):
    component = component_class()

    def signal_handler(sig, frame):
        component.set_stop_flag()

    # Handle SIGINT and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    component.start()
