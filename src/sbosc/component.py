import signal
import time
from datetime import datetime

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

    @staticmethod
    def is_preferred_window():
        current_time = datetime.now().time()
        start_time_str, end_time_str = config.PREFERRED_WINDOW.split('-')
        start_time = datetime.strptime(start_time_str, '%H:%M').time()
        end_time = datetime.strptime(end_time_str, '%H:%M').time()
        if start_time >= end_time:
            return start_time <= current_time or current_time <= end_time
        return start_time <= current_time <= end_time

    def get_migration_id(self):
        if not config.SOURCE_TABLE or not config.DESTINATION_TABLE:
            raise Exception("Migration not configured")

        with self.db.cursor() as cursor:
            cursor: Cursor
            try:
                cursor.execute(f'''
                    SELECT id FROM {config.SBOSC_DB}.migration_plan
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

            except MySQLdb.OperationalError as e:
                if e.args[0] == 1049:
                    print(f"Database not found. {e.args[1]}")
                    return None

            except MySQLdb.ProgrammingError as e:
                if e.args[0] == 1146:
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
