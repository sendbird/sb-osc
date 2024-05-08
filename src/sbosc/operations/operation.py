from abc import abstractmethod
from contextlib import contextmanager
from typing import Literal

from MySQLdb.cursors import Cursor

from modules.db import Database
from modules.redis import RedisData


class MigrationOperation:
    """Abstract class for migration operations."""

    def __init__(self, migration_id):
        self.migration_id = migration_id
        self.redis_data = RedisData(migration_id)

        metadata = self.redis_data.metadata
        self.source_db = metadata.source_db
        self.source_table = metadata.source_table
        self.destination_db = metadata.destination_db
        self.destination_table = metadata.destination_table
        self.source_columns: str = metadata.source_columns
        self.source_column_list: list = metadata.source_columns.split(',')
        self.start_datetime = metadata.start_datetime

    @abstractmethod
    def insert_batch(self, db: Database, start_pk: int, end_pk: int, upsert=False, limit=None) -> Cursor:
        """
        Executes a query to insert a batch of records from the source table into the destination table.
        Used by the worker to insert a batch of records into the destination table.
        """
        pass

    @abstractmethod
    def apply_update(self, db: Database, updated_pks: list) -> Cursor:
        """
        Executes a query to apply DML update (insert) events to the destination table.
        """
        pass

    @abstractmethod
    def get_not_imported_pks(self, source_cursor: Cursor, dest_cursor: Cursor, start_pk: int, end_pk: int) -> list:
        """
        Returns a list of primary keys that have not been imported into the destination table.
        Used in BULK_IMPORT_VALIDATION stage to validate that all records have been imported.
        """
        pass

    def _get_event_pks(
            self, cursor: Cursor, event_type: Literal['insert', 'update'], start_timestamp, end_timestamp):
        table_names = {
            'insert': f'inserted_pk_{self.migration_id}',
            'update': f'updated_pk_{self.migration_id}'
        }
        cursor.execute(f'''
            SELECT source_pk FROM sbosc.{table_names[event_type]}
            WHERE event_timestamp BETWEEN {start_timestamp} AND {end_timestamp}
        ''')
        return ','.join([str(row[0]) for row in cursor.fetchall()])

    @abstractmethod
    def get_not_inserted_pks(self, source_cursor: Cursor, dest_cursor: Cursor, start_timestamp, end_timestamp):
        """
        Returns a list of primary keys that have not been inserted into the destination table.
        Used in APPLY_DML_EVENTS_VALIDATION stage to validate that all inserts have been applied.
        """
        pass

    @abstractmethod
    def get_not_updated_pks(self, source_cursor: Cursor, dest_cursor: Cursor, start_timestamp, end_timestamp):
        """
        Returns a list of primary keys that have not been updated in the destination table.
        Used in APPLY_DML_EVENTS_VALIDATION stage to validate that all updates have been applied.
        """
        pass

    @abstractmethod
    def get_rematched_updated_pks(self, db: Database, not_updated_pks: set) -> set:
        """
        Returns a list of primary keys that have been updated in the destination table after first unmatch.
        Used in APPLY_DML_EVENTS_VALIDATION stage to validate unmatched pks.
        """
        pass

    @abstractmethod
    def get_rematched_removed_pks(self, db: Database, not_removed_pks: set) -> set:
        """
        Returns a list of primary keys that have been updated in the destination table after first unmatch.
        Used in APPLY_DML_EVENTS_VALIDATION stage to validate unmatched pks.
        """
        pass

    @contextmanager
    def override_source_table(self, table_name: str):
        """Context manager to override the source table name."""
        source_table = self.source_table
        if table_name:
            self.source_table = table_name
        yield
        self.source_table = source_table
