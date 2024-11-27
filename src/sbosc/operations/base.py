import pandas as pd
from MySQLdb.cursors import Cursor

from config import config
from sbosc.operations.operation import MigrationOperation
import sbosc.operations.utils as operation_utils


class BaseOperation(MigrationOperation):
    def _insert_batch_query(self, start_pk, end_pk):
        return f'''
            INSERT INTO {self.destination_db}.{self.destination_table}({self.source_columns})
            SELECT {self.source_columns}
            FROM {self.source_db}.{self.source_table} AS source
            WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
        '''

    def insert_batch(self, db, start_pk, end_pk, upsert=False, limit=None):
        query = self._insert_batch_query(start_pk, end_pk)
        if limit:
            query = operation_utils.apply_limit(query, self.pk_column, limit)
        if upsert:
            query = operation_utils.insert_to_upsert(query, self.source_column_list)
        with db.cursor() as cursor:
            cursor.execute(query)
            return cursor

    def apply_update(self, db, updated_pks):
        with db.cursor() as cursor:
            updated_pks_str = ",".join([str(pk) for pk in updated_pks])
            query = f'''
                INSERT INTO {self.destination_db}.{self.destination_table}({self.source_columns})
                SELECT {self.source_columns}
                FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({updated_pks_str})
            '''
            query = operation_utils.insert_to_upsert(query, self.source_column_list)
            cursor.execute(query)
        return cursor

    def get_max_pk(self, db, start_pk, end_pk):
        metadata = self.redis_data.metadata
        with db.cursor(host='dest') as cursor:
            cursor: Cursor
            cursor.execute(f'''
               SELECT MAX({self.pk_column}) FROM {metadata.destination_db}.{metadata.destination_table}
               WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
           ''')
            return cursor.fetchone()[0]

    def _get_not_imported_pks_query(self, start_pk, end_pk):
        return f'''
            SELECT source.{self.pk_column} FROM {self.source_db}.{self.source_table} AS source
            LEFT JOIN {self.destination_db}.{self.destination_table} AS dest
            ON source.{self.pk_column} = dest.{self.pk_column}
            WHERE source.{self.pk_column} BETWEEN {start_pk} AND {end_pk}
            AND dest.{self.pk_column} IS NULL
        '''

    def get_not_imported_pks(self, source_cursor, dest_cursor, start_pk, end_pk):
        not_imported_pks = []
        query = self._get_not_imported_pks_query(start_pk, end_pk)
        source_cursor.execute(query)
        if source_cursor.rowcount > 0:
            not_imported_pks = [row[0] for row in source_cursor.fetchall()]
        return not_imported_pks

    def get_not_inserted_pks(self, source_cursor, dest_cursor, event_pks):
        not_inserted_pks = []
        if event_pks:
            event_pks_str = ','.join([str(pk) for pk in event_pks])
            source_cursor.execute(f'''
                SELECT source.{self.pk_column} FROM {self.source_db}.{self.source_table} AS source
                LEFT JOIN {self.destination_db}.{self.destination_table} AS dest
                ON source.{self.pk_column} = dest.{self.pk_column}
                WHERE source.{self.pk_column} IN ({event_pks_str})
                AND dest.{self.pk_column} IS NULL
            ''')
            not_inserted_pks = [row[0] for row in source_cursor.fetchall()]
        return not_inserted_pks

    def get_not_updated_pks(self, source_cursor, dest_cursor, event_pks):
        not_updated_pks = []
        if event_pks:
            event_pks_str = ','.join([str(pk) for pk in event_pks])
            source_cursor.execute(f'''
              SELECT combined.{self.pk_column}
                FROM (
                    SELECT {self.source_columns}, 'source' AS table_type
                    FROM {self.source_db}.{self.source_table}
                    WHERE {self.pk_column} IN ({event_pks_str})
                    UNION ALL
                    SELECT {self.source_columns}, 'destination' AS table_type
                    FROM {self.destination_db}.{self.destination_table}
                    WHERE {self.pk_column} IN ({event_pks_str})
                ) AS combined
                GROUP BY {self.source_columns}
                HAVING COUNT(1) = 1 AND SUM(table_type = 'source') = 1
            ''')
            not_updated_pks = [row[0] for row in source_cursor.fetchall()]
        return not_updated_pks

    def get_rematched_updated_pks(self, db, not_updated_pks):
        not_updated_pks_str = ','.join([str(pk) for pk in not_updated_pks])
        with db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT combined.{self.pk_column} FROM (
                    SELECT {self.source_columns} FROM {self.source_db}.{self.source_table}
                    WHERE {self.pk_column} IN ({not_updated_pks_str}) UNION ALL
                    SELECT {self.source_columns} FROM {self.destination_db}.{self.destination_table}
                    WHERE {self.pk_column} IN ({not_updated_pks_str})
                ) AS combined GROUP BY {self.source_columns} HAVING COUNT(*) = 2
            ''')
            rematched_pks = set([row[0] for row in cursor.fetchall()])
            # add deleted pks
            cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({not_updated_pks_str})
            ''')
            remaining_pks = set([row[0] for row in cursor.fetchall()])
            deleted_pks = not_updated_pks - remaining_pks
            return rematched_pks | deleted_pks

    def get_rematched_removed_pks(self, db, not_removed_pks):
        not_removed_pks_str = ','.join([str(pk) for pk in not_removed_pks])
        with db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT source_pk FROM {config.SBOSC_DB}.unmatched_rows WHERE source_pk NOT IN (
                    SELECT {self.pk_column} FROM {self.destination_db}.{self.destination_table}
                    WHERE {self.pk_column} IN ({not_removed_pks_str})
                ) AND source_pk IN ({not_removed_pks_str}) AND migration_id = {self.migration_id}
            ''')
            rematched_pks = set([row[0] for row in cursor.fetchall()])
            # add reinserted pks
            cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({not_removed_pks_str})
            ''')
            reinserted_pks = set([row[0] for row in cursor.fetchall()])
            return rematched_pks | reinserted_pks


class CrossClusterBaseOperation(MigrationOperation):
    def _select_batch_query(self, start_pk, end_pk):
        return f'''
            SELECT {self.source_columns}
            FROM {self.source_db}.{self.source_table} AS source
            WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
        '''

    def insert_batch(self, db, start_pk, end_pk, upsert=False, limit=None):
        select_batch_query = self._select_batch_query(start_pk, end_pk)
        if limit:
            select_batch_query = operation_utils.apply_limit(select_batch_query, self.pk_column, limit)
        with db.cursor(host='source', role='reader') as cursor:
            cursor.execute(select_batch_query)
            rows = cursor.fetchall()
        if rows:
            insert_batch_query = f'''
                INSERT INTO {self.destination_db}.{self.destination_table}({self.source_columns})
                VALUES ({','.join(['%s'] * len(self.source_column_list))})
            '''
            if upsert:
                insert_batch_query = operation_utils.insert_to_upsert(insert_batch_query, self.source_column_list)
            with db.cursor(host='dest', role='writer') as cursor:
                cursor.executemany(insert_batch_query, rows)
                return cursor
        else:
            return cursor

    def apply_update(self, db, updated_pks):
        with db.cursor(host='source', role='reader') as cursor:
            updated_pks_str = ",".join([str(pk) for pk in updated_pks])
            cursor: Cursor
            cursor.execute(f'''
                SELECT {self.source_columns} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({updated_pks_str})
            ''')
            rows = cursor.fetchall()
        if rows:
            with db.cursor(host='dest', role='writer') as cursor:
                query = f'''
                    INSERT INTO {self.destination_db}.{self.destination_table}({self.source_columns})
                    VALUES ({','.join(['%s'] * len(self.source_column_list))})
                '''
                query = operation_utils.insert_to_upsert(query, self.source_column_list)
                cursor.executemany(query, rows)
                return cursor
        else:
            return cursor

    def get_max_pk(self, db, start_pk, end_pk):
        metadata = self.redis_data.metadata
        with db.cursor(host='dest') as cursor:
            cursor: Cursor
            cursor.execute(f'''
               SELECT MAX({self.pk_column}) FROM {metadata.destination_db}.{metadata.destination_table}
               WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
           ''')
            return cursor.fetchone()[0]

    def get_not_imported_pks(self, source_cursor, dest_cursor, start_pk, end_pk):
        source_cursor.execute(f'''
            SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
            WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
        ''')
        source_pks = [row[0] for row in source_cursor.fetchall()]
        dest_cursor.execute(f'''
            SELECT {self.pk_column} FROM {self.destination_db}.{self.destination_table}
            WHERE {self.pk_column} BETWEEN {start_pk} AND {end_pk}
        ''')
        dest_pks = [row[0] for row in dest_cursor.fetchall()]
        return list(set(source_pks) - set(dest_pks))

    def get_not_inserted_pks(self, source_cursor, dest_cursor, event_pks):
        not_inserted_pks = []
        if event_pks:
            event_pks_str = ','.join([str(pk) for pk in event_pks])
            source_cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({event_pks_str})
            ''')
            source_pks = [row[0] for row in source_cursor.fetchall()]
            dest_cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.destination_db}.{self.destination_table}
                WHERE {self.pk_column} IN ({event_pks_str})
            ''')
            dest_pks = [row[0] for row in dest_cursor.fetchall()]
            not_inserted_pks = list(set(source_pks) - set(dest_pks))
        return not_inserted_pks

    def get_not_updated_pks(self, source_cursor, dest_cursor, event_pks):
        not_updated_pks = []
        if event_pks:
            event_pks_str = ','.join([str(pk) for pk in event_pks])
            source_cursor.execute(f'''
                SELECT {self.source_columns} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({event_pks_str})
            ''')
            source_df = pd.DataFrame(source_cursor.fetchall(), columns=[c[0] for c in source_cursor.description])
            dest_cursor.execute(f'''
                SELECT {self.source_columns} FROM {self.destination_db}.{self.destination_table}
                WHERE {self.pk_column} IN ({event_pks_str})
            ''')
            dest_df = pd.DataFrame(dest_cursor.fetchall(), columns=[c[0] for c in dest_cursor.description])

            dest_df = dest_df[source_df.columns]
            source_df.set_index(self.pk_column.strip('`'), inplace=True)
            dest_df.set_index(self.pk_column.strip('`'), inplace=True)
            common_index = dest_df.index.intersection(source_df.index)
            source_df = source_df.loc[common_index]
            dest_df = dest_df.loc[common_index]

            comparison_df = source_df.compare(dest_df).dropna(how='all')
            not_updated_pks = comparison_df.index.unique().tolist()
        return not_updated_pks

    def get_rematched_updated_pks(self, db, not_updated_pks):
        not_updated_pks_str = ','.join([str(pk) for pk in not_updated_pks])
        # Get rematched_pks
        try:
            with db.cursor(host='source', role='reader') as cursor:
                cursor: Cursor
                cursor.execute(f'''
                    SELECT {self.source_columns} FROM {self.source_db}.{self.source_table}
                    WHERE {self.pk_column} IN ({not_updated_pks_str})
                ''')
                source_df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])
            with db.cursor(host='dest', role='reader') as cursor:
                cursor: Cursor
                cursor.execute(f'''
                    SELECT {self.source_columns} FROM {self.destination_db}.{self.destination_table}
                    WHERE {self.pk_column} IN ({not_updated_pks_str})
                ''')
                dest_df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])
            dest_df = dest_df.astype(source_df.dtypes.to_dict())
            merged_df = source_df.merge(dest_df, how='inner', on=source_df.columns.tolist(), indicator=True)
            rematched_pks = set(merged_df[merged_df['_merge'] == 'both'][self.pk_column.strip('`')].tolist())
        except pd.errors.IntCastingNaNError:
            rematched_pks = set()
        # add deleted pks
        with db.cursor(host='source', role='reader') as cursor:
            cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({not_updated_pks_str})
            ''')
            remaining_pks = set([row[0] for row in cursor.fetchall()])
        deleted_pks = not_updated_pks - remaining_pks
        return rematched_pks | deleted_pks

    def get_rematched_removed_pks(self, db, not_removed_pks):
        not_removed_pks_str = ','.join([str(pk) for pk in not_removed_pks])
        with db.cursor(host='dest', role='reader') as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.destination_db}.{self.destination_table}
                WHERE {self.pk_column} IN ({not_removed_pks_str})
            ''')
            still_not_removed_pks_str = ','.join([str(row[0]) for row in cursor.fetchall()])
        with db.cursor(host='source', role='reader') as cursor:
            cursor: Cursor
            query = f'''
                SELECT source_pk FROM {config.SBOSC_DB}.unmatched_rows
                WHERE source_pk IN ({not_removed_pks_str}) AND migration_id = {self.migration_id}
            '''
            if still_not_removed_pks_str:
                query += f" AND source_pk NOT IN ({still_not_removed_pks_str})"
            cursor.execute(query)
            rematched_pks = set([row[0] for row in cursor.fetchall()])
            # add reinserted pks
            cursor.execute(f'''
                SELECT {self.pk_column} FROM {self.source_db}.{self.source_table}
                WHERE {self.pk_column} IN ({not_removed_pks_str})
            ''')
            reinserted_pks = set([row[0] for row in cursor.fetchall()])
            return rematched_pks | reinserted_pks
