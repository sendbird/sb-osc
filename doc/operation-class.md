# Operation Class
Operation class is a feature that allows users to customize queries for specific use cases such as data retention, table redesign, and more.  

SB-OSC provides two default operation classes. `BaseOperation` is the default operation class that is used for normal schema migration. It copies all columns and records from the source table to the destination table.   

`CrossClusterOperation` is an operation class that allows replication across different Aurora clusters. Instead of `INSERT INTO ... SELECT ...` it selects from source cluster and inserts into destination cluster with two separate connections. This can be used in various scenarios such as cross-region replication, cross-account replication, clone cluster replication, etc.

You can create your own operation class by inheriting `BaseOperation` and overriding its methods. If you pass the operation class name to the `operation_class` parameter in the migration configuration, SB-OSC detect any operation class defined below `src/sbosc/opeartion` directory and use it for the migration process.  

## Example

### BaseOperation
```python
from sbosc.operations.base import BaseOperation


class MessageRetentionOperation(BaseOperation):
    def _insert_batch_query(self, start_pk, end_pk):
        return f"""
            INSERT INTO {self.source_db}.{self.destination_table}({self.source_columns})
            SELECT {self.source_columns}
            FROM {self.source_db}.{self.source_table} AS source
            WHERE source.id BETWEEN {start_pk} AND {end_pk}
            AND source.ts > DATE_SUB(NOW(), INTERVAL 30 DAY)
        """
    def _get_not_imported_pks_query(self, start_pk, end_pk):
        return f'''
            SELECT source.id FROM {self.source_db}.{self.source_table} AS source
            LEFT JOIN {self.source_db}.{self.destination_table} AS dest ON source.id = dest.id
            WHERE source.id BETWEEN {start_pk} AND {end_pk}
            AND source.ts > DATE_SUB(NOW(), INTERVAL 30 DAY)
            AND dest.id IS NULL
        '''
```

### CrossClusterOperation
```python
class CrossClusterMessageRetentionOperation(CrossClusterBaseOperation):
    def _select_batch_query(self, start_pk, end_pk):
        return f'''
            SELECT {self.source_columns} FROM {self.source_db}.{self.source_table}
            WHERE id BETWEEN {start_pk} AND {end_pk}
            AND source.ts > DATE_SUB(NOW(), INTERVAL 30 DAY)
        '''

    def get_not_imported_pks(self, source_cursor, dest_cursor, start_pk, end_pk):
        source_cursor.execute(f'''
            SELECT id FROM {self.source_db}.{self.source_table}
            WHERE id BETWEEN {start_pk} AND {end_pk}
            AND source.ts > DATE_SUB(NOW(), INTERVAL 30 DAY)
        ''')
        source_pks = [row[0] for row in source_cursor.fetchall()]
        dest_cursor.execute(f'''
            SELECT id FROM {self.destination_db}.{self.destination_table}
            WHERE id BETWEEN {start_pk} AND {end_pk}
            AND source.ts > DATE_SUB(NOW(), INTERVAL 30 DAY)
        ''')
        dest_pks = [row[0] for row in dest_cursor.fetchall()]
        return list(set(source_pks) - set(dest_pks))
```
