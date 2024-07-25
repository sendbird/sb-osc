import random
import threading
import time
from datetime import datetime
from threading import Thread

import numpy as np
import pandas as pd
import pytest

from config import config
from config.config import IndexConfig
from sbosc.const import Stage, ChunkStatus
from sbosc.controller import Controller
from modules.redis import RedisData

TABLE_SIZE = 10000


############
# Fixtures #
############

@pytest.fixture
def setup_table(sqlalchemy_engine, cursor, request):
    param = request.param if hasattr(request, 'param') else ''
    cursor.execute(f"DROP TABLE IF EXISTS {config.SOURCE_DB}.{config.SOURCE_TABLE}")
    if param == 'with_data':
        df = pd.DataFrame(np.random.choice(['a', 'b', 'c'], size=(TABLE_SIZE, 3)), columns=['A', 'B', 'C'])
        df['id'] = range(1, 1 + len(df))
        df.to_sql(config.SOURCE_TABLE, sqlalchemy_engine, if_exists='replace', index=False, schema=config.SOURCE_DB)
        df.to_sql(config.DESTINATION_TABLE, sqlalchemy_engine, if_exists='replace', index=False, schema=config.DESTINATION_DB)
        cursor.execute(f"ALTER TABLE {config.SOURCE_DB}.{config.SOURCE_TABLE} MODIFY COLUMN id int AUTO_INCREMENT PRIMARY KEY")
        cursor.execute(f"ALTER TABLE {config.DESTINATION_DB}.{config.DESTINATION_TABLE} MODIFY COLUMN id int AUTO_INCREMENT PRIMARY KEY")
    else:
        cursor.execute(f'''
            CREATE TABLE {config.SOURCE_DB}.{config.SOURCE_TABLE} (
                id INT PRIMARY KEY AUTO_INCREMENT,
                A CHAR(1), B CHAR(1), C CHAR(1)
            )
        ''')
        cursor.execute(f"INSERT INTO {config.SOURCE_DB}.{config.SOURCE_TABLE} (id, A, B, C) VALUES ({TABLE_SIZE}, 'a', 'b', 'c')")


@pytest.fixture
def controller(request, redis_data):
    param = request.param if hasattr(request, 'param') else ''
    controller = Controller()
    if param == 'object':
        yield controller
    else:
        controller_thread = Thread(target=controller.start)
        controller_thread.start()
        yield controller, controller_thread
        controller.set_stop_flag()

        while controller_thread.is_alive():
            time.sleep(1000)


########
# Test #
########
def test_chunk_creation(controller, setup_table, redis_data):
    controller, controller_thread = controller
    controller.initializer.fetch_metadata(redis_data)
    redis_data.set_current_stage(Stage.BULK_IMPORT_CHUNK_CREATION)
    while redis_data.current_stage != Stage.BULK_IMPORT:
        assert controller_thread.is_alive()
        time.sleep(100)
        if redis_data.current_stage == Stage.START_EVENT_HANDLER:
            redis_data.set_current_stage(Stage.BULK_IMPORT_CHUNK_CREATION)

    # test chunk creation
    assert redis_data.chunk_set.getall() == set([f'{redis_data.migration_id}-{i}' for i in range(10)])
    assert len(redis_data.chunk_stack) == 10

    # test metadata
    assert redis_data.metadata.source_table == config.SOURCE_TABLE
    assert redis_data.metadata.destination_table == config.DESTINATION_TABLE
    assert set(redis_data.metadata.source_columns.split(',')) == {'`id`', '`A`', '`B`', '`C`'}

    # test chunk validation
    done_chunk_count = 2
    done_but_invalid_chunk_count = 2
    while len(redis_data.chunk_stack) > 0:
        chunk_id = redis_data.chunk_stack.pop()
        chunk_info = redis_data.get_chunk_info(chunk_id)
        if done_chunk_count > 0:
            chunk_info.status = ChunkStatus.DONE
            chunk_info.last_pk_inserted = chunk_info.end_pk
            done_chunk_count -= 1
        elif done_but_invalid_chunk_count > 0:
            chunk_info.status = ChunkStatus.DONE
            done_but_invalid_chunk_count -= 1
    redis_data.set_current_stage(Stage.BULK_IMPORT_VALIDATION)

    while redis_data.current_stage != Stage.BULK_IMPORT:
        time.sleep(100)

    assert redis_data.current_stage == Stage.BULK_IMPORT
    assert len(redis_data.chunk_stack) == 8


@pytest.mark.parametrize('setup_table', ['with_data'], indirect=True)
@pytest.mark.parametrize('controller', ['object'], indirect=True)
def test_bulk_import_validation(controller: Controller, setup_table, cursor, override_operation_class):
    assert controller.validator.bulk_import_validation()
    delete_pks = random.sample(range(1, TABLE_SIZE), 10)
    cursor.execute(f'''
        DELETE FROM {config.SOURCE_DB}.{config.SOURCE_TABLE}
        WHERE id IN ({','.join([str(i) for i in delete_pks[:5]])})
    ''')
    assert controller.validator.bulk_import_validation()
    controller.validator.stop_flag = False
    cursor.execute(f'''
        DELETE FROM {config.DESTINATION_DB}.{config.DESTINATION_TABLE}
        WHERE id IN ({','.join([str(i) for i in delete_pks])})
    ''')
    assert not controller.validator.bulk_import_validation()


@pytest.mark.parametrize('setup_table', ['with_data'], indirect=True)
@pytest.mark.parametrize('controller', ['object'], indirect=True)
@pytest.mark.parametrize('case', ['normal', 'on_restart'])
def test_add_index(controller: Controller, setup_table, cursor, case):
    cursor.execute(f'''
        ALTER TABLE {config.DESTINATION_DB}.{config.DESTINATION_TABLE}
        MODIFY COLUMN A VARCHAR(128), MODIFY COLUMN B VARCHAR(128), MODIFY COLUMN C VARCHAR(128)
    ''')

    config.INDEXES = [
        IndexConfig('idx_1', 'A'),
        IndexConfig('idx_2', 'B'),
        IndexConfig('idx_3', 'C'),
        IndexConfig('idx_4', 'A,B'),
        IndexConfig('idx_5', 'A,C'),
        IndexConfig('idx_6', 'B,C')
    ]

    cursor.execute(f"TRUNCATE TABLE {config.SBOSC_DB}.index_creation_status")
    cursor.executemany(f'''
        INSERT INTO {config.SBOSC_DB}.index_creation_status
        (migration_id, index_name, index_columns, is_unique)
        VALUES (%s, %s, %s, %s)
    ''', [(1, index.name, index.columns, index.unique) for index in config.INDEXES])

    if case == 'on_restart':
        cursor.execute(f"UPDATE {config.SBOSC_DB}.index_creation_status SET started_at = NOW() WHERE id = 1")

        def delayed_add_index():
            time.sleep(100)
            cursor.execute(f'''
                ALTER TABLE {config.DESTINATION_DB}.{config.DESTINATION_TABLE} ADD INDEX idx_1 (A)
            ''')

        threading.Thread(target=delayed_add_index).start()
        controller.add_index()
    else:
        controller.add_index()

    cursor.execute('''
        SELECT DISTINCT index_name FROM mysql.innodb_index_stats
        WHERE table_name = %s AND index_name not like 'PRIMARY'
    ''', (config.DESTINATION_TABLE,))
    created_indexes = set([row[0] for row in cursor.fetchall()])
    expected_indexes = set([index.name for index in config.INDEXES])
    assert created_indexes == expected_indexes


@pytest.mark.parametrize('setup_table', ['with_data'], indirect=True)
@pytest.mark.parametrize('controller', ['object'], indirect=True)
def test_apply_dml_events_validation(controller: Controller, setup_table, redis_data, cursor, override_operation_class):
    controller.initializer.fetch_metadata(redis_data)

    timestamp_range = (1, 100)
    insert_events = [(random.randint(1, TABLE_SIZE), random.randint(*timestamp_range)) for _ in range(500)]
    update_events = [(random.randint(1, TABLE_SIZE), random.randint(*timestamp_range)) for _ in range(500)]
    delete_events = [(random.randint(1, TABLE_SIZE), random.randint(*timestamp_range)) for _ in range(500)]
    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SBOSC_DB}.inserted_pk_1 (source_pk, event_timestamp) VALUES (%s, %s)
    ''', insert_events)
    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SBOSC_DB}.updated_pk_1 (source_pk, event_timestamp) VALUES (%s, %s)
    ''', update_events)
    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SBOSC_DB}.deleted_pk_1 (source_pk, event_timestamp) VALUES (%s, %s)
    ''', delete_events)
    cursor.execute(f"TRUNCATE TABLE {config.SBOSC_DB}.event_handler_status")
    cursor.execute(f"TRUNCATE TABLE {config.SBOSC_DB}.apply_dml_events_status")

    # Event handler status doesn't have any row
    assert not controller.validator.apply_dml_events_validation()

    # Insert row to event handler status and validate
    cursor.execute(f'''
        INSERT INTO {config.SBOSC_DB}.event_handler_status (migration_id, log_file, log_pos, last_event_timestamp, created_at)
        VALUES (1, 'mysql-bin.000001', 4, {timestamp_range[1]}, NOW())
    ''')
    controller.validator.apply_dml_events_validation()

    # Check if the validation is correct
    cursor.execute(f'''
        SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows WHERE migration_id = 1 AND unmatch_type = 'not_removed'
    ''')
    not_removed_event_count = cursor.fetchone()[0]

    # Not deleted from source table
    # Since rows are not deleted from source table. All pks in deleted_pk will be counted as reinserted pks.
    assert not_removed_event_count == 0

    # Delete rows from source table and validate
    cursor.execute(f'''
        DELETE FROM {config.SBOSC_DB}.apply_dml_events_validation_status WHERE migration_id = 1
    ''')
    cursor.execute(f'''
        DELETE FROM {config.SOURCE_DB}.{config.SOURCE_TABLE} WHERE id IN (SELECT source_pk FROM {config.SBOSC_DB}.deleted_pk_1)
    ''')
    controller.validator.apply_dml_events_validation()

    # Check if the validation is correct
    cursor.execute(f'''
        SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows WHERE migration_id = 1 AND unmatch_type = 'not_removed'
    ''')
    not_removed_event_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.deleted_pk_1")
    deleted_pk_count = cursor.fetchone()[0]
    assert not_removed_event_count == deleted_pk_count

    # Delete rows from destination table and validate
    cursor.execute(f'''
        DELETE FROM {config.SOURCE_DB}.{config.SOURCE_TABLE}
        WHERE id IN ({','.join([str(i) for i in [i[0] for i in delete_events]])})
    ''')
    cursor.execute(f'''
        DELETE FROM {config.DESTINATION_DB}.{config.DESTINATION_TABLE}
        WHERE id IN ({','.join([str(i) for i in [i[0] for i in delete_events]])})
    ''')
    assert controller.validator.apply_dml_events_validation()
    cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows")
    assert cursor.fetchone()[0] == 0

    # Add new insert, update event
    new_timestamp_range = (101, 200)
    new_insert_events = [
        (random.randint(TABLE_SIZE, TABLE_SIZE * 2), random.randint(*new_timestamp_range)) for _ in range(500)]
    new_update_events = [(random.randint(1, TABLE_SIZE), random.randint(*new_timestamp_range)) for _ in range(500)]

    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SBOSC_DB}.inserted_pk_1 (source_pk, event_timestamp) VALUES (%s, %s)
    ''', new_insert_events)
    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SBOSC_DB}.updated_pk_1 (source_pk, event_timestamp) VALUES (%s, %s)
    ''', new_update_events)

    cursor.executemany(f'''
        INSERT IGNORE INTO {config.SOURCE_DB}.{config.SOURCE_TABLE} (id, A, B, C) VALUES (%s, %s, %s, %s)
    ''', [(i[0], 'a', 'b', 'c') for i in new_insert_events])
    cursor.execute(f'''
        UPDATE {config.SOURCE_DB}.{config.SOURCE_TABLE} SET A = 'x' WHERE id IN ({','.join([str(i) for i in [i[0] for i in new_update_events]])})
    ''')

    cursor.execute(f'''
        INSERT INTO {config.SBOSC_DB}.event_handler_status (migration_id, log_file, log_pos, last_event_timestamp, created_at)
        VALUES (1, 'mysql-bin.000001', 4, {new_timestamp_range[1]}, NOW())
    ''')

    assert not controller.validator.apply_dml_events_validation()

    # Apply changes to destination table
    cursor.executemany(f'''
        INSERT IGNORE INTO {config.DESTINATION_DB}.{config.DESTINATION_TABLE} (id, A, B, C) VALUES (%s, %s, %s, %s)
    ''', [(i[0], 'a', 'b', 'c') for i in new_insert_events])
    cursor.execute(f'''
        UPDATE {config.DESTINATION_DB}.{config.DESTINATION_TABLE} SET A = 'x' WHERE id IN ({','.join([str(i) for i in [i[0] for i in new_update_events]])})
    ''')
    assert controller.validator.apply_dml_events_validation()
    cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows")
    assert cursor.fetchone()[0] == 0

    # Test full validation
    assert controller.validator.full_dml_event_validation()
    cursor.execute(f"SELECT is_valid FROM {config.SBOSC_DB}.full_dml_event_validation_status")
    assert cursor.fetchone()[0] == 1

    cursor.execute(f"USE {config.SBOSC_DB}")
    cursor.execute("TRUNCATE TABLE event_handler_status")
    cursor.execute("TRUNCATE TABLE inserted_pk_1")
    cursor.execute("TRUNCATE TABLE updated_pk_1")
    cursor.execute("TRUNCATE TABLE deleted_pk_1")
    cursor.execute("TRUNCATE TABLE unmatched_rows")
    cursor.execute("TRUNCATE TABLE apply_dml_events_validation_status")
    cursor.execute("TRUNCATE TABLE full_dml_event_validation_status")


@pytest.mark.parametrize('setup_table', ['with_data'], indirect=True)
@pytest.mark.parametrize('controller', ['object'], indirect=True)
def test_swap_tables(controller: Controller, setup_table, cursor, redis_data: RedisData):
    redis_data.updated_pk_set.delete()
    redis_data.removed_pk_set.delete()
    assert redis_data.last_catchup_timestamp < time.time()

    # Auto swap is disabled
    redis_data.set_current_stage(Stage.SWAP_TABLES)
    controller.swap_tables()
    assert redis_data.current_stage == Stage.APPLY_DML_EVENTS_VALIDATION

    # Auto swap is enabled, but updated_pk_set is not empty
    config.AUTO_SWAP = True
    redis_data.updated_pk_set.add([1])
    redis_data.set_current_stage(Stage.SWAP_TABLES)
    controller.swap_tables()
    assert redis_data.current_stage == Stage.APPLY_DML_EVENTS_VALIDATION

    # last_catchup_timestamp hasn't been updated after rename
    redis_data.set_last_catchup_timestamp(time.time())
    redis_data.updated_pk_set.get(1)
    redis_data.set_current_stage(Stage.SWAP_TABLES)
    controller.swap_tables()
    assert redis_data.current_stage == Stage.SWAP_TABLES_FAILED
    cursor.execute(f'''
        SELECT COUNT(1) FROM information_schema.TABLES
        WHERE TABLE_NAME in ('{config.SOURCE_TABLE}','{config.DESTINATION_TABLE}')
    ''')
    assert cursor.fetchone()[0] == 2

    # Correct condition
    redis_data.set_last_catchup_timestamp(time.time() + 1)
    redis_data.set_current_stage(Stage.SWAP_TABLES)
    controller.swap_tables()
    assert redis_data.current_stage == Stage.DONE
    cursor.execute(f'''
        SELECT COUNT(1) FROM information_schema.TABLES
        WHERE TABLE_NAME in ('{config.SOURCE_TABLE}','{config.DESTINATION_TABLE}')
    ''')
    assert cursor.fetchone()[0] == 1

    # Clean up
    cursor.execute(f"DROP TABLE {config.SOURCE_DB}._{config.SOURCE_TABLE}_old_{datetime.now().strftime('%Y%m%d')}")
