import random
import time
from threading import Thread

import pytest

from config import config
from modules.redis import RedisData
from sbosc.const import Stage
from sbosc.eventhandler import EventHandler


############
# Fixtures #
############
@pytest.fixture(autouse=True)
def ignore_deprecation_warning():
    # pymysqlreplication is using deprecated distutils.version.LooseVersion
    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)


@pytest.fixture
def setup_table(cursor):
    # Truncate table
    cursor.execute(f"TRUNCATE TABLE {config.SBOSC_DB}.event_handler_status")
    cursor.execute(f"TRUNCATE TABLE {config.SBOSC_DB}.apply_dml_events_status")
    # Source table
    cursor.execute(f"DROP TABLE IF EXISTS {config.SOURCE_DB}.{config.SOURCE_TABLE}")
    cursor.execute(f'''
        CREATE TABLE {config.SOURCE_DB}.{config.SOURCE_TABLE} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            A CHAR(10), B CHAR(10), C CHAR(10)
        )
    ''')
    print(f"Created table {config.SOURCE_DB}.{config.SOURCE_TABLE}")


@pytest.fixture(autouse=True)
def init_redis(redis_data):
    redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_VALIDATION)
    redis_data.metadata.source_db = config.SOURCE_DB
    redis_data.metadata.source_table = config.SOURCE_TABLE
    redis_data.updated_pk_set.delete()
    redis_data.removed_pk_set.delete()


@pytest.fixture
def event_handler(setup_table):
    event_handler = EventHandler()
    event_handler_thread = Thread(target=event_handler.start)
    event_handler_thread.start()
    yield event_handler
    event_handler.set_stop_flag()

    while event_handler_thread.is_alive():
        time.sleep(1000)


########
# Test #
########
def test_event_handler(event_handler, cursor, redis_data: RedisData):
    time.sleep(100)
    assert redis_data.current_stage == Stage.APPLY_DML_EVENTS_VALIDATION
    assert event_handler.live_mode
    time.sleep(100)

    # test insert event
    cursor.executemany(
        f'INSERT INTO {config.SOURCE_DB}.{config.SOURCE_TABLE} (A, B, C) VALUES (%s, %s, %s)',
        [('a', 'b', 'c'), ('d', 'e', 'f')]
    )
    time.sleep(100)

    assert len(redis_data.updated_pk_set) == 2
    assert set(redis_data.updated_pk_set.get(2)) == {'1', '2'}

    # test update event on same pk
    cursor.executemany(
        f'UPDATE {config.SOURCE_DB}.{config.SOURCE_TABLE} SET A=%s, B=%s, C=%s WHERE id=%s',
        [('a', 'b', 'c', 1), ('d', 'e', 'f', 1)]
    )
    time.sleep(100)
    assert len(redis_data.updated_pk_set) == 1
    assert set(redis_data.updated_pk_set.get(1)) == {'1'}

    # test delete event
    cursor.execute(f'DELETE FROM {config.SOURCE_DB}.{config.SOURCE_TABLE} WHERE id=1')
    time.sleep(100)
    assert len(redis_data.updated_pk_set) == 0
    assert len(redis_data.removed_pk_set) == 1
    assert set(redis_data.removed_pk_set.get(1)) == {'1'}


def test_event_handler_save_to_database(event_handler, cursor, redis_data):
    time.sleep(100)

    total_events = 1000
    redis_data.metadata.max_id = total_events
    insert_events = total_events // 2
    update_events = (total_events - insert_events) // 2
    delete_events = total_events - insert_events - update_events

    # Remove previous data
    for table in ['inserted_pk_1', 'updated_pk_1', 'deleted_pk_1']:
        cursor.execute(f'TRUNCATE TABLE {config.SBOSC_DB}.{table}')
    redis_data.updated_pk_set.delete()
    redis_data.removed_pk_set.delete()

    # Set current stage to START_EVENT_HANDLER
    event_handler.live_mode = False
    redis_data.set_current_stage(Stage.START_EVENT_HANDLER)
    time.sleep(100)

    # Insert events
    for _ in range(insert_events):
        cursor.execute(f'INSERT INTO {config.SOURCE_DB}.{config.SOURCE_TABLE} (A, B, C) VALUES (%s, %s, %s)', ('a', 'b', 'c'))
    after_insert = time.time()
    while redis_data.last_catchup_timestamp < after_insert:
        print("Waiting for INSERT events to be processed...")
        time.sleep(100)

    # Update events
    for i in range(update_events):
        target_id = random.choice(range(1, insert_events + 1))
        # Update events are only created when data is changed
        cursor.execute(
            f'UPDATE {config.SOURCE_DB}.{config.SOURCE_TABLE} SET A=%s, B=%s, C=%s WHERE id=%s',
            (f'a{i}', f'b{i}', f'c{i}', target_id)
        )
    after_update = time.time()
    while redis_data.last_catchup_timestamp < after_update:
        print("Waiting for UPDATE events to be processed...")
        time.sleep(100)

    # Delete events
    deleted_ids = random.sample(range(1, insert_events + 1), delete_events)
    for target_id in deleted_ids:
        cursor.execute(f'DELETE FROM {config.SOURCE_DB}.{config.SOURCE_TABLE} WHERE id=%s', (target_id,))
    after_delete = time.time()
    while redis_data.last_catchup_timestamp < after_delete:
        print("Waiting for DELETE events to be processed...")
        time.sleep(100)

    # Check if the events have been saved to the database
    cursor.execute(f'SELECT COUNT(*) FROM {config.SBOSC_DB}.inserted_pk_1')
    assert cursor.fetchone()[0] == insert_events
    cursor.execute(f'SELECT COUNT(*) FROM {config.SOURCE_DB}.{config.SOURCE_TABLE} WHERE A != %s', ('a',))
    updated_source_rows = cursor.fetchone()[0]
    cursor.execute(
        f'SELECT COUNT(*) FROM {config.SBOSC_DB}.updated_pk_1 WHERE source_pk NOT IN (SELECT source_pk FROM {config.SBOSC_DB}.deleted_pk_1)')
    assert cursor.fetchone()[0] == updated_source_rows
    cursor.execute(f'SELECT COUNT(*) FROM {config.SBOSC_DB}.deleted_pk_1')
    assert cursor.fetchone()[0] == delete_events

    # Set current stage to APPLY_DML_EVENTS
    redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)
    while event_handler.event_loader.last_loaded_timestamp != event_handler.event_store.last_event_timestamp:
        time.sleep(100)

    # Check if the primary keys in updated_pk_set have been saved to the database
    assert len(redis_data.updated_pk_set) == insert_events - delete_events

    # Check if the primary keys in removed_pk_set have been saved to the database
    assert len(redis_data.removed_pk_set) == delete_events

    while redis_data.current_stage != Stage.APPLY_DML_EVENTS_VALIDATION:
        # Event timestamp is in seconds, so same events can be loaded multiple times
        redis_data.updated_pk_set.delete()
        redis_data.removed_pk_set.delete()
        time.sleep(100)
    assert redis_data.current_stage == Stage.APPLY_DML_EVENTS_VALIDATION
