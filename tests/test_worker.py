import random
import time
from threading import Thread

import pytest

from config import config
from sbosc.const import Stage, WorkerStatus, ChunkStatus
from sbosc.worker.manager import WorkerManager

import numpy as np
import pandas as pd

############
# Fixtures #
############
SOURCE_COLUMNS = ['A', 'B', 'C']
TEST_TABLE_VALUES = ['a', 'b', 'c']
TABLE_SIZE = 10000


@pytest.fixture
def setup_table(sqlalchemy_engine, cursor, request):
    param = request.param if hasattr(request, 'param') else ""

    if "sparse" in param:
        source_df = pd.DataFrame({
            'id': np.random.choice(range(1, TABLE_SIZE * 10 + 1), size=TABLE_SIZE, replace=False),
            'A': np.random.choice(TEST_TABLE_VALUES, size=TABLE_SIZE),
            'B': np.random.choice(TEST_TABLE_VALUES, size=TABLE_SIZE),
            'C': np.random.choice(TEST_TABLE_VALUES, size=TABLE_SIZE),
        })
    else:
        source_df = pd.DataFrame(np.random.choice(TEST_TABLE_VALUES, size=(TABLE_SIZE, 3)), columns=SOURCE_COLUMNS)

    if "duplicate_key" in param:
        dest_df = source_df.head(TABLE_SIZE // 20)
    elif "with_data" in param:
        dest_df = source_df
    else:
        dest_df = pd.DataFrame(columns=SOURCE_COLUMNS)

    source_df.to_sql(config.SOURCE_TABLE, sqlalchemy_engine, if_exists='replace', index=False)
    dest_df.to_sql(config.DESTINATION_TABLE, sqlalchemy_engine, if_exists='replace', index=False)

    if "sparse" in param:
        cursor.execute(f'alter table {config.SOURCE_TABLE} modify column id int primary KEY AUTO_INCREMENT;')
    else:
        cursor.execute(f'alter table {config.SOURCE_TABLE} add column id int primary KEY AUTO_INCREMENT;')
    cursor.execute(f'alter table {config.DESTINATION_TABLE} add column id int primary KEY AUTO_INCREMENT;')


@pytest.fixture(autouse=True)
def init_redis(redis_data):
    redis_data.worker_config.thread_count = 0
    redis_data.worker_config.batch_size = 100
    redis_data.worker_config.commit_interval = 100
    redis_data.worker_config.revision = 1
    redis_data.set_current_stage(Stage.BULK_IMPORT_CHUNK_CREATION)
    redis_data.metadata.source_columns = '`id`,`' + '`,`'.join(SOURCE_COLUMNS) + '`'
    redis_data.remove_all_chunks()


@pytest.fixture
def worker_manager():
    worker_manager = WorkerManager()
    worker_manager_thread = Thread(target=worker_manager.start)
    worker_manager_thread.start()
    yield worker_manager
    worker_manager.set_stop_flag()

    while worker_manager_thread.is_alive():
        time.sleep(1000)


########
# Test #
########
def test_setup_table(cursor, setup_table):
    cursor.execute(f"SELECT * FROM {config.SOURCE_TABLE} LIMIT 1")
    result = cursor.fetchall()
    assert result[0][0] in TEST_TABLE_VALUES


def check_thread_count(_worker_manager, desired_thread_count):
    for _ in range(10):
        if _worker_manager.desired_thread_count == desired_thread_count and \
                _worker_manager.thread_count == desired_thread_count:
            break
        time.sleep(100)
    assert _worker_manager.thread_count == desired_thread_count
    assert _worker_manager.desired_thread_count == desired_thread_count


def test_add_remove_thread(worker_manager, redis_data):
    # init condition check
    assert worker_manager.desired_thread_count == 0

    # add thread
    redis_data.worker_config.thread_count = 3
    check_thread_count(worker_manager, 3)
    assert worker_manager.created_threads == 3

    # remove thread
    redis_data.worker_config.thread_count = 1
    check_thread_count(worker_manager, 1)
    assert worker_manager.created_threads == 3


def test_check_worker_status(worker_manager, redis_data):
    # create one thread
    redis_data.worker_config.thread_count = 1

    time.sleep(100)

    # set the condition to pass the if statement in check_worker_status

    # set the worker status to BUSY
    assert worker_manager.thread_count == 1
    assert worker_manager.created_threads == 1
    worker_manager.worker_threads[0][0].status = WorkerStatus.BUSY

    time.sleep(100)
    redis_data.remove_all_chunks()
    assert len(redis_data.chunk_stack) == 0

    # set the current stage to BULK_IMPORT
    redis_data.set_current_stage(Stage.BULK_IMPORT)
    assert redis_data.current_stage == Stage.BULK_IMPORT

    # check the current stage
    time.sleep(100)
    assert redis_data.current_stage == Stage.BULK_IMPORT_VALIDATION

    # check the current stage when if statement is not passed in check_worker_status
    time.sleep(100)
    assert redis_data.current_stage == Stage.BULK_IMPORT_VALIDATION


@pytest.mark.parametrize(
    'setup_table',
    ["standard", "duplicate_key", "sparse"],
    indirect=True
)
def test_bulk_import(setup_table, cursor, worker_manager, request_id, redis_data, override_operation_class):
    redis_data.worker_config.thread_count = 0

    # Create chunks
    chunk_count = 10
    chunk_size = TABLE_SIZE // chunk_count
    if "sparse" in request_id:
        chunk_size *= 10
        config.USE_BATCH_SIZE_MULTIPLIER = True
    for chunk_id in range(chunk_count):
        redis_data.push_chunk(chunk_id)
        chunk_info = redis_data.get_chunk_info(chunk_id)
        chunk_info.set({
            'start_pk': chunk_size * chunk_id + 1,
            'end_pk': chunk_size * (chunk_id + 1),
            'status': ChunkStatus.NOT_STARTED
        })

    # Check tables
    cursor.execute(f"SELECT COUNT(1) FROM {config.SOURCE_TABLE}")
    assert cursor.fetchone()[0] == TABLE_SIZE

    cursor.execute(f"SELECT MAX(id) FROM {config.DESTINATION_TABLE}")
    if "duplicate_key" in request_id:
        assert cursor.fetchone()[0] == TABLE_SIZE // 20
    else:
        assert cursor.fetchone()[0] is None
    time.sleep(100)

    # Create threads
    assert worker_manager.thread_count == 0
    assert len(redis_data.chunk_stack) == 10

    desired_thread_count = 5
    redis_data.worker_config.thread_count = desired_thread_count
    redis_data.worker_config.batch_size = 300
    check_thread_count(worker_manager, 5)

    # Check worker status
    time.sleep(100)
    for i in range(worker_manager.thread_count):
        worker = worker_manager.worker_threads[i][0]
        assert worker.status == WorkerStatus.IDLE
        # Check configs
        assert worker.use_batch_size_multiplier == config.USE_BATCH_SIZE_MULTIPLIER
        assert isinstance(worker.migration_operation, config.operation_class)

    # Set current stage to BULK_IMPORT
    t1 = time.time()
    redis_data.set_current_stage(Stage.BULK_IMPORT)
    assert redis_data.current_stage == Stage.BULK_IMPORT
    while redis_data.current_stage == Stage.BULK_IMPORT:
        time.sleep(100)
    t2 = time.time()
    print(f"Execution time: {t2 - t1}")

    cursor.execute(f"SELECT COUNT(1) FROM {config.DESTINATION_TABLE}")
    assert cursor.fetchone()[0] == TABLE_SIZE

    # test worker metrics
    avg_insert_rate = redis_data.worker_metric.average_insert_rate
    datapoints = []
    for worker, _ in worker_manager.worker_threads:
        datapoints += worker.datapoints
    calculated_avg_insert_rate = sum(datapoints) / len(datapoints)
    print(f"Average insert rate: {avg_insert_rate}, calculated average insert rate: {calculated_avg_insert_rate}")
    assert abs(avg_insert_rate / calculated_avg_insert_rate - 1) < 0.01


@pytest.mark.parametrize('setup_table', ["with_data"], indirect=True)
def test_apply_dml_events(setup_table, worker_manager, cursor, redis_data, override_operation_class):
    # set worker config
    redis_data.worker_config.thread_count = 5

    time.sleep(100)

    updated_rows = 1000
    removed_rows = 1000
    removed_pks = random.sample(range(1, TABLE_SIZE + 1), removed_rows)
    updated_pks = set(random.sample(range(1, TABLE_SIZE + 1), updated_rows)) - set(removed_pks)

    redis_data.updated_pk_set.delete()
    redis_data.removed_pk_set.delete()

    redis_data.updated_pk_set.add(updated_pks)
    redis_data.removed_pk_set.add(removed_pks)

    # update rows
    modified_column = SOURCE_COLUMNS[0]
    cursor.execute(f'''
        UPDATE {config.SOURCE_TABLE} SET {modified_column} = 'x' WHERE id IN ({','.join(str(pk) for pk in updated_pks)})
    ''')
    redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)

    while len(redis_data.updated_pk_set) > 0 or len(redis_data.removed_pk_set) > 0:
        time.sleep(100)
    time.sleep(100)

    # check updated rows
    cursor.execute(f'''
        SELECT count(*) FROM {config.DESTINATION_TABLE} WHERE {modified_column} = 'x'
    ''')
    assert cursor.fetchone()[0] == len(updated_pks)

    # check removed rows
    cursor.execute(f'''
        SELECT count(*) FROM {config.DESTINATION_TABLE} WHERE id IN ({','.join(str(pk) for pk in removed_pks)})
    ''')
    assert cursor.fetchone()[0] == 0

    # check sets
    assert len(redis_data.updated_pk_set) == 0
    assert len(redis_data.removed_pk_set) == 0


@pytest.mark.parametrize('setup_table', ["with_data"], indirect=True)
def test_swap_tables(setup_table, worker_manager, cursor, redis_data, request_id):
    redis_data.worker_config.thread_count = 1

    def insert_and_check(source_table):
        cursor.execute(f"INSERT INTO {source_table} (A, B, C) VALUES (1, 2, 3)")
        last_inserted_id = cursor.lastrowid
        redis_data.updated_pk_set.add([last_inserted_id])
        for _ in range(10):
            cursor.execute(f"SELECT * FROM {config.DESTINATION_TABLE} WHERE id = {last_inserted_id}")
            if cursor.rowcount > 0:
                break
            time.sleep(100)
        assert cursor.rowcount == 1

    # swap table stage
    old_source_table = f"_{config.SOURCE_TABLE}_old"
    cursor.execute(f"RENAME TABLE {config.SOURCE_TABLE} TO {old_source_table}")
    redis_data.set_old_source_table(old_source_table)
    redis_data.set_current_stage(Stage.SWAP_TABLES)

    insert_and_check(old_source_table)

    cursor.execute(f"RENAME TABLE {old_source_table} TO {config.SOURCE_TABLE}")
    redis_data.set_old_source_table(None)

    time.sleep(100)

    insert_and_check(config.SOURCE_TABLE)
