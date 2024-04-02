import os
import time
from unittest.mock import patch

import MySQLdb
import pytest
from sqlalchemy import create_engine

from modules.logger import get_logger

# current dir
PATH = os.path.dirname(os.path.abspath(__file__))

ENVS = {
    'AWS_REGION': 'ap-northeast-2',
    'POD_NAME': 'local',
    'CONFIG_FILE': f'{PATH}/configs/config.yaml',
    'SECRET_FILE': f'{PATH}/configs/secrets.json',
}
os.environ.update(ENVS)

# Set up logger
get_logger()


@pytest.fixture(scope='session')
def secret():
    from config import secret
    return secret


@pytest.fixture(scope='session')
def config():
    from config import config
    return config


@pytest.fixture
def request_id(request):
    return request.node.callspec.id


@pytest.fixture(scope='session')
def cursor(config, secret):
    connection = MySQLdb.connect(
        host=config.SOURCE_WRITER_ENDPOINT,
        port=secret.PORT,
        user=secret.USERNAME,
        password=secret.PASSWORD,
        db=config.SOURCE_DB,
        autocommit=True
    )
    with connection.cursor() as cursor:
        yield cursor


@pytest.fixture
def sqlalchemy_engine(config, secret):
    return create_engine(f'mysql+mysqldb://{secret.USERNAME}:@{config.SOURCE_WRITER_ENDPOINT}:{secret.PORT}/sbosc')


@pytest.fixture(autouse=True)
def time_sleep_mock():
    """
    Mock time.sleep to speed up the test (1ms = 1s)
    """
    sleep = time.sleep
    with patch('time.sleep', side_effect=lambda duration: sleep(duration / 1000)):
        yield


@pytest.fixture(scope='session')
def redis_data():
    from modules.redis import RedisData
    return RedisData(1, False)


@pytest.fixture(autouse=True, scope='session')
def init_migration(config, cursor, redis_data):
    from sbosc.const import Stage
    from sbosc.controller.initializer import Initializer

    cursor.execute(f'''
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{config.SOURCE_DB}'
    ''')
    for table, in cursor.fetchall():
        cursor.execute(f'DROP TABLE {table}')

    for table in [config.SOURCE_TABLE, config.DESTINATION_TABLE]:
        cursor.execute(f"CREATE TABLE {table} (id int)")
    migration_id = Initializer().init_migration()

    # Validate Initializer.init_migration
    assert migration_id == 1
    assert redis_data.current_stage == Stage.START_EVENT_HANDLER
    assert redis_data.metadata.source_db == config.SOURCE_DB
    assert redis_data.metadata.source_table == config.SOURCE_TABLE
    assert redis_data.metadata.destination_db == config.DESTINATION_DB
    assert redis_data.metadata.destination_table == config.DESTINATION_TABLE
    assert redis_data.metadata.source_columns == '`id`'
    assert redis_data.metadata.start_datetime is not None


@pytest.fixture(scope='module', params=['BaseOperation', 'CrossClusterBaseOperation'])
def override_operation_class(request):
    config.OPERATION_CLASS = request.param
