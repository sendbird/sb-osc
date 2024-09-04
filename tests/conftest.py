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
    'SECRET_FILE': f'{PATH}/configs/secret.json',
}
os.environ.update(ENVS)

# Set up logger
get_logger()


@pytest.fixture(scope='package')
def secret():
    from config import secret
    return secret


@pytest.fixture(scope='package')
def config():
    from config import config
    return config


@pytest.fixture
def request_id(request):
    return request.node.callspec.id


@pytest.fixture(scope='package')
def cursor(config, secret):
    connection = MySQLdb.connect(
        host=config.SOURCE_WRITER_ENDPOINT,
        port=secret.PORT,
        user=secret.USERNAME,
        password=secret.PASSWORD,
        autocommit=True
    )
    with connection.cursor() as cursor:
        yield cursor


@pytest.fixture
def sqlalchemy_engine(config, secret):
    return create_engine(
        f'mysql+mysqldb://{secret.USERNAME}:@{config.SOURCE_WRITER_ENDPOINT}:{secret.PORT}/{config.SOURCE_DB}')


@pytest.fixture(autouse=True)
def time_sleep_mock():
    """
    Mock time.sleep to speed up the test (1ms = 1s)
    """
    sleep = time.sleep
    with patch('time.sleep', side_effect=lambda duration: sleep(duration / 1000)):
        yield


@pytest.fixture(scope='session')
def migration_id():
    return 1


@pytest.fixture(scope='session')
def redis_data(migration_id):
    from modules.redis import RedisData
    return RedisData(migration_id, False)


@pytest.fixture(autouse=True, scope='package')
def init_migration(config, cursor, redis_data, migration_id):
    from sbosc.const import Stage
    from sbosc.controller.initializer import Initializer

    for db in [config.SOURCE_DB, config.DESTINATION_DB, config.SBOSC_DB]:
        cursor.execute(f'DROP DATABASE IF EXISTS {db}')
        cursor.execute(f'CREATE DATABASE {db}')

    cursor.execute(f'CREATE TABLE {config.SOURCE_DB}.{config.SOURCE_TABLE} (id int AUTO_INCREMENT PRIMARY KEY)')
    cursor.execute(f'INSERT INTO {config.SOURCE_DB}.{config.SOURCE_TABLE} VALUES (1)')
    cursor.execute(f'CREATE TABLE {config.DESTINATION_DB}.{config.DESTINATION_TABLE} (id int AUTO_INCREMENT PRIMARY KEY)')

    retrieved_migration_id = Initializer().init_migration()

    # Validate Initializer.init_migration
    assert retrieved_migration_id == migration_id
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
