import os
import time
from unittest.mock import patch

import MySQLdb
import pytest
from sqlalchemy import create_engine

# current dir
PATH = os.getcwd()

ENVS = {
    'AWS_REGION': 'ap-northeast-2',
    'POD_NAME': 'local',
    'CONFIG_FILE': f'{PATH}/configs/config.yaml',
    'SECRET_FILE': f'{PATH}/configs/secrets.json',
}
os.environ.update(ENVS)


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
