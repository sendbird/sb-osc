import os
import re
from dataclasses import dataclass
from importlib import import_module
from pkgutil import walk_packages

import yaml
import dns.resolver

from config.env import Env


def _get_cluster_id(endpoint, cluster_id=None) -> str:
    """
    Get RDS cluster identifier from endpoint or cname record if cluster_id is not provided
    :param endpoint: RDS cluster endpoint or CNAME record that targets RDS cluster endpoint
    :param cluster_id: Optional RDS cluster identifier. If provided, it will be returned
    :return: Resolved RDS cluster identifier
    """

    def _is_valid_cluster_endpoint(_endpoint):
        return bool(re.match(r'.*\.cluster-[a-z0-9]+\..+\.rds\.amazonaws\.com', _endpoint))

    if cluster_id is not None:
        return cluster_id
    elif _is_valid_cluster_endpoint(endpoint):
        return endpoint.split('.')[0]
    else:
        try:
            answers = dns.resolver.resolve(endpoint, 'CNAME')
            for answer in answers:
                target = str(answer.target)
                if _is_valid_cluster_endpoint(target):
                    return target.split('.')[0]
            raise Exception(
                f"Can't get cluster_id from endpoint. Endpoint {endpoint} doesn't target valid cluster endpoint")
        except dns.resolver.NoAnswer:
            raise Exception(f"Can't get cluster_id from endpoint. Endpoint {endpoint} doesn't have cname record")


@dataclass
class IndexConfig:
    name: str
    columns: str
    unique: bool = False


class Config:
    # Migration plan
    SBOSC_DB = 'sbosc'
    SOURCE_WRITER_ENDPOINT = ''
    SOURCE_READER_ENDPOINT = ''
    SOURCE_CLUSTER_ID = None  # optional
    SOURCE_DB = None
    SOURCE_TABLE = ''
    DESTINATION_WRITER_ENDPOINT = None
    DESTINATION_READER_ENDPOINT = None
    DESTINATION_CLUSTER_ID = None  # optional
    DESTINATION_DB = None
    DESTINATION_TABLE = ''
    MIN_CHUNK_SIZE = 100000
    MAX_CHUNK_COUNT = 200
    AUTO_SWAP = False
    WAIT_INTERVAL_UNTIL_AUTO_SWAP_IN_SECONDS = 60
    PREFERRED_WINDOW = '00:00-23:59'
    SKIP_BULK_IMPORT = False
    OPERATION_CLASS = 'BaseOperation'
    INDEXES = []
    INDEX_CREATED_PER_QUERY = 4

    # Worker config
    MIN_BATCH_SIZE = 100
    BATCH_SIZE_STEP_SIZE = 100
    MAX_BATCH_SIZE = 10000
    MIN_THREAD_COUNT = 4
    THREAD_COUNT_STEP_SIZE = 4
    MAX_THREAD_COUNT = 64
    COMMIT_INTERVAL_IN_SECONDS = 0.01
    OPTIMAL_VALUE_USE_LIMIT = 10
    USE_BATCH_SIZE_MULTIPLIER = False

    # EventHandler config
    EVENTHANDLER_THREAD_COUNT = 4
    EVENTHANDLER_THREAD_TIMEOUT_IN_SECONDS = 300
    INIT_BINLOG_FILE: str = None
    INIT_BINLOG_POSITION: int = None

    # Threshold
    CPU_SOFT_THRESHOLD = 70
    CPU_HARD_THRESHOLD = 90
    WRITE_LATENCY_SOFT_THRESHOLD = 20  # milliseconds
    WRITE_LATENCY_HARD_THRESHOLD = 50  # milliseconds

    # Validation
    BULK_IMPORT_VALIDATION_BATCH_SIZE = 100000
    APPLY_DML_EVENTS_VALIDATION_BATCH_SIZE = 100000
    VALIDATION_THREAD_COUNT = 4
    APPLY_DML_EVENTS_VALIDATION_INTERVAL_IN_SECONDS = 10
    FULL_DML_EVENT_VALIDATION_INTERVAL_IN_HOURS = 1

    # DML event loading
    PK_SET_MAX_SIZE = 1000000
    EVENT_BATCH_DURATION_IN_SECONDS = 3600

    @property
    def operation_class(self):
        if self._operation_class is not None:
            return self._operation_class

        package = import_module('sbosc.operations')

        for _, name, is_pkg in walk_packages(package.__path__, package.__name__ + '.'):
            if not is_pkg:
                module = import_module(name)
                if hasattr(module, self.OPERATION_CLASS):
                    self._operation_class = getattr(module, self.OPERATION_CLASS)
                    return self._operation_class

        raise ImportError(f"Operation class {self.OPERATION_CLASS} not found")

    def __init__(self):
        env = Env()
        if os.path.exists(env.CONFIG_FILE):
            with open(env.CONFIG_FILE, 'r') as f:
                config = yaml.safe_load(f)
                for key, value in config.items():
                    setattr(self, key.upper(), value)

        if self.DESTINATION_WRITER_ENDPOINT is None:
            self.DESTINATION_WRITER_ENDPOINT = self.SOURCE_WRITER_ENDPOINT
        if self.DESTINATION_READER_ENDPOINT is None:
            self.DESTINATION_READER_ENDPOINT = self.SOURCE_READER_ENDPOINT
        if self.DESTINATION_DB is None:
            self.DESTINATION_DB = self.SOURCE_DB
        if self.SOURCE_WRITER_ENDPOINT != self.DESTINATION_WRITER_ENDPOINT:
            self.AUTO_SWAP = False
            if self.OPERATION_CLASS == 'BaseOperation':
                self.OPERATION_CLASS = 'CrossClusterBaseOperation'

        self.SOURCE_CLUSTER_ID = _get_cluster_id(self.SOURCE_WRITER_ENDPOINT, self.SOURCE_CLUSTER_ID)
        self.DESTINATION_CLUSTER_ID = _get_cluster_id(self.DESTINATION_WRITER_ENDPOINT, self.DESTINATION_CLUSTER_ID)

        if self.INDEXES:
            self.INDEXES = [IndexConfig(**index) for index in self.INDEXES]

        if self.INIT_BINLOG_FILE is not None and self.INIT_BINLOG_POSITION is None:
            raise ValueError('INIT_BINLOG_POSITION is required when INIT_BINLOG_FILE is set')

        if self.SKIP_BULK_IMPORT and self.INIT_BINLOG_FILE is None:
            raise ValueError('INIT_BINLOG_FILE is required when SKIP_BULK_IMPORT is True')

        self._operation_class = None  # Will be set by property operation_class
