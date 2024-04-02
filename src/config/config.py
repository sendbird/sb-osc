import os
from dataclasses import dataclass
from importlib import import_module
from pkgutil import walk_packages

import yaml
import dns.resolver

from config import Env
from sbosc.operations.base import BaseOperation, CrossClusterBaseOperation


def get_operation_class(class_name):
    package = import_module('sbosc.operations')

    for _, name, is_pkg in walk_packages(package.__path__, package.__name__ + '.'):
        if not is_pkg:
            module = import_module(name)
            if hasattr(module, class_name):
                return getattr(module, class_name)

    raise ImportError(f"Operation class {class_name} not found")


def get_cluster_id(endpoint, cluster_id=None) -> str:
    """
    Get RDS cluster identifier from endpoint or cname record if cluster_id is not provided
    :param endpoint: RDS cluster endpoint or CNAME record that targets RDS cluster endpoint
    :param cluster_id: Optional RDS cluster identifier. If provided, it will be returned
    :return: Resolved RDS cluster identifier
    """
    if cluster_id is not None:
        return cluster_id
    elif 'rds.amazonaws.com' in endpoint:
        return endpoint.split('.')[0]
    else:
        try:
            answers = dns.resolver.resolve(endpoint, 'CNAME')
            if len(answers) == 1 and 'rds.amazonaws.com' in answers[0].target:
                return answers[0].target.split('.')[0]
            else:
                raise Exception(
                    f"Can't get cluster_id from endpoint. Endpoint {endpoint} targets multiple cname records")
        except dns.resolver.NoAnswer:
            raise Exception(f"Can't get cluster_id from endpoint. Endpoint {endpoint} doesn't have cname record")


@dataclass
class IndexConfig:
    name: str
    columns: str
    unique: bool = False


class Config:
    # Migration plan
    SOURCE_WRITER_ENDPOINT = ''
    SOURCE_READER_ENDPOINT = ''
    SOURCE_CLUSTER_ID = None  # optional
    SOURCE_DB = 'sbosc'
    SOURCE_TABLE = ''
    DESTINATION_WRITER_ENDPOINT = None
    DESTINATION_READER_ENDPOINT = None
    DESTINATION_CLUSTER_ID = None  # optional
    DESTINATION_DB = 'sbosc'
    DESTINATION_TABLE = ''
    MIN_CHUNK_SIZE = 100000
    MAX_CHUNK_COUNT = 200
    AUTO_SWAP = False
    PREFERRED_WINDOW = '00:00-23:59'
    SKIP_BULK_IMPORT = False
    OPERATION_CLASS = BaseOperation
    INDEXES = []
    INDEX_CREATED_PER_QUERY = 4

    # Worker config
    MIN_BATCH_SIZE = 100
    BATCH_SIZE_STEP_SIZE = 100
    MAX_BATCH_SIZE = 10000
    MIN_THREAD_COUNT = 4
    THREAD_COUNT_STEP_SIZE = 4
    MAX_THREAD_COUNT = 64
    COMMIT_INTERVAL = 0.01
    OPTIMAL_VALUE_USE_LIMIT = 10
    USE_BATCH_SIZE_MULTIPLIER = False

    # EventHandler config
    EVENT_HANDLER_THREAD_COUNT = 4
    EVENT_HANDLER_THREAD_TIMEOUT = 300
    INIT_BINLOG_FILE: str = None
    INIT_BINLOG_POSITION: int = None

    # Threshold
    CPU_SOFT_THRESHOLD = 70
    CPU_HARD_THRESHOLD = 90
    LATENCY_SOFT_THRESHOLD = 20  # milliseconds
    LATENCY_HARD_THRESHOLD = 50  # milliseconds

    # Validation
    BULK_IMPORT_VALIDATION_BATCH_SIZE = 100000
    APPLY_DML_EVENTS_VALIDATION_BATCH_SIZE = 100000
    VALIDATION_THREAD_COUNT = 4
    FULL_DML_EVENT_VALIDATION_INTERVAL = 1  # hours

    # DML event loading
    PK_SET_MAX_SIZE = 1000000
    EVENT_BATCH_DURATION = 3600

    def __init__(self):
        env = Env()
        if os.path.exists(env.CONFIG_FILE):
            with open(env.CONFIG_FILE, 'r') as f:
                config = yaml.safe_load(f)
                for key, value in config.items():
                    setattr(self, key.upper(), value)

        if type(self.OPERATION_CLASS) == str:
            self.OPERATION_CLASS = get_operation_class(self.OPERATION_CLASS)

        if self.DESTINATION_WRITER_ENDPOINT is None:
            self.DESTINATION_WRITER_ENDPOINT = self.SOURCE_WRITER_ENDPOINT
        if self.DESTINATION_READER_ENDPOINT is None:
            self.DESTINATION_READER_ENDPOINT = self.SOURCE_READER_ENDPOINT
        if self.DESTINATION_DB is None:
            self.DESTINATION_DB = self.SOURCE_DB
        if self.SOURCE_WRITER_ENDPOINT != self.DESTINATION_WRITER_ENDPOINT:
            self.AUTO_SWAP = False
            if self.OPERATION_CLASS == BaseOperation:
                self.OPERATION_CLASS = CrossClusterBaseOperation
        if self.SOURCE_DB != self.DESTINATION_DB:
            self.AUTO_SWAP = False

        self.SOURCE_CLUSTER_ID = get_cluster_id(self.SOURCE_WRITER_ENDPOINT, self.SOURCE_CLUSTER_ID)
        self.DESTINATION_CLUSTER_ID = get_cluster_id(self.DESTINATION_WRITER_ENDPOINT, self.DESTINATION_CLUSTER_ID)

        if self.INDEXES:
            self.INDEXES = [IndexConfig(**index) for index in self.INDEXES]

        if self.INIT_BINLOG_FILE is not None and self.INIT_BINLOG_POSITION is None:
            raise ValueError('INIT_BINLOG_POSITION is required when INIT_BINLOG_FILE is set')

        if self.SKIP_BULK_IMPORT and self.INIT_BINLOG_FILE is None:
            raise ValueError('INIT_BINLOG_FILE is required when SKIP_BULK_IMPORT is True')