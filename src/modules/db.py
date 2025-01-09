from contextlib import contextmanager, ExitStack
from queue import Queue
from typing import Literal

import MySQLdb
from MySQLdb import OperationalError

from config import config, secret


class Database:
    def __init__(self):
        self.connections = {
            'source': {
                'writer': Connection(config.SOURCE_WRITER_ENDPOINT),
                'reader': Connection(config.SOURCE_READER_ENDPOINT)
            },
            'dest': {
                'writer': Connection(config.DESTINATION_WRITER_ENDPOINT),
                'reader': Connection(config.DESTINATION_READER_ENDPOINT)
            }
        }

    def __del__(self):
        self.close()

    def cursor(
            self, cursorclass=None,
            host: Literal['source', 'dest'] = 'source',
            role: Literal['writer', 'reader'] = 'writer'
    ):
        return self.connections[host][role].cursor(cursorclass)

    @staticmethod
    def get_reader_connection_pool(maxsize: int, host: Literal['source', 'dest'] = 'source'):
        endpoint = config.SOURCE_READER_ENDPOINT if host == 'source' else config.DESTINATION_READER_ENDPOINT
        return ConnectionPool(endpoint, maxsize)

    def get_instance_id(self, host: Literal['source', 'dest'] = 'source', role: Literal['writer', 'reader'] = 'writer'):
        with self.cursor(host=host, role=role) as cursor:
            cursor.execute("SELECT @@aurora_server_id;")
            return cursor.fetchone()[0]

    def close(self):
        for host in self.connections:
            for role in self.connections[host]:
                self.connections[host][role].close()


class Connection:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self._conn = None

    def connect(self):
        return MySQLdb.connect(
            host=self.endpoint,
            user=secret.USERNAME,
            password=secret.PASSWORD,
            port=secret.PORT,
            autocommit=True
        )

    def cursor(self, cursorclass=None):
        if not self._conn:
            self._conn = self.connect()
        try:
            self._conn.ping()
        except OperationalError:
            self._conn = self.connect()
        cursor: cursorclass = self._conn.cursor(cursorclass)
        return cursor

    def ping(self):
        if not self._conn:
            self._conn = self.connect()
        try:
            self._conn.ping()
        except OperationalError:
            self._conn = self.connect()

    def close(self):
        if self._conn:
            self._conn.close()


class ConnectionPool:
    def __init__(self, endpoint, maxsize=30, prefill=False):
        self.endpoint = endpoint
        self.free_connections = Queue(maxsize=maxsize)
        self.size = 0
        self.maxsize = maxsize
        if prefill:
            for _ in range(maxsize):
                self.free_connections.put(Connection(self.endpoint))
                self.size = maxsize

    @contextmanager
    def get_connection(self):
        if self.free_connections.qsize() > 0:
            conn = self.free_connections.get()
        else:
            if self.size >= self.maxsize:
                raise Exception("Connection pool full")
            conn = Connection(self.endpoint)
            self.size += 1

        yield conn

        conn.ping()
        if self.free_connections.full():
            raise Exception("Connection pool full")
        else:
            self.free_connections.put(conn)

    @contextmanager
    def get_connections(self, count):
        with ExitStack() as stack:
            connections = [stack.enter_context(self.get_connection()) for _ in range(count)]
            yield connections

    def close(self):
        while not self.free_connections.empty():
            conn = self.free_connections.get()
            conn.close()
            self.size -= 1
