from typing import Literal, Collection

import redis

from utils import from_string, to_string
from modules.redis.connect import get_redis_connection


class Hash:
    def __init__(self, name, conn: redis.Redis = None):
        self.name = name
        self.conn = conn or get_redis_connection()

        data = self.conn.hgetall(self.name)
        for key in self.__annotations__:
            super().__setattr__(key, None)
        for key, value in data.items():
            super().__setattr__(key, from_string(value))

    def __setattr__(self, key, value):
        if hasattr(self, 'conn'):
            # only set values to redis after self.conn init
            self.conn.hset(self.name, key, to_string(value))
        super().__setattr__(key, value)

    def set(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    @property
    def data(self):
        return {
            key: value for key, value in self.__dict__.items()
            if key in self.__annotations__
        }

    def delete(self):
        self.conn.delete(self.name)


class Set:
    def __init__(self, name, conn: redis.Redis = None):
        self.name = name
        self.conn = conn or get_redis_connection()

    def __len__(self):
        return self.conn.scard(self.name)

    def __iter__(self):
        return self.conn.sscan_iter(self.name)

    def __contains__(self, value):
        return self.conn.sismember(self.name, value)

    def add(self, *values):
        if not values:
            return
        self.conn.sadd(self.name, *values)

    def remove(self, *values):
        if not values:
            return
        self.conn.srem(self.name, *values)

    def get(self, count=None):
        return self.conn.spop(self.name, count=count)

    def getall(self):
        return self.conn.smembers(self.name)

    def delete(self):
        self.conn.delete(self.name)


class SortedSet:
    def __init__(self, name, conn: redis.Redis = None):
        self.name = name
        self.conn = conn or get_redis_connection()

    def __len__(self):
        return self.conn.zcard(self.name)

    def __contains__(self, value):
        return self.conn.zscore(self.name, value) is not None

    def add(self, values: Collection):
        if not values:
            return
        if not isinstance(values, dict):
            values = {value: value for value in values}
        self.conn.zadd(self.name, values)

    def remove(self, values):
        if not values:
            return
        self.conn.zrem(self.name, *values)

    def get(self, count=None, minmax: Literal['min', 'max'] = 'min'):
        if minmax == 'min':
            items = self.conn.zpopmin(self.name, count)
        elif minmax == 'max':
            items = self.conn.zpopmax(self.name, count)
        else:
            raise ValueError(f'Invalid minmax: {minmax}')
        return [item[0] for item in items]

    def delete(self):
        self.conn.delete(self.name)


class List:
    def __init__(self, name, conn: redis.Redis = None):
        self.name = name
        self.conn = conn or get_redis_connection()

    def __len__(self):
        return self.conn.llen(self.name)

    def __getitem__(self, index):
        return self.conn.lindex(self.name, index)

    def __setitem__(self, index, value):
        self.conn.lset(self.name, index, value)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def append(self, value):
        self.conn.rpush(self.name, value)

    def delete(self):
        self.conn.delete(self.name)


class Queue(List):
    def push(self, *values):
        if not values:
            return
        self.conn.rpush(self.name, *values)

    def pop(self, count=None):
        return self.conn.lpop(self.name, count=count)


class Stack(List):
    def push(self, *values):
        if not values:
            return
        self.conn.rpush(self.name, *values)

    def pop(self, count=None):
        return self.conn.rpop(self.name, count=count)
