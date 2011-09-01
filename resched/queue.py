__author__ = 'Kiril Savino'

import simplejson
from base import RedisBacked

class Queue(RedisBacked):
    """
    A durable Queue implementation that allows you to track
    in-progress (popped but not forgotten) items, and
    tracks in-progress items on a per-worker basis without
    losing them in the ether.

    >>> from redis import Redis
    >>> from base import ContentType
    >>> q = Queue(Redis('localhost'), 'stuff', ContentType.JSON)
    >>> q.clear()
    >>> assert not q.peek()
    >>> dict_value = {'hello': 'world'}
    >>> string_value = '{"hello": "world"}'
    >>> q.push(dict_value)
    >>> assert q.peek()
    >>> assert q.pop(destructively=True)
    >>> assert not q.pop(destructively=True)
    >>> q.push(string_value)
    >>> assert q.peek()
    >>> assert q.pop(destructively=True) == dict_value
    >>> q.push(dict_value)
    >>> assert q.pop() == dict_value
    >>> q.complete(dict_value)
    >>> assert q.size() == 0
    >>> assert q.number_in_progress() == 0
    >>> assert q.number_active_workers() == 1
    """

    def __init__(self, redis_client, namespace, content_type, worker_id='global'):
        RedisBacked.__init__(self, redis_client, namespace, content_type)
        self.worker_id = worker_id
        self.QUEUE_KEY = 'queue.{ns}'.format(ns=namespace)
        self.WORKERS_KEY = 'queue.{ns}.workers'.format(ns=namespace)
        self.WORKING_KEY = 'queue.{ns}.working.{wid}'.format(ns=namespace, wid=worker_id)

    def clear(self):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.delete(self.QUEUE_KEY)
            pipe.delete(self.WORKING_KEY)
            pipe.srem(self.WORKERS_KEY, self.worker_id)
            pipe.execute()

    def size(self):
        return self.server.llen(self.QUEUE_KEY)

    def number_in_progress(self):
        return self.server.llen(self.WORKING_KEY)

    def number_active_workers(self):
        return self.server.scard(self.WORKERS_KEY)

    def push(self, value):
        self.server.lpush(self.QUEUE_KEY, self.pack(value))

    def pop(self, destructively=False):
        self.server.sadd(self.WORKERS_KEY, self.worker_id)
        v = self.server.rpop(self.QUEUE_KEY) if destructively else self.server.rpoplpush(self.QUEUE_KEY, self.WORKING_KEY)
        return self.unpack(v)

    def peek(self):
        return self.unpack(self.server.lindex(self.QUEUE_KEY, 0))

    def complete(self, value):
        self.server.lrem(self.WORKING_KEY, self.pack(value))

    def unpop(self, value):
        packed = self.pack(value)
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.WORKING_KEY, packed)
            self.server.lpush(self.QUEUE_KEY, packed)
            pipe.execute()






if __name__ == '__main__':
    import doctest
    doctest.testmod()
