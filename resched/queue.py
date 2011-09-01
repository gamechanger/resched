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
    >>> import time

    >>> q = Queue(Redis('localhost'), 'stuff', ContentType.JSON)
    >>> q.clear()
    >>> q.reclaim_tasks()
    >>> assert q.number_active_workers() == 0
    >>> assert q.size() == 0
    >>> assert q.number_in_progress() == 0
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
    >>> q.reclaim_tasks()

    >>> qa = Queue(Redis('localhost'), 'stuff2', ContentType.JSON, worker_id='a')
    >>> qb = Queue(Redis('localhost'), 'stuff2', ContentType.JSON, worker_id='b')
    >>> qa.work_ttl_seconds = 1
    >>> qb.work_ttl_seconds = 1
    >>> qa.clear()
    >>> qa.push({'hello': 'cruelworld'})
    >>> qa.size()
    1
    >>> qb.size()
    1
    >>> assert qb.pop()
    >>> assert qb.number_in_progress() == 1
    >>> assert qa.number_in_progress() == 0
    >>> qb.complete({'hello': 'cruelworld'})
    >>> qb.number_in_progress()
    0
    >>> qb.push({'hello': 'happyworld'})
    >>> assert qb.pop()
    >>> qb.number_in_progress()
    1
    >>> qa.number_in_progress()
    0
    >>> qb.size() + qa.size()
    0
    >>> time.sleep(1.75)
    >>> qb.number_in_progress()
    1
    >>> qb.size()
    0
    >>> qa.reclaim_tasks()
    >>> qb.size()
    1
    >>> qb.number_in_progress()
    0
    """

    work_ttl_seconds = 60

    def __init__(self, redis_client, namespace, content_type, worker_id='global'):
        RedisBacked.__init__(self, redis_client, namespace, content_type)
        self.worker_id = worker_id
        self.QUEUE_LIST_KEY = 'queue.{ns}'.format(ns=namespace)
        self.WORKER_SET_KEY = 'queue.{ns}.workers'.format(ns=namespace)
        self.WORKING_LIST_KEY = 'queue.{ns}.working.{wid}'.format(ns=namespace, wid=worker_id)
        self.WORKING_ACTIVE_KEY = 'queue.{ns}.active.{wid}'.format(ns=namespace, wid=worker_id)

    def _on_activity(self):
        self.server.sadd(self.WORKER_SET_KEY, self.worker_id)
        self.server.set(self.WORKING_ACTIVE_KEY, 'active')
        self.server.expire(self.WORKING_ACTIVE_KEY, self.work_ttl_seconds)

    def clear(self):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.delete(self.QUEUE_LIST_KEY)
            pipe.delete(self.WORKING_LIST_KEY)
            pipe.delete(self.WORKING_ACTIVE_KEY)
            pipe.srem(self.WORKER_SET_KEY, self.worker_id)
            for worker_id in self.server.smembers(self.WORKER_SET_KEY):
                pipe.delete('queue.{ns}.working.{wid}'.format(ns=self.namespace, wid=worker_id))
            pipe.delete(self.WORKER_SET_KEY)
            pipe.execute()

    def reclaim_tasks(self):
        for worker_id in self.server.smembers(self.WORKER_SET_KEY):
            active_key = 'queue.{ns}.active.{wid}'.format(ns=self.namespace, wid=worker_id)
            if self.server.get(active_key):
                continue
            working_key = 'queue.{ns}.working.{wid}'.format(ns=self.namespace, wid=worker_id)
            for x in range(self.server.llen(working_key)):
                self.server.rpoplpush(working_key, self.QUEUE_LIST_KEY)

    def size(self):
        return self.server.llen(self.QUEUE_LIST_KEY)

    def number_in_progress(self):
        return self.server.llen(self.WORKING_LIST_KEY)

    def number_active_workers(self):
        return self.server.scard(self.WORKER_SET_KEY)

    def push(self, value):
        self._on_activity()
        self.server.lpush(self.QUEUE_LIST_KEY, self.pack(value))

    def pop(self, destructively=False):
        self._on_activity()
        v = self.server.rpop(self.QUEUE_LIST_KEY) if destructively else self.server.rpoplpush(self.QUEUE_LIST_KEY, self.WORKING_LIST_KEY)
        return self.unpack(v)

    def peek(self):
        self._on_activity()
        return self.unpack(self.server.lindex(self.QUEUE_LIST_KEY, 0))

    def complete(self, value):
        self._on_activity()
        self.server.lrem(self.WORKING_LIST_KEY, self.pack(value))

    def noop(self):
        self._on_activity()

    def unpop(self, value):
        self._on_activity()
        packed = self.pack(value)
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.lrem(self.WORKING_LIST_KEY, packed)
            self.server.lpush(self.QUEUE_LIST_KEY, packed)
            pipe.execute()






if __name__ == '__main__':
    import doctest
    doctest.testmod()
