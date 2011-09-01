__author__ = 'Kiril Savino'

import simplejson
from base import RedisBacked

class Queue(RedisBacked):
    """
    A durable Queue implementation that allows you to track
    in-progress (popped but not forgotten) items, and
    tracks in-progress items on a per-worker basis without
    losing them in the ether.
    """

    def __init__(self, redis_client, namespace, content_type, worker_id='global'):
        RedisBacked.__init__(self, redis_client, namespace, content_type)
        self.worker_id = worker_id
        self.QUEUE_KEY = 'queue.{ns}'.format(ns=namespace)
        self.WORKERS_KEY = 'queue.{ns}.workers'.format(ns=namespace)
        self.WORKING_KEY = 'queue.{ns}.working.{wid}'.format(ns=namespace, wid=worker_id)

        def push(self, value):
            self.server.lpush(self.QUEUE_KEY, self.pack(value))

        def pop(self, destructively=False):
            self.server.zadd(self.WORKERS_KEY, self.worker_id)
            return self.unpack(self.server.rpoplpush(self.QUEUE_KEY, self.WORKING_KEY))

        def complete(self, value):
            self.server.zrem(self.WORKING_KEY, self.pack(value))

        def unpop(self, value):
            packed = self.pack(value)
            with self.server.pipeline() as pipe:
                pipe.multi()
                pipe.zrem(self.WORKING_KEY, packed)
                self.server.lpush(self.QUEUE_KEY, packed)
                pipe.execute()



