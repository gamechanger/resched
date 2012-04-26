__author__ = 'Kiril Savino'

from base import RedisBacked, ContentType

class Queue(RedisBacked):
    """
    A durable Queue implementation that allows you to track
    in-progress (popped but not forgotten) items, and
    tracks in-progress items on a per-worker basis without
    losing them in the ether.

    >>> from redis import Redis
    >>> from base import ContentType
    >>> import time
    >>> client = Redis('localhost')

    >>> q = Queue(client, 'stuff', ContentType.JSON)
    >>> q.keep_entry_set = True
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
    >>> q.contains(dict_value)
    True
    >>> q.pop(destructively=True)
    {'hello': 'world'}
    >>> q.pop(destructively=True)
    >>> q.push(dict_value)
    >>> assert q.pop() == dict_value
    >>> q.complete(dict_value)
    >>> assert q.size() == 0
    >>> assert q.number_in_progress() == 0
    >>> assert q.number_active_workers() == 1
    >>> q.reclaim_tasks()

    >>> qa = Queue(client, 'stuff2', ContentType.JSON, worker_id='a', work_ttl=1)
    >>> qb = Queue(client, 'stuff2', ContentType.JSON, worker_id='b', work_ttl=1)
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
    >>> qc = Queue(client, 'stuff3', ContentType.STRING, worker_id='c', work_ttl=60, track_entries=True)
    >>> qc.clear()
    >>> qc.push('hello')
    >>> assert qc.contains('hello')
    >>> assert qc.pop()
    >>> assert qc.contains('hello')
    >>> qc.complete('hello')
    >>> assert not qc.contains('hello')
    >>> qd = Queue(client, 'stuff4', ContentType.STRING, worker_id='c', work_ttl=60, track_entries=True, track_working_entries=False)
    >>> qd.clear()
    >>> qd.push('hello')
    >>> assert qd.contains('hello')
    >>> assert qd.pop()
    >>> assert not qd.contains('hello')
    >>> qd.complete('hello')
    >>> assert not qd.contains('hello')
    >>> first = Queue(client, 'abc')
    >>> second = Queue(client, 'abc_errors')
    >>> first.clear()
    >>> second.clear()
    >>> first.pipe(Queue.RESULT_ERROR, second)
    >>> first.push('a', 'aaa')
    >>> first.pop(return_key=True)
    ('a', 'aaa')
    >>> first.complete('a', Queue.RESULT_ERROR)
    >>> first.size()
    0
    >>> second.size()
    1
    >>> second.peek()
    'a'
    >>> second.pop()
    'aaa'
    >>> qe = Queue(client, 'stuff5', ContentType.STRING, worker_id='e', work_ttl=60, track_entries=True)
    >>> qe.clear()
    >>> qe.push('hello', 'payload1')
    >>> qe.size()
    1
    >>> qe.push('hello', 'payload2')
    >>> qe.size()
    1
    >>> qe.pop()
    'payload2'
    """

    FIFO = 'fifo'
    FILO = LIFO = 'filo'
    RESULT_ERROR = 'error' # useful for piping errors somewhere
    RESULT_SUCCESS = 'success' # useful for piping completion somewhere
    DEFAULT_WORK_TTL_SECONDS = 60

    def __init__(self, redis_client, namespace, content_type=ContentType.STRING, **kwargs):
        """
        @param  redis_client       the redis.py client to use
        @param  namespace          the 'name' of this Queue
        @param  content_type       A resched.ContentType, defaulting to STRING

        @optional  worker_id       defaults to 'global', but useful if doing multi-processing
        @optional  track_entries   whether to keep a set around to track membership, defaults to False
        @optional  strategy        'filo' or 'fifo', defaults to 'fifo'
        @optional  work_ttl        work_ttl_seconds
        @optional  pipes           A list of KV pairs of completion result keys and resultant Queues.
        """
        RedisBacked.__init__(self, redis_client, namespace, content_type, **kwargs)
        self.worker_id = kwargs.get('worker_id', 'global')
        self.strategy = kwargs.get('strategy', self.FIFO)
        assert self.strategy in (self.FIFO, self.LIFO)
        self.keep_entry_set = kwargs.get('track_entries', False)
        self.keep_working_entry_set = kwargs.get('track_working_entries', True)
        self.work_ttl_seconds = kwargs.get('work_ttl', self.DEFAULT_WORK_TTL_SECONDS)
        self.pipes = dict(kwargs.get('pipes', []))
        for result_code, queue in self.pipes.iteritems():
            assert isintance(result_code, basestring) and isinstance(queue, Queue)

        self.QUEUE_LIST_KEY = 'queue.{ns}'.format(ns=namespace)
        self.ENTRY_SET_KEY = 'queue.{ns}.entries'.format(ns=namespace)
        self.WORKER_SET_KEY = 'queue.{ns}.workers'.format(ns=namespace)
        self.WORKING_LIST_KEY = self._working_list_key()
        self.WORKING_ACTIVE_KEY = self._working_active_key()
        self.PAYLOADS = 'queue.{ns}.payload'.format(ns=namespace)


    def pipe(self, result, queue):
        """
        @param result     A string used to pipe completions to another Queue.
        @param queue      The resched.queue.Queue to pipe them to.
        """
        assert result and isinstance(result, basestring)
        assert isinstance(queue, Queue)
        self.pipes[result] = queue

    def _working_list_key(self, worker_id=None):
        worker_id = worker_id or self.worker_id
        return 'queue.{ns}.working.{wid}'.format(ns=self.namespace, wid=worker_id)

    def _working_active_key(self, worker_id=None):
        worker_id = worker_id or self.worker_id
        return 'queue.{ns}.active.{wid}'.format(ns=self.namespace, wid=worker_id)

    def _on_activity(self):
        self.server.sadd(self.WORKER_SET_KEY, self.worker_id)
        self.server.set(self.WORKING_ACTIVE_KEY, 'active')
        self.server.expire(self.WORKING_ACTIVE_KEY, self.work_ttl_seconds)

    def clear(self):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.delete(self.QUEUE_LIST_KEY)
            pipe.delete(self.ENTRY_SET_KEY)
            pipe.delete(self.WORKING_LIST_KEY)
            pipe.delete(self.WORKING_ACTIVE_KEY)
            pipe.srem(self.WORKER_SET_KEY, self.worker_id)
            for worker_id in self.server.smembers(self.WORKER_SET_KEY):
                pipe.delete(self._working_list_key(worker_id))
            pipe.delete(self.WORKER_SET_KEY)
            pipe.execute()

    def reclaim_tasks(self):
        for worker_id in self.server.smembers(self.WORKER_SET_KEY):
            if self.server.get(self._working_active_key(worker_id)):
                continue
            working_key = self._working_list_key(worker_id)
            for x in range(self.server.llen(working_key)):
                self.server.rpoplpush(working_key, self.QUEUE_LIST_KEY)
            self.server.srem(self.WORKER_SET_KEY, worker_id)

    def size(self):
        return self.server.llen(self.QUEUE_LIST_KEY)

    def number_in_progress(self, all=False):
        if all:
            return sum(self.server.llen(self._working_list_key(worker_id)) for worker_id in self.server.smembers(self.WORKER_SET_KEY))
        return self.server.llen(self.WORKING_LIST_KEY)

    def number_of_entries(self):
        return self.server.scard(self.ENTRY_SET_KEY)

    def number_active_workers(self):
        return self.server.scard(self.WORKER_SET_KEY)

    def push(self, value, payload=None, pipeline=None):
        with (pipeline or self.server.pipeline()) as pipe:
            value = self.pack(value)
            payload = self.pack(payload)
            if self._is_pushable(value):
                if self.strategy == self.FIFO:
                    pipe.lpush(self.QUEUE_LIST_KEY, value)
                else:
                    pipe.rpush(self.QUEUE_LIST_KEY, value)
            if self.keep_entry_set:
                pipe.sadd(self.ENTRY_SET_KEY, value)
            if payload:
                pipe.hset(self.PAYLOADS, value, payload)
            pipe.execute()

    def _is_pushable(self, value):
        if not self.keep_entry_set:
            return True
        return not self.contains(value)

    def contains(self, value):
        value = self.pack(value)
        return self.server.sismember(self.ENTRY_SET_KEY, value)

    def pop(self, destructively=False, return_key=False, blocking=False):
        self._on_activity()
        v = None
        if destructively:
            if blocking:
                v = self.server.brpop(self.QUEUE_LIST_KEY)
            else:
                v = self.server.rpop(self.QUEUE_LIST_KEY)
        else:
            if blocking:
                v = self.server.brpoplpush(self.QUEUE_LIST_KEY, self.WORKING_LIST_KEY)
            else:
                v = self.server.rpoplpush(self.QUEUE_LIST_KEY, self.WORKING_LIST_KEY)
        if v and (destructively or not self.keep_working_entry_set):
            self.server.srem(self.ENTRY_SET_KEY, v)
        payload = self.server.hget(self.PAYLOADS, v)
        payload = self.unpack(payload)
        v = self.unpack(v)
        if return_key:
            return v, payload
        return payload or v

    def blocking_pop(self, destructively=False, return_key=False):
        return self.pop(destructively, return_key, blocking=True)


    def peek(self):
        self._on_activity()
        return self.unpack(self.server.lindex(self.QUEUE_LIST_KEY, 0))

    def complete(self, value, result=None):
        """
        Mark an item as complete.
        This does the following:
        * removes the item from the WORKING list for the current worker
        * removes the item from the ENTRY set (if used)
        * moves the key and payload to a RESULT QUEUE if specified
        * removes the PAYLOAD from its collection.

        @param value       The key corresponding to a value that's in-progress.
        @param result      An optional result string, which could trigger a queue transition.
        """
        self._on_activity()
        with self.server.pipeline() as pipe:
            value = self.pack(value)
            pipe.lrem(self.WORKING_LIST_KEY, value)
            pipe.srem(self.ENTRY_SET_KEY, value)
            pipe.hdel(self.PAYLOADS, value)
            if result and result in self.pipes:
                payload = self.server.hget(self.PAYLOADS, value)
                self.pipes[result].push(value, payload=payload, pipeline=pipe)
            pipe.execute()

    def noop(self):
        self._on_activity()

    def unpop(self, value):
        self._on_activity()
        packed = self.pack(value)
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.lrem(self.WORKING_LIST_KEY, packed)
            pipe.lpush(self.QUEUE_LIST_KEY, packed)
            if self.keep_entry_set:
                pipe.sadd(self.ENTRY_SET_KEY, packed)
            pipe.execute()






if __name__ == '__main__':
    import doctest
    doctest.testmod()
