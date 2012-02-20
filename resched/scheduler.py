__author__ = 'Kiril Savino'

import time
from redis import WatchError
from base import RedisBacked
import logging

format_version = '0.1.0'

class Scheduler(RedisBacked):
    """
    >>> import datetime
    >>> from redis import Redis
    >>> from base import ContentType
    >>> scheduler = Scheduler(Redis('localhost'), 'foo', ContentType.STRING)
    >>> scheduler.whipe()
    >>> value = 'foo'
    >>> scheduler.schedule(value, datetime.datetime.now()+datetime.timedelta(seconds=1))
    >>> scheduler.is_scheduled(value)
    True
    >>> scheduler.pop_due()
    (None, None)
    >>> time.sleep(1)
    >>> scheduler.pop_due(progress_ttl=1)
    ('foo', 'foo')
    >>> scheduler.is_scheduled(value)
    False
    >>> time.sleep(2)
    >>> scheduler.is_scheduled(value)
    False
    >>> scheduler.reschedule_dropped_items()
    >>> scheduler.is_scheduled(value)
    True
    >>> scheduler.peek_due()
    'foo'
    >>> scheduler.pop_due() == (value, value)
    True
    >>> scheduler.complete(value)
    >>> time.sleep(2)
    >>> scheduler.reschedule_dropped_items()
    >>> scheduler.is_scheduled(value)
    False
    """

    __PROGRESS_TTL_SECONDS = 60

    def __init__(self, redis_client, namespace, content_type):
        """
        Create a scheduler, in a namespace.
        """
        RedisBacked.__init__(self, redis_client, namespace, content_type)
        self.SCHEDULED = 'schedule:{0}:waiting'.format(namespace)
        self.INPROGRESS = 'schedule:{0}:inprogress'.format(namespace)
        self.PAYLOADS = 'schedule:{0}:payload'.format(namespace)
        self.EXPIRATIONS = 'schedule:{0}:expiration'.format(namespace)
        self.VERSION = 'schedule:{0}:version'.format(namespace)
        self.WORKING_TTL = 'schedule:{0}:working'.format(namespace)

    def whipe(self):
        for key in self.server.keys('schedule:{0}:*'.format(self.namespace)):
            self.server.delete(key)

    def _clear_value(self, value, pipe=None):
        with pipe or self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.SCHEDULED, value)
            pipe.zrem(self.INPROGRESS, value)
            pipe.hdel(self.PAYLOADS, value)
            pipe.hdel(self.EXPIRATIONS, value)
            pipe.hdel(self.WORKING_TTL, value)
            # just to be safe, this is for the old storage format...
            pipe.delete(self._payload_key(value))
            # also for the old format...
            pipe.delete(self._working_lock_key(value))
            pipe.execute()

    def _reschedule_value(self, value, fire_time):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zadd(self.SCHEDULED, value, fire_time)
            pipe.zrem(self.INPROGRESS, value)
            pipe.hdel(self.WORKING_TTL, value)
            # leave this here for backwards compat
            pipe.delete(self._working_lock_key(value))
            pipe.execute()

    def schedule(self, value, fire_datetime, expire_datetime=None, payload=None):
        """
        Schedule a task (value) to become due at some future date.

        value:            the 'task' (string) which will become due
        fire_datetime:    when the thing becomes due
        expire_datetime:  (optional) the time after which the item shouldn't be processed
        payload:          (optional) the actual content to return for this task, which can vary over schedule calls

        * calling this multiple times will only schedule the task once, at the time
          specified in the last call to the function.
        """
        value = self.pack(value)
        payload = self.pack(payload) or value
        fire_time = time.mktime(fire_datetime.timetuple())
        expire_time = time.mktime(expire_datetime.timetuple()) if expire_datetime else None

        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zadd(self.SCHEDULED, value, fire_time) # for sorting
            pipe.hset(self.PAYLOADS, value, payload)
            if expire_time:
                pipe.hset(self.EXPIRATIONS, value, expire_time)
            pipe.execute()

    def deschedule(self, value):
        """
        Remove a future scheduled task.

        value:            the 'task' (string) to remove.

        * if the task isn't scheduled, no error is thrown.
        """
        value = self.pack(value)
        self._clear_value(value)

    def subscribe(self):
        # TODO: subscribe to schedule alerts
        pass

    def _publish(self, event, value):
        # TODO: publish to the schedule channel(s?)
        value = self.pack(value)
        pass

    def pop_due(self, progress_ttl=60, destructively=False):
        """
        Pop the next 'due' item from the Schedule.  Specifically:

        progress_ttl:      (optional) the number of seconds after which an in-progress task becomes eligible for re-scheduling [60]
        destructively:     (optional) if set to True, dequeues the item without marking as in-process. [False]

        * find the first non-expired, currently due item
        * put that item in in-progress collection
        * give it to you to work on

        The following properties hold:

        * Popping is a constant-time operation (fast)
        * The move from Scheduled to In Progress is atomic (barring Redis catastrophic failure)
        * You've got a limited-time lock on the item, and must call complete() to prevent re-queue
        * You can govern that by passing 'progress_ttl' a custom # of seconds you'll have before your
          job goes back into the pool.
        """
        with self.server.pipeline() as pipe:
            time_now = time.time()
            while True:
                try:
                    pipe.watch(self.SCHEDULED) # ensure we get a clean remove of the item

                    due_list = pipe.zrange(self.SCHEDULED, 0, 0, withscores=True)
                    if not due_list:
                        return None, None
                    value, scheduled_time = due_list[0]

                    if not value or scheduled_time > time_now:
                        return None, None # guaranteed this was the earliest, so we're done here

                    payload = self.server.hget(self.PAYLOADS, value)
                    if payload:
                        # the new hotness...
                        expire_time = self.server.hget(self.EXPIRATIONS, value)
                        if expire_time and float(expire_time) > time_now:
                            self._clear_value(value, pipe)
                            continue
                        else:
                            self._clear_value(value, pipe) if destructively else self._start_work(value, scheduled_time, progress_ttl, pipe)
                            return (value, self.unpack(payload))

                    else: # try to get it the old way
                        payload = self.server.get(self._payload_key(value))

                        if payload:
                            self._clear_value(value, pipe) if destructively else self._start_work(value, scheduled_time, progress_ttl, pipe)
                            return (value, self.unpack(payload))
                        else:
                            self._clear_value(value, pipe)

                except WatchError:
                    continue

                finally:
                    pipe.unwatch()
        return None, None

    def _start_work(self, value, scheduled_time, progress_ttl, pipe):
        pipe.multi()
        pipe.zrem(self.SCHEDULED, value)
        pipe.zadd(self.INPROGRESS, value, scheduled_time)
        pipe.hset(self.WORKING_TTL, value, time.time() + progress_ttl)
        pipe.execute()

    def _working_lock_key(self, value):
        return 'schedule:{ns}:{value}:working'.format(ns=self.namespace, value=value)

    def _payload_key(self, value):
        return 'schedule:{ns}:{val}'.format(ns=self.namespace, val=value)

    def complete(self, value):
        """
        Mark an in-progress task as having been completed, which will de-schedule it
        if it's scheduled, and remove it from the in-progress set.

        value:      the task (string) to mark as completed.

        * this is the method you must call to prevent your task from going back into the pool later
        """
        self._clear_value(self.pack(value))

    def count_scheduled(self):
        return self.server.zcard(self.SCHEDULED)

    def is_expired(self, value):
        value = self.pack(value)
        expire_date = self.server.hget(self.EXPIRATIONS, value)
        if expire_date:
            # new method just tracks a TTL in a hash
            return float(expire_date) > time.time()
        # old method is with expiring key
        return self.server.get(self._payload_key(value)) is not None

    def is_scheduled(self, value):
        value = self.pack(value)
        return self.server.zscore(self.SCHEDULED, value) is not None and not self.is_expired(value)

    def reschedule_dropped_items(self):
        in_progress = self.server.zrange(self.INPROGRESS, 0, -1, withscores=True)
        now = time.time()
        for value, scheduled_time in in_progress:
            if self.server.get(self._working_lock_key(value)) is not None:
                continue # locked for work

            payload = self.server.hget(self.PAYLOADS, value)
            if payload:
                # new method is explicit, check if it's expired or not
                if self.is_expired(value):
                    self._clear_value(value) # expired, see ya
                    continue
                else:
                    self._reschedule_value(value, scheduled_time)

            else:
                # old method uses key expiration, so if it's gone it's expired
                if self.server.get(self._payload_key(value)) is None:
                    self._clear_value(value)
                    continue # oops, gone, expired

                if not self.is_scheduled(value): # try not to overwrite someone else... but don't try too hard
                    self._reschedule_value(value, scheduled_time)

    def peek_due(self):
        """
        Return the first non-expired and currently due item, without locking it in any way.
        """
        next_one = self.server.zrange(self.SCHEDULED, 0, 0, withscores=True)
        if not next_one:
            return None # schedule is empty
        value, scheduled_time = next_one[0]
        if scheduled_time > time.time():
            return None # not ready yet
        payload = self.server.hget(self.PAYLOADS, value) or self.server.get(self._payload_key(value))
        return self.unpack(payload)



if __name__ == '__main__':
    import doctest
    doctest.testmod()
