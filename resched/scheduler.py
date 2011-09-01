__author__ = 'Kiril Savino'

import time
from redis import WatchError
from base import RedisBacked

class Scheduler(RedisBacked):
    """
    >>> import datetime
    >>> from redis import Redis
    >>> from base import ContentType
    >>> scheduler = Scheduler(Redis('localhost'), 'foo', ContentType.STRING)
    >>> value = 'foo'
    >>> scheduler.schedule(value, datetime.datetime.now()+datetime.timedelta(seconds=1))
    >>> scheduler.is_scheduled(value)
    True
    >>> scheduler.pop_due() is None
    True
    >>> time.sleep(1)
    >>> scheduler.pop_due(progress_ttl=0.5) == value
    True
    >>> scheduler.is_scheduled(value)
    False
    >>> time.sleep(0.5)
    >>> scheduler.is_scheduled(value)
    False
    >>> scheduler.reschedule_dropped_items()
    >>> scheduler.is_scheduled(value)
    True
    >>> scheduler.peek_due() == value
    True
    >>> scheduler.pop_due() == value
    True
    >>> scheduler.mark_completed(value)
    >>> time.sleep(0.5)
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
        self.SCHEDULED = 'schedule.{0}.waiting'.format(namespace)
        self.INPROGRESS = 'schedule.{0}.inprogress'.format(namespace)
        self.EXPROGRESS = 'schedule.{0}.exprogress'.format(namespace)
        self.EXPIRES = 'schedule.{0}.expires'.format(namespace)

    def _clear_value(self, value, pipe=None):
        with pipe or self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.SCHEDULED, value)
            pipe.zrem(self.INPROGRESS, value)
            pipe.hdel(self.EXPROGRESS, value)
            pipe.hdel(self.EXPIRES, value)
            pipe.execute()

    def _reschedule_value(self, value, fire_time):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zadd(self.SCHEDULED, value, fire_time)
            pipe.zrem(self.INPROGRESS, value)
            pipe.zrem(self.EXPROGRESS, value)
            pipe.execute()

    def schedule(self, value, fire_datetime, expire_datetime=None):
        """
        Schedule a task (value) to become due at some future date.

        value:            the 'task' (string) which will become due
        fire_datetime:    when the thing becomes due
        expire_datetime:  (optional) the time after which the item shouldn't be processed

        * calling this multiple times will only schedule the task once, at the time
          specified in the last call to the function.
        """
        value = self.pack(value)
        fire_time = time.mktime(fire_datetime.timetuple())
        with self.server.pipeline() as pipe:
            if expire_datetime:
                pipe.multi()
                pipe.hset(self.EXPIRES, value, time.mktime(expire_datetime.timetuple()))
            pipe.zadd(self.SCHEDULED, value, fire_time)
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
        * You've got a limited-time lock on the item, and must call mark_completed() to prevent re-queue
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
                        return None
                    value, scheduled_time = due_list[0]

                    if not value or scheduled_time > time_now:
                        return None # guaranteed this was the earliest, so we're done here

                    if self.is_expired(value):
                        self._clear_value(value, pipe)
                        continue

                    progress_expiry = time.time() + progress_ttl

                    if destructively:
                        self._clear_value(value, pipe)
                    else:
                        pipe.multi()
                        pipe.zrem(self.SCHEDULED, value)
                        pipe.zadd(self.INPROGRESS, **{value: scheduled_time})
                        pipe.hset(self.EXPROGRESS, value, progress_expiry)
                        pipe.execute()

                    return self.unpack(value)

                except WatchError:
                    continue

                finally:
                    pipe.unwatch()

    def mark_completed(self, value):
        """
        Mark an in-progress task as having been completed, which will de-schedule it
        if it's scheduled, and remove it from the in-progress set.

        value:      the task (string) to mark as completed.

        * this is the method you must call to prevent your task from going back into the pool later
        """
        self._clear_value(self.pack(value))

    def is_expired(self, value):
        value = self.pack(value)
        return self.server.hexists(self.EXPIRES, value) and self.server.hget(self.EXPIRES, value) < time.time()

    def is_inprogress(self, value):
        value = self.pack(value)
        if not self.server.hexists(self.EXPROGRESS, value):
            return False
        return self.server.hget(self.EXPROGRESS, value) < time.time()

    def is_scheduled(self, value):
        return self.server.zscore(self.SCHEDULED, self.pack(value)) is not None

    def reschedule_dropped_items(self):
        in_progress = self.server.zrange(self.INPROGRESS, 0, -1, withscores=True)
        now = time.time()
        for value, scheduled_time in in_progress:
            if self.is_inprogress(value):
                continue

            if self.is_expired(value):
                self._clear_value(value)
                continue

            if not self.is_scheduled(value): # try not to overwrite someone else... but don't try too hard
                self._reschedule_value(value, scheduled_time)

    def peek_due(self):
        """
        Return the first non-expired and currently due item, without locking it in any way.
        """
        next_one = self.server.zrange(self.SCHEDULED, 0, 0, withscores=True)
        if not next_one:
            return None
        item, scheduled_time = next_one[0]
        return self.unpack(item) if scheduled_time <= time.time() else None



if __name__ == '__main__':
    import doctest
    doctest.testmod()
