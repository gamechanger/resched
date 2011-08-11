__author__ = 'Kiril Savino'

import redis
import time

class Scheduler(object):
    """
    >>> import datetime
    >>> import time
    >>> scheduler = Scheduler('localhost')
    >>> scheduler.schedule('foo', datetime.datetime.now()+datetime.timedelta(seconds=5))
    >>> x = scheduler.pop_due()
    >>> x # note following line is empty... 'None'
    >>> time.sleep(5)
    >>> scheduler.PROGRESS_TTL_SECONDS = 1
    >>> x = scheduler.pop_due()
    >>> x
    'foo'
    >>> scheduler.is_scheduled(x)
    False
    >>> time.sleep(1)
    >>> scheduler.is_scheduled(x)
    False
    >>> scheduler.reschedule_dropped_items()
    >>> scheduler.is_scheduled(x)
    True
    """

    SCHEDULED = 'schedule.waiting'
    INPROGRESS = 'schedule.inprogress'
    EXPROGRESS = 'schedule.exprogress'
    EXPIRES = 'schedule.expirations'

    PROGRESS_TTL_SECONDS = 60

    def __init__(self, host, port=None):
        kwargs = {'host': host}
        if port: kwargs['port'] = port
        self.server = redis.Redis(**kwargs)

    def _pre_drop_ttl(self):
        return time.time() + self.PROGRESS_TTL_SECONDS

    def _clear_value(self, value):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.SCHEDULED, value).zrem(INPROGRESS, value).hrem(EXPROGRESS, value).hrem(EXPIRES, value)
            pipe.execute()

    def schedule(self, value, fire_datetime, expire_datetime=None):
        fire_time = time.mktime(fire_datetime.timetuple())
        self.server.zadd(self.SCHEDULED, value, fire_time)
        if expire_datetime:
            self.server.hset(self.EXPIRES, value, time.mktime(expire_datetime.timetuple()))

    def deschedule(self, value):
        self._clear_value(value)

    def subscribe(self):
        # TODO: subscribe to schedule alerts
        pass

    def pop_due(self):
        """
        Pop the next 'due' item from the Schedule.  Specifically:

        * find the first non-expired, currently due item
        * put that item in in-progress collection
        * give it to you to work on

        The following properties hold:

        * Popping is a constant-time operation (fast)
        * The move from Scheduled to In Progress is atomic (barring Redis catastrophic failure)
        * You've got a limited-time lock on the item, and must call mark_completed() to prevent re-queue
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
                        self._clear_value(value)
                        continue

                    pipe.multi()
                    pipe.zrem(self.SCHEDULED, value)
                    pipe.zadd(self.INPROGRESS, **{value: scheduled_time})
                    pipe.hset(self.EXPROGRESS, value, self._pre_drop_ttl())
                    pipe.execute()

                    return value

                except redis.WatchError:
                    continue

                finally:
                    pipe.unwatch()

    def mark_completed(self, value):
        self._clear_value(value)

    def is_expired(self, value):
        return self.server.hexists(self.EXPIRES, value) and self.server.hget(self.EXPIRES, value) < time.time()

    def _reschedule_value(self, value, fire_time):
        with self.server.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.INPROGRESS, value).zrem(self.EXPROGRESS, value).zadd(self.SCHEDULED, value, fire_time)
            pipe.execute()

    def is_inprogress(self, value):
        return self.server.hexists(self.EXPROGRESS, value) and self.server.hget(self.EXPROGRESS, value) < time.time()

    def is_scheduled(self, value):
        return self.server.zscore(self.SCHEDULED, value) is not None

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
        return item if scheduled_time <= time.time() else None



class ScheduledTask(object):
    def __init__(self, value, time, postdate_ttl=None):
        self.time = time
        self.value = value
        self.postdate_ttl = postdate_ttl

    def __repr__(self):
        return "ScheduledItem(value={value}, time={time}, postdate_ttl={postdate_ttl})".format(**self.__dict__)

    def __str__(self):
        return "{value} @ {time} +{postdate_ttl}".format(**self.__dict__)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
