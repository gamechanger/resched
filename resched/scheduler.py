import redis
import time

class Scheduler(object):
    SCHEDULE_KEY = 'schedule'
    POPPED_SET_KEY = 'schedule_popped'

    def __init__(self, host, port):
        self.server = redis.StrictRedis(host=host, port=port)

    def schedule(self, value, fire_datetime, postdate_ttl=None):
        fire_time = time.mktime(fire_datetime.timetuple())
        self.server.zadd(SCHEDULE_KEY, value, fire_time)
        if postdate_ttl:
            self.server.set('%s_ttl' % value, postdate_ttl)

    def deschedule(self, value):
        self.server.zrem(SCHEDULE_KEY, value)

    def subscribe(self):
        # TODO: subscribe to schedule alerts
        pass

    def _handle_ttl_seconds(self):
        return 60

    def _handle_popped_by(self):
        return int(time.time()) + _handle_ttl_seconds()

    def pop_due(self):
        with self.server.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(SCHEDULE_KEY)
                    due_list = pipe.zrange(SCHEDULE_KEY, 0, 1, withscores=True)
                    item, score = due_list[0] if len(due_list) else None

                    if item and score <= time.time():
                        pipe.multi()
                        pipe.zrem(SCHEDULE_KEY, item)
                        pipe.zadd(POPPED_SET_KEY, item, self._handle_popped_by())
                        pipe.execute()
                        return item
                    else:
                        return None

                except redis.WatchError:
                    continue

    def peek_due(self):
        pass
