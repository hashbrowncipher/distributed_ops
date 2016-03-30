import time

class SleeperTimeout(Exception):
    pass

class Sleeper(object):
    def __init__(self, capacity):
        self.capacity = capacity

    def use(self, amount):
        # This almost certainly has floating point issues
        to_sleep = min(amount, self.capacity)
        self.capacity -= to_sleep
        time.sleep(to_sleep)
        if self.capacity <= 0:
            raise SleeperTimeout
