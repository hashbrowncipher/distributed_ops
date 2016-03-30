from ..hacheck_utils import hadown
from ..hacheck_utils import haup
from ..sleeper import Sleeper


class SmartstackSemaphore(object):

    def __init__(self, name, minimum, zk, node_ids):
        self.name = name
        self.minimum = minimum
        self.zk = zk
        self.node_ids = set(node_ids)

    def _decrement(self):
        hadown(self.name)

    def _increment(self):
        haup(self.name)

    def _verify(self, sleep_delay, timeout):
        """Verifies in Zookeeper that the nodes in question are actually
        absent.

        This prevents situations where we decrement, but Nerve (due to
        configuration mishap or some other reason) does not remove our
        nodes from Zookeeper.
        """

        sleeper = Sleeper(timeout)
        while True:
            children = self.zk.get_children(self.name)
            children_ids = set(i.rsplit('_', 1)[0] for i in children)
            if len(self.node_ids & children_ids) == 0:
                break

            sleeper.use(sleep_delay)

    def _inner_wait(self, sleep_delay, timeout):
        """
        Uses Smartstack registration data to test if a named service meets a
        given minimum replication level, and blocks until the test passes.
        """

        sleeper = Sleeper(timeout)
        while True:
            children = self.zk.get_children(self.name)
            if len(children) > self.minimum:
                break

            sleeper.use(sleep_delay)

    def wait(self,
             wait_delay=1, wait_timeout=180,
             verify_delay=0.25, verify_timeout=10):
        self._inner_wait(wait_delay, wait_timeout)
        self._decrement()
        try:
            self._verify(verify_delay, verify_timeout)
        except:
            self._increment()
            raise

    def post(self):
        self._increment()
