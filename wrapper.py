from time import sleep

import logging


def wrap_operation(zk, inner_op, wait, post, lock_delay):
    """ Uses a distributed semaphore to wrap an operation, where an operation
    is an arbitrary function.

    Args:
        name: the name uniquely identifying this operation. The name is the
        unit of lock contention throttling the speed of the operation.
        inner_op: the operation to be wrapped.
        dectest: the workhorse operation. Decrements and tests the counter.
        It must block until it is safe to proceed with inner_op. dectest will
        be called under mutual exclusion from all other operations of the same
        `name`.
        increment: increments the counter. Mutual exclusion is NOT provided
        when calling `increment`.
        lock_delay: the number of seconds to wait for events that occurred
        before we held the lock to become visible to us. Useful if dectest has
        some propagation delay.

    """
    with zk.Lock("/lock"):
        logging.info(
            "Got lock. Sleeping to let any previous changes propagate")
        sleep(lock_delay)
        logging.info("Starting wait on semaphore")
        wait()
        logging.info("Finished wait on semaphore")

    inner_op()
    logging.info("Finished with operation. Posting semaphore.")
    post()
    logging.info("Posted semaphore. Done!")
