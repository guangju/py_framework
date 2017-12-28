import multiprocessing
from multiprocessing.managers import RemoteError
import time
import sys
import os
from ..base import TaskExecutorBase, TaskerBase, TaskerPoolBase


if sys.version_info >= (3, 0):
    import queue
else:
    import Queue as queue


def capture_termination(func):
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except(RemoteError, IOError, EOFError):
            exit(0)
    return wrap


class TaskExecutor(TaskExecutorBase):
    """
    Executes a task in a separate process
    """

    def __init__(self, input_, output, shared_data, task_counter):
        """
        :param input_:
        :param output: Dictionary which is used to return results
        :param shared_data:
        :param task_counter:
        :return:
        """
        self.input = input_
        self.output = output
        self._shared_data = shared_data
        self._task_counter = task_counter
        self.listen()

    @property
    @capture_termination
    def is_alive(self):
        return self._shared_data[0]

    @capture_termination
    def _set_result(self, key, data):
        self.output[key] = data

    @capture_termination
    def _decrement_counter(self, key):
        self._task_counter.pop(0)

    def listen(self):
        while self.is_alive:
            try:
                self.execute(*self.input.get(timeout=0.00001))
            except queue.Empty:
                time.sleep(0.02)


class Tasker(TaskerBase):

    _counter = 0

    def __init__(self, task_id=None, single_job=False):
        self._terminated = False
        super(Tasker, self).__init__(task_id, single_job)

    def _setup(self):
        self.manager = multiprocessing.Manager()
        self.results = self.manager.dict()
        self._shared_data = self.manager.Array('i', range(1))
        self._shared_data[0] = 1
        self.tasks = multiprocessing.Queue(0)
        self._task_counter = self.manager.list()
        self.process = multiprocessing.Process(
            target=TaskExecutor,
            args=(self.tasks, self.results, self._shared_data, self._task_counter)
        )
        self.process.start()

    def __len__(self):
        return len(self._task_counter)

    def _new_key(self):
        key = Tasker._counter
        Tasker._counter += 1
        return key

    def _increment_counter(self):
        self._task_counter.append(0)

    def _submit_task(self, data):
        print "_submit_task"
        self.tasks.put(data)

    def terminate(self):
        self._shared_data[0] = 0
        self._terminated = True
        self.process.join()

    def has_terminated(self):
        return self._terminated


class TaskerPool(TaskerPoolBase):
    """Multi-process based task pool"""

    def __init__(self, pool_size=None):
        """
        :param pool_size: Allocate the number of pools. If no pool specified, default to machine's CPU count
        :type pool_size: int | None
        :return:
        """
        if pool_size is None:
            pool_size = self.mac

            hine_cpu_count()

        if not isinstance(pool_size, int) or pool_size < 1:
            raise ValueError('The TaskerPool pool_size parameter can only be an integer greater than 0, or None')

        taskers = [Tasker(task_id=i) for i in range(pool_size)]
        super(TaskerPool, self).__init__(taskers)

    @staticmethod
    def machine_cpu_count():
        """
        :return: Retrieves the machine's CPU count
        :rtype: int
        """
        return multiprocessing.cpu_count()
