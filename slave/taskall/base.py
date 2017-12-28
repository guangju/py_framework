import binascii
import os
from threading import Thread
import time
import functools

from .future import Future, FutureCollection
from taskall.util import deserialize, serialize


class TaskExecutorBase(object):
    """Base class for a TaskExecutor, responsible for executing given tasks"""

    @property
    def is_alive(self):
        """
        Checks to see if the calling Tasker has terminated

        :rtype: bool
        """
        raise NotImplemented('TaskExecutorBase.is_alive')

    def _set_result(self, key, data):
        """
        Called on task completion to return the result to the Tasker
        """
        raise NotImplemented('TaskExecutorBase._set_result')

    def _decrement_counter(self, key):
        """
        Informs the calling Tasker that a task has completed

        :param key: Task which has completed
        """
        raise NotImplemented('TaskExecutorBase._decrement_counter')

    def execute(self, key, serialized_data):
        """
        Executes the task in a separate thread, and once complete,
        it reports back to the calling Tasker

        :param key: The key attached to this task
        :param serialized_data: Task and arguments of the task
        """
        task, args, kwargs = deserialize(serialized_data)
        print "do execute"
        def _execute():
            try:
                result = task(*args, **kwargs)
            except Exception as e:
                result = e

            self._set_result(key, serialize(result))
            self._decrement_counter(key)

        Thread(target=_execute).start()


class TaskerBase(object):

    def __init__(self, task_id=None, single_job=False):
        """
        :type task_id: Any | None
        :type single_job: bool
        """

        if task_id is None:
            task_id = self._new_key()

        self.id = task_id
        self.single_job = single_job

        self.results = None
        self._setup()

        if self.results is None:
            raise AttributeError(
                '{}.results not initialized after call to {}._setup'.format(
                    type(self).__name__, type(self).__name__))
        self._setup = None

    def _setup(self):
        """
        Initializes the Tasker
        """
        raise NotImplementedError('TaskerBase._setup')

    def __len__(self):
        """
        Returns the length of active tasks
        """
        raise NotImplementedError('TaskerBase.__len__')

    def terminate(self):
        """
        Terminates the Tasker
        """
        raise NotImplementedError('TaskerBase.terminate')

    def has_terminated(self):
        """
        Checks to see if the Tasker has terminated

        :rtype: bool
        """
        raise NotImplementedError('TaskerBase._not_terminated')

    def _increment_counter(self):
        """
        Increment the task counter
        """
        raise NotImplementedError('TaskerBase._increment_counter')

    def _submit_task(self, data):
        """
        Submits the task to the TaskExecutor
        """
        raise NotImplementedError('TaskerBase._submit_task')

    def _has_result(self, key):
        """
        Checks to see if the result for the given key is ready

        :rtype: bool
        """
        return key in self.results

    def _get_result(self, key):
        """
        Returns the result for the given key

        :rtype: Any
        """
        return self.results[key]

    def _set_result(self, key, data):
        """
        Sets the result for the given key
        """
        self.results[key] = data

    def _new_key(self):
        """
        Generates a random key

        :rtype: str
        """
        return binascii.hexlify(os.urandom(4))

    def taskify(self, func):
        """
        Converts a function into a Task which returns a Future

        :type func: () -> Any
        :rtype: () -> taskall.future.Future
        """
        self._raise_if_terminated()

        if hasattr(func, '_parent_tasker'):
            if func._parent_tasker == self:
                return func
            elif hasattr(func, '_original_func'):
                func = func._original_func
            else:
                raise AssertionError('Could not taskify function')

        @functools.wraps(func)
        def inner(*args, **kwargs):
            return self._add_task(func, args, kwargs)

        inner._parent_tasker = self
        inner._original_func = func
        return inner

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.terminate()

    def map(self, func, *params):
        """
        Create a task for each separate input and return a collection
        of Futures

        :rtype: FutureCollection
        """
        return FutureCollection(map(self.taskify(func), *params))

    def add_task(self, func, *args, **kwargs):
        """
        Returns the given function as a task which returns a Future
        """
        return self.taskify(func)(*args, **kwargs)

    def _raise_if_terminated(self):
        if self.has_terminated():
            raise IOError('{} has already terminated'.format(self))

    def _add_task(self, func, args=(), kwargs=None):

        self._raise_if_terminated()

        if kwargs is None:
            kwargs = {}

        key = self._new_key()

        def callback(timeout=0):
            t0 = time.time() + timeout
            while not self._has_result(key):
                self._raise_if_terminated()
                if timeout and time.time() > t0:
                    raise KeyError(key)
            if self.single_job:
                self.terminate()
            return self._get_result(key)

        self._increment_counter()
        self._submit_task((key, serialize((func, args, kwargs))))
        return Future(callback, func.__name__, args, kwargs)

    def __repr__(self):
        return '{}(id={}, queued={})'.format(self.__class__.__name__, self.id, len(self))


class TaskerPoolBase(object):
    """Base class for all task pool implementations"""

    def __init__(self, taskers):
        """
        :param taskers: Collection of Tasker objects
        :type taskers: collections.Iterable[Tasker]
        """
        self.taskers = taskers
        self.__terminated = False

    @property
    def pool_size(self):
        return len(self.taskers)

    def poolify(self, func):
        """
        Converts a function into a pooled function which returns a Future

        :type func: () -> Any
        :rtype: () -> taskall.future.Future
        """
        self._raise_if_terminated()
        if hasattr(func, '_parent_pool'):
            if func._parent_pool == self:
                return func
            elif hasattr(func, '_original_func'):
                func = func._original_func
            else:
                raise AssertionError('Could not poolify function')

        @functools.wraps(func)
        def inner(*args, **kwargs):
            return self.add_task(func, *args, **kwargs)

        inner._parent_pool = self
        inner._original_func = func
        return inner

    def map(self, func, *params):
        """
        Map the function and a parameter to the Tasker with the least amount of
        tasks and return a future collection

        :param func: Function which accepts a parameter
        :type func: (Any) -> Any
        :param params: List of parameters to be fed into the given function
        :type params: collections.Iterable[Any]
        :return: A collection of Future objects
        :rtype: FutureCollection
        """
        return FutureCollection(self.__min_tasker.add_task(func, *param) for param in zip(*params))

    def __len__(self):
        return sum(len(t) for t in self.taskers)

    def add_task(self, func, *args, **kwargs):
        self._raise_if_terminated()
        return self.__min_tasker.add_task(func, *args, **kwargs)

    @property
    def __min_tasker(self):
        """
        :return: Return the Tasker with the least amount of jobs
        :rtype: Tasker
        """
        return min(self.taskers, key=len)

    def _raise_if_terminated(self):
        if self.__terminated:
            raise IOError('{} has already terminated'.format(self))

    def terminate(self):
        """
        Terminates all Taskers which are still active
        """
        for t in self.taskers:
            t.terminate()
        self.__terminated = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.terminate()
