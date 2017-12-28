import time

from .util import deserialize

__author__ = 'shadyrafehi'


class FutureGenerator(object):

    def __init__(self, futures):
        self.futures = list(futures)

    def __iter__(self):
        return self

    def next(self):
        # Collection finished
        if not self.futures:
            raise StopIteration

        # If next in line has been received, don't check rest
        if not self.futures[0].result_recieved:
            while not any(Future.results_received(self.futures)):
                Future.sort_by_completion(self.futures)
                time.sleep(0.025)

        # Return next recieved
        return self.futures.pop(0).result

    __next__ = next


class Future(object):

    def __init__(self, callback, func_name, func_args, func_kwargs):
        self._func_name, self._func_args, self._func_kwargs = func_name, func_args, func_kwargs
        self.__result = None
        self._result_received = False

        def callback_wrapper(*args, **kwargs):
            self.__result = deserialize(callback(*args, **kwargs))
            self._result_received = True
            if issubclass(type(self.__result), Exception):
                raise self.__result
            return self.__result

        self.__callback = callback_wrapper

    @property
    def result(self):
        if self._result_received:
            return self.__result
        return self.__callback()

    def check(self):
        if self._result_received:
            return True

        try:
            self.__callback(timeout=0.0000001)
            return True
        except KeyError:
            return False

    @property
    def result_recieved(self):
        return self._result_received

    def __repr__(self):
        args = ', '.join(map(str, self._func_args)) if self._func_args else ''
        kwargs = ', '.join('{}={}'.format(k, v) for k, v in self._func_kwargs.items()) if self._func_kwargs else ''
        return 'Future(func={}({}{}))'.format(self._func_name, args, kwargs)

    @classmethod
    def sort_by_completion(cls, futures):
        futures.sort(key=lambda f: f.check(), reverse=True)

    @classmethod
    def results_received(cls, futures):
        return [f.result_recieved for f in futures]

    @classmethod
    def complete(cls, futures):
        while not all(cls.results_received(futures)):
            cls.sort_by_completion(futures)

    @classmethod
    def iter(cls, futures):
        return FutureGenerator(futures)


class FutureCollection(tuple):
    """Collection of Future objects"""

    @property
    def results(self):
        self.run_until_completion()
        return tuple(iter(self))

    def iterfutures(self):
        return iter(super(FutureCollection, self).__iter__())

    def run_until_completion(self):
        """
        Wait until all futures are complete
        """
        Future.complete(list(self.iterfutures()))

    def __iter__(self):
        return Future.iter(self.iterfutures())

    def __repr__(self):
        return '{}{}'.format(type(self).__name__, tuple(self.iterfutures()))