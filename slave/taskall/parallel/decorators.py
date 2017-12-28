from .base import Tasker, TaskerPool


def taskify(func):
    task = Tasker()
    return task.taskify(func)


def poolify(pool_size=None):
    pool = TaskerPool(pool_size=pool_size)
    return pool.poolify
