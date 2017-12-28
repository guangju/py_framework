from __future__ import print_function
__author__ = 'shadyrafehi'

import base64
import time

try:
    import dill
    _serializer = dill
except ImportError:
    import pickle
    _serializer = pickle


def deserialize(string):
    return _serializer.loads(base64.b64decode(string))


def serialize(obj):
    return base64.b64encode(_serializer.dumps(obj))
