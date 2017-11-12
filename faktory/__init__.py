from contextlib import contextmanager

from . worker import Worker
from . client import Client
from . exceptions import *


def get_client(*args, **kwargs):
    return Client(*args, **kwargs)


@contextmanager
def connection(*args, **kwargs):
    c = get_client(*args, **kwargs)
    c.connect()
    yield c
    c.disconnect()
