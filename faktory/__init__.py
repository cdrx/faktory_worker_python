from contextlib import contextmanager

from faktory.configuration import config

from .client import Client
from .exceptions import *
from .worker import Worker

__version__ = "0.4.0"
__url__ = "https://github.com/cdrx/faktory_worker_python"


def get_client(*args, **kwargs):
    return Client(*args, **kwargs)


@contextmanager
def connection(*args, **kwargs):
    c = get_client(*args, **kwargs)
    c.connect()
    yield c
    c.disconnect()
