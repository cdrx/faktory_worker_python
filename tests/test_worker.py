import json
import time
from datetime import datetime
from typing import Callable, Iterable, Optional
from unittest.mock import MagicMock

import pytest

from faktory._proto import Connection
from faktory.client import Client
from faktory.worker import Task, Worker


@pytest.fixture
def conn_factory() -> Callable:
    def build_conn(
        connect_return_value: bool = True,
        disconnect_return_value: None = None,
        reply_return_value: None = None,
        get_message_return_value: Optional[Iterable[str]] = None,
    ):
        if get_message_return_value is None:
            get_message_return_value = ["OK"]

        mock_conn = MagicMock()

        mock_conn.connect = MagicMock(return_value=connect_return_value)
        mock_conn.disconnect = MagicMock(return_value=disconnect_return_value)
        mock_conn.reply = MagicMock(return_value=reply_return_value)
        # Expects an iterator
        mock_conn.get_message = MagicMock(return_value=iter(get_message_return_value))
        return mock_conn

    return build_conn


@pytest.fixture
def worker_factory(
    conn_factory: Callable, monkeypatch, *args, **kwargs
) -> Callable[[], Worker]:
    def build_worker(*args, **kwargs) -> Worker:
        conn = conn_factory(*args, **kwargs)
        monkeypatch.setattr("faktory.worker.Connection", conn)

        work = Worker()
        return work

    return build_worker


@pytest.fixture
def conn() -> Connection:

    mock_conn = MagicMock()
    mock_conn.connect = MagicMock(return_value=True)
    mock_conn.disconnect = MagicMock(return_value=None)
    mock_conn.reply = MagicMock(return_value=None)
    # Expects an iterator
    mock_conn.get_message = MagicMock(return_value=iter(["OK"]))

    return mock_conn


@pytest.fixture
def worker(conn: MagicMock, monkeypatch):

    monkeypatch.setattr("faktory.worker.Connection", conn)

    work = Worker()
    return work


def test_mocked_connection_correctly(conn, monkeypatch):
    """
    Quick test to make sure we're mocking out the Worker's
    connection correctly so that we're okay to assume
    the rest of the test suite is valid.
    """

    conn_mock = MagicMock(return_value=conn)

    monkeypatch.setattr("faktory.worker.Connection", conn_mock)
    worker = Worker()
    assert worker.faktory == conn


class TestWorkerRegister:
    def test_errors_without_callable_func(self, worker: Worker):

        with pytest.raises(ValueError):
            worker.register("test", "will_cause_error", True)

    @pytest.mark.parametrize("bind", [True, False])
    def test_can_register_with_and_without_binds(self, worker: Worker, bind: bool):
        def func(x):
            return x

        worker.register("test", func, bind=bind)

    def test_registers_task_instance(self, worker: Worker):
        def func(x):
            return x

        worker.register("test", func)
        task = worker.get_registered_task("test")
        assert isinstance(task, Task)

    def test_can_get_registered_task(self, worker: Worker):
        # This can arguably be under this set of tests
        # or the `get_registered_task` tests,
        # but it makes sure that any registered task
        # can be retrieved by name.
        def func(x):
            return x

        worker.register("test", func)
        task = worker.get_registered_task("test")

        assert task.func == func
        assert task.name == "test"
        assert task.bind == False


class TestWorkerHeartbeat:
    def test_assigns_heartbeat_timestamp(self, worker_factory):
        worker = worker_factory(
            get_message_return_value=json.dumps([{"unneeded": "value"}])
        )
        now = datetime.now()
        time.sleep(0.001)

        assert worker._last_heartbeat is None
        worker.heartbeat()
        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat

    def test_sends_heartbeat_with_worker_id(self, worker_factory):
        worker = worker_factory(
            get_message_return_value=json.dumps([{"unneeded": "value"}])
        )
        worker.heartbeat()

        heartbeat_args = worker.faktory.reply.call_args[0]
        command, args = heartbeat_args
        assert command == "BEAT"
        assert args == {"wid": worker.worker_id}

    def test_terminates_if_told(self, worker_factory):
        worker = worker_factory(
            get_message_return_value=[{"state": "not sure", "terminate": True}]
        )
        assert worker.is_disconnecting == False
        assert worker._last_heartbeat is None
        now = datetime.now()
        time.sleep(0.001)
        worker.heartbeat()
        assert worker.is_disconnecting == True

        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat

    def test_quiets_if_told(self, worker_factory):
        worker = worker_factory(
            get_message_return_value=[{"state": "not sure", "quiet": True}]
        )

        assert worker.is_quiet == False
        assert worker._last_heartbeat is None
        now = datetime.now()
        time.sleep(0.001)
        worker.heartbeat()

        assert worker.is_quiet == True

        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat


class TestWorkerGetWorkerId:
    def test_is_different(self, worker: Worker):
        worker_id = worker.get_worker_id()
        second_worker_id = worker.get_worker_id()
        assert worker_id != second_worker_id

    def test_is_json_serializable(self, worker: Worker):
        worker_id = worker.get_worker_id()
        try:
            json.dumps(worker_id)
        except TypeError as exc:
            pytest.fail("Worker id: {} isn't JSON serializable".format(worker_id))


class TestGetRegisteredTask:
    def test_finds_existing_task(self, worker):
        def func(x):
            return x

        worker.register("test", func)
        task = worker.get_registered_task("test")
        assert task.func == func
        assert task.name == "test"

    def test_errors_on_bad_task(self, worker):
        def func(x):
            return x

        worker.register("test", func)
        with pytest.raises(ValueError):
            task = worker.get_registered_task("other")
