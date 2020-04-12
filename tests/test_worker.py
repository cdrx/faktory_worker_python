import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Optional
from unittest.mock import MagicMock, PropertyMock

import pytest

from faktory._proto import Connection
from faktory.client import Client
from faktory.worker import Task, Worker


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

    mock_executor = MagicMock()

    monkeypatch.setattr("faktory.worker.Connection", conn)

    work = Worker()
    work._last_heartbeat = datetime.now()
    monkeypatch.setattr(work, "_executor", mock_executor)
    return work


def create_future(
    done_return_value: bool = True,
    result_side_effect: Optional[Any] = None,
    result_return_value: Optional[Any] = None,
):
    fut = MagicMock()
    fut.done = MagicMock(return_value=done_return_value)
    fut.job_id = uuid.uuid4().hex
    fut.result = MagicMock(
        return_value=result_return_value, side_effect=result_side_effect
    )
    return fut


def test_get_queues(worker):

    assert worker.get_queues() == worker._queues


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


class TestWorkerTick:
    """
    Lots of tests in this class mock `time.sleep`. Some are
    for actual testing purposes (and will assert so), others
    are to make sure the test suite doesn't take forever
    to run.
    """

    def add_mocks(
        self,
        worker,
        monkeypatch,
        *,
        should_fetch_job: bool = True,
        should_send_heartbeat: bool = True
    ) -> MagicMock:
        """
        Since the mocking process on the `worker` object takes a decent
        number of lines of code, we're breaking it out into a helper function
        here. This (kind of illogically) returns the mock for `time`,
        since that's the only object we create here within
        the helper but can't access outside of it.
        """

        mock_time = MagicMock()
        mock_time.sleep = MagicMock()

        send_status_to_factory = MagicMock()
        heartbeat = MagicMock()
        process = MagicMock()
        disconnect = MagicMock()
        mock_should_fetch_job = PropertyMock(return_value=should_fetch_job)
        mock_should_send_heartbeat = PropertyMock(return_value=should_send_heartbeat)

        monkeypatch.setattr("faktory.worker.time", mock_time)
        monkeypatch.setattr(worker, "heartbeat", heartbeat)
        monkeypatch.setattr(worker, "disconnect", disconnect)
        monkeypatch.setattr(worker, "send_status_to_faktory", send_status_to_factory)
        monkeypatch.setattr(type(worker), "should_fetch_job", mock_should_fetch_job)
        monkeypatch.setattr(
            type(worker), "should_send_heartbeat", mock_should_send_heartbeat
        )
        # Mocking a "private" function might be considered a bad idea, but if we don't,
        # we'd need to mock out the entire success and failure paths of a succesfully
        # run future, which is way out of scope for unit tests.
        monkeypatch.setattr(worker, "_process", process)

        return mock_time

    def test_updates_faktory_on_pending(self, worker, monkeypatch):

        jobs = [create_future(), create_future()]
        mock_time = self.add_mocks(worker, monkeypatch)

        worker._pending = jobs

        worker.tick()

        worker.send_status_to_faktory.assert_called_once()

    def test_doesnt_update_when_no_pending(self, worker, monkeypatch):
        self.add_mocks(worker, monkeypatch)

        worker._pending = []

        worker.tick()

        worker.send_status_to_faktory.assert_not_called()

    def test_sleeps_if_not_fetching(self, worker, monkeypatch):

        mock_time = self.add_mocks(
            worker, monkeypatch, should_fetch_job=False, should_send_heartbeat=False
        )

        worker.tick()

        mock_time.sleep.assert_called_once()

    def test_submits_job_if_present(self, worker, monkeypatch):
        job = {
            "jid": "our job ID",
            "jobtype": "super important probably",
            "args": [1, 2],
        }
        self.add_mocks(worker, monkeypatch)
        worker.faktory.fetch.return_value = {
            "jid": "our job ID",
            "jobtype": "super important probably",
            "args": [1, 2],
        }

        worker.tick()

        worker._process.assert_called_once_with(job["jid"], job["jobtype"], job["args"])

    @pytest.mark.parametrize("empty_payload", [None, {}])
    def test_doesnt_submit_if_no_work(self, worker, monkeypatch, empty_payload):
        """
        Tests to make sure that even if we should be fetching a job,
        if there's no job, we don't submit anything for execution.
        """

        self.add_mocks(worker, monkeypatch)
        worker.faktory.fetch.return_value = empty_payload

        worker.tick()

        worker._process.assert_not_called()

    def test_can_disconnect_gracefully_after_emptying_jobs(self, worker, monkeypatch):
        worker._pending = []
        self.add_mocks(worker, monkeypatch, should_fetch_job=False)
        worker.is_disconnecting = True
        worker._disconnect_after = datetime.now() + timedelta(seconds=10)

        worker.tick()

        worker.faktory.disconnect.assert_called_once_with()

    def test_waits_on_finishing_jobs_before_forcing(self, worker, monkeypatch):
        """
        Tests the scenario between where a disconnect has started and when
        its forced that if there are jobs that are still pending, the
        worker won't kill them off until the timeout.
        """

        worker._pending = [create_future(done_return_value=False)]
        self.add_mocks(worker, monkeypatch, should_fetch_job=False)
        worker.is_disconnecting = True
        worker._disconnect_after = datetime.now() + timedelta(seconds=10)

        worker.tick()
        worker.faktory.disconnect.assert_not_called()
        worker.disconnect.assert_not_called()

    def test_forces_disconnect_after_timeout(self, worker, monkeypatch):
        worker._pending = [create_future()]
        self.add_mocks(worker, monkeypatch, should_fetch_job=False)
        worker.is_disconnecting = True
        worker._disconnect_after = datetime.now() - timedelta(seconds=10)

        worker.tick()

        worker.disconnect.assert_called_once_with(force=True)


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
    def test_assigns_heartbeat_timestamp(self, worker):
        worker.faktory.get_message.return_value = iter(
            json.dumps([{"unneeded": "value"}])
        )
        worker._last_heartbeat = None

        now = datetime.now()
        time.sleep(0.001)

        assert worker._last_heartbeat is None
        worker.heartbeat()
        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat

    def test_sends_heartbeat_with_worker_id(self, worker):
        worker.faktory.get_message.return_value = iter(
            json.dumps([{"unneeded": "value"}])
        )

        worker.heartbeat()

        heartbeat_args = worker.faktory.reply.call_args[0]
        command, args = heartbeat_args
        assert command == "BEAT"
        assert args == {"wid": worker.worker_id}

    def test_terminates_if_told(self, worker):
        worker.faktory.get_message.return_value = iter(
            [{"state": "not sure", "terminate": True}]
        )

        worker._last_heartbeat = None

        assert worker.is_disconnecting == False
        assert worker._last_heartbeat is None
        now = datetime.now()
        time.sleep(0.001)
        worker.heartbeat()
        assert worker.is_disconnecting == True

        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat

    def test_quiets_if_told(self, worker):
        worker._last_heartbeat = None
        worker.is_quiet = False
        worker.faktory.get_message.return_value = iter(
            [json.dumps({"state": "not sure", "quiet": True})]
        )

        assert worker.is_quiet == False
        assert worker._last_heartbeat is None
        now = datetime.now()
        time.sleep(0.001)
        worker.heartbeat()

        assert worker._last_heartbeat is not None
        assert now < worker._last_heartbeat
        assert worker.is_quiet == True


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


class TestWorkerCanDisconnect:
    def test_can_disconnect_if_no_tasks(self, worker):

        assert worker.can_disconnect is True

    def test_cant_disconnect_if_processing(self, worker):

        worker._pending.append("some item")
        assert worker.can_disconnect is False


class TestWorkerShouldFetchJob:
    def test_cant_fetch_job_if_disconnecting(self, worker):
        worker.is_disconnecting = True
        assert worker.should_fetch_job is False

    def test_cant_fetch_job_if_quiet(self, worker):
        worker.is_quiet = True
        assert worker.should_fetch_job is False

    def test_cant_fetch_job_if_has_enough(self, worker):

        worker.is_quiet = False
        worker.is_disconnecting = False

        worker._pending.append("item")
        worker._pending.append("item")
        worker._pending.append("item")

        worker.concurrency = 2

        assert worker.should_fetch_job is False

    def test_can_fetch_if_needs_work(self, worker):
        worker.is_quiet = False
        worker.is_disconnecting = False

        worker._pending.append("item")

        worker.concurrency = 2

        assert worker.should_fetch_job is True


class TestWorkerSendStatusToFaktory:
    def test_does_nothing_if_work_isnt_done(self, worker, monkeypatch):

        job_1 = create_future(done_return_value=False)
        job_2 = create_future(done_return_value=False)

        jobs = [job_1, job_2]

        worker._pending = jobs

        monkeypatch.setattr(worker, "_ack", MagicMock())
        monkeypatch.setattr(worker, "_fail", MagicMock())

        worker.send_status_to_faktory()

        job_1.result.assert_not_called()
        job_2.result.assert_not_called()
        worker._ack.assert_not_called()
        worker._fail.assert_not_called()

    def test_acks_successful_jobs(self, worker, monkeypatch):

        job_1 = create_future(done_return_value=True)

        worker._pending = [job_1]

        worker.send_status_to_faktory()

        job_1.result.assert_called_once()
        worker.faktory.reply.assert_called_with("ACK", {"jid": job_1.job_id})
        worker.faktory.reply.assert_called_once()

        worker.faktory.get_message.assert_called_once()

    def test_fails_errored_jobs(self, worker, monkeypatch):

        job_1 = create_future(
            done_return_value=True, result_side_effect=KeyboardInterrupt()
        )

        worker._pending = [job_1]

        worker.send_status_to_faktory()

        job_1.result.assert_called_once()
        worker.faktory.reply.assert_called_once_with("FAIL", {"jid": job_1.job_id})

        worker.faktory.get_message.assert_called_once()

    def test_relays_exception_info_on_errors(self, worker):

        job = create_future(result_side_effect=ValueError("our testing error"))
        worker._pending = [job]
        worker.send_status_to_faktory()

        worker.faktory.reply.assert_called_once_with(
            "FAIL",
            {
                "jid": job.job_id,
                "errtype": "ValueError",
                "message": "our testing error",
            },
        )

        worker.faktory.get_message.assert_called_once()


class TestWorkerFailAllJobs:
    def test_doesnt_fail_successful_jobs(self, worker, monkeypatch):

        jobs = [create_future(), create_future(), create_future()]
        worker._pending = jobs
        monkeypatch.setattr(worker, "_ack", MagicMock())
        monkeypatch.setattr(worker, "_fail", MagicMock())

        worker.fail_all_jobs()

        assert worker._ack.call_count == 3
        worker._fail.assert_not_called()

    def test_fails_pending_jobs(self, worker, monkeypatch):
        success_job = create_future()
        pending_job = create_future(done_return_value=False)
        jobs = [success_job, pending_job]

        worker._pending = jobs
        monkeypatch.setattr(worker, "_ack", MagicMock())
        monkeypatch.setattr(worker, "_fail", MagicMock())

        worker.fail_all_jobs()

        worker._ack.assert_called_once_with(success_job.job_id)

        worker._fail.assert_called_once_with(pending_job.job_id)


class TestExecutor:
    def test_creates_if_not_exists(self, worker):
        worker._executor = None
        worker._executor_class = MagicMock()

        assert worker._executor is None

        executor = worker.executor
        worker._executor_class.assert_called_once_with(max_workers=worker.concurrency)
        assert worker._executor is not None

    def test_returns_if_existing(self, worker):
        executor = MagicMock()
        worker._executor = executor
        worker._executor_class = MagicMock()

        executor = worker.executor
        worker._executor_class.assert_not_called()

        assert worker._executor == executor

    def test_only_creates_once(self, worker):

        worker._executor = None
        worker._executor_class = MagicMock()

        assert worker._executor is None

        executor = worker.executor
        other_executor = worker.executor
        worker._executor_class.assert_called_once_with(max_workers=worker.concurrency)
        assert worker._executor is not None
