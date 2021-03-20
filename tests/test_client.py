import json
from typing import Iterable
from unittest.mock import MagicMock

import pytest

from faktory._proto import Connection
from faktory.client import Client


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
def client(conn) -> Client:
    return Client(connection=conn)


def test_constructor_uses_existing_conn(conn):

    client = Client(connection=conn)
    assert client.faktory == conn


def test_creates_connection_from_url():

    client = Client("tcp://a-server:7419")
    assert client.faktory.host == "a-server"
    assert client.faktory.port == 7419


def test_connect_connects_to_server(client):

    assert client.is_connected == False

    was_successful = client.connect()
    assert was_successful == True

    client.faktory.connect.assert_called_once()


def test_disconnect_disconnects_from_server(client):

    assert client.is_connected == False

    was_successful = client.connect()
    assert was_successful == True

    client.faktory.connect.assert_called_once()

    client.disconnect()

    client.faktory.disconnect.assert_called_once()


def test_context_manager_no_errors(client):

    assert client.is_connected == False

    with client:
        client.faktory.connect.assert_called_once()
        assert client.is_connected == True

    client.faktory.disconnect.assert_called_once()
    assert client.is_connected == False


def test_context_manager_closes_on_error(client):
    assert client.is_connected == False

    try:
        with client:
            client.faktory.connect.assert_called_once()
            assert client.is_connected == True
            raise ValueError("An error!")

    except ValueError:
        pass

    client.faktory.disconnect.assert_called_once()
    assert client.is_connected == False


def test_random_job_id(client):

    random_id = client.random_job_id()
    second_random_id = client.random_job_id()

    assert random_id != second_random_id


def test_job_id_is_serializable(client):

    random_id = client.random_job_id()
    # Will raise an error
    json.dumps(random_id)


class TestClientQueue:
    def test_can_queue_job(self, client: Client):

        was_successful = client.queue("test", args=(1, 2))
        assert was_successful
        client.faktory.reply.assert_called_once()
        client.faktory.get_message.assert_called_once()
        assert client.faktory.reply.call_args[0][0] == "PUSH"

        request = client.faktory.reply.call_args[0][1]
        assert request["jobtype"] == "test"
        assert request["args"] == [1, 2]
        assert request["jid"] is not None
        assert request["queue"] == "default"

    def test_can_queue_job_with_generator(self, client: Client):
        # Split out in another test because generator gets consumed
        # when being cast to list, so we can't use it in parametrize
        was_successful = client.queue("test", args=(val for val in [1, 2]))
        assert was_successful
        client.faktory.reply.assert_called_once()
        client.faktory.get_message.assert_called_once()
        assert client.faktory.reply.call_args[0][0] == "PUSH"

        request = client.faktory.reply.call_args[0][1]
        assert request["jobtype"] == "test"
        assert request["args"] == [1, 2]
        assert request["jid"] is not None
        assert request["queue"] == "default"

    @pytest.mark.parametrize(
        "args",
        [(1, "value"), [1, "value"], {1, "value"}],
    )
    @pytest.mark.parametrize("queue", ["default", "not default"])
    @pytest.mark.parametrize("num_retries", [1, 2])
    @pytest.mark.parametrize("job_priority", [3, 4])
    @pytest.mark.parametrize("job_name", ["test_job", "test_job_2"])
    @pytest.mark.parametrize("job_id", ["test_job_id", "test_job_id_2"])
    def test_uses_inputs_for_job_submission(
        self,
        client: Client,
        job_id: str,
        job_name: str,
        job_priority: int,
        num_retries: int,
        queue: str,
        args: Iterable,
    ):
        was_successful = client.queue(
            task=job_name, args=args, queue=queue, jid=job_id, priority=job_priority
        )
        assert was_successful
        client.faktory.reply.assert_called_once()
        client.faktory.get_message.assert_called_once()
        assert client.faktory.reply.call_args[0][0] == "PUSH"
        request = client.faktory.reply.call_args[0][1]
        assert request["jobtype"] == job_name
        assert request["args"] == list(args)
        assert request["jid"] == job_id
        assert request["queue"] == queue
        assert request["priority"] == job_priority

    def test_disconnects_if_not_connected(self, client: Client):
        client.connect = MagicMock()
        client.disconnect = MagicMock()
        was_successful = client.queue("test", args=(1, 2))

        client.connect.assert_called_once()
        client.disconnect.assert_called_once()

    def test_stays_connected_if_connected(self, client: Client):
        client.connect()
        client.disconnect = MagicMock()
        was_successful = client.queue("test", args=(1, 2))

        client.disconnect.assert_not_called()

    def test_requires_task_name(self, client: Client):

        with pytest.raises(ValueError):
            client.queue(None, args=(1, 2))

    def test_requires_queue_name(self, client: Client):
        with pytest.raises(ValueError):
            client.queue("test", queue=None)

    def test_requires_sequence_args(self, client: Client):

        with pytest.raises(ValueError):
            client.queue("test", args="will error because not sequence")
