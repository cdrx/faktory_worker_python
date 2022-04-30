import hashlib
import io
import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call

import pytest
from faktory._proto import Connection
from faktory.exceptions import (
    FaktoryAuthenticationError,
    FaktoryConnectionResetError,
    FaktoryHandshakeError,
)


class TestConnectionConstructor:
    def test_init(self):
        server_url = "tcp://a-server:7419"
        timeout = 45
        buffer_size = 2048
        worker_id = "a custom worker id"
        labels = ["label 1", "label 2"]

        conn = Connection(
            faktory=server_url,
            timeout=timeout,
            buffer_size=buffer_size,
            worker_id=worker_id,
            labels=labels,
        )
        assert conn.host == "a-server"
        assert conn.port == 7419
        assert conn.password is None
        assert conn.timeout == timeout
        assert conn.buffer_size == buffer_size
        assert conn.worker_id == worker_id
        assert conn.labels == labels

    def test_ignores_env_if_specified(self, monkeypatch):
        env_value = "tcp://other-server:5000"
        monkeypatch.setenv("FAKTORY_URL", env_value)

        conn = Connection("tcp://real-server:6000")

        assert conn.host == "real-server"
        assert conn.port == 6000

    def test_uses_env_value_if_exists_and_not_overriden(self, monkeypatch):
        env_value = "tcp://other-server:5000"
        monkeypatch.setenv("FAKTORY_URL", env_value)

        conn = Connection()

        assert conn.host == "other-server"
        assert conn.port == 5000

    def test_has_default_if_not_provided_or_in_env(self, monkeypatch):
        monkeypatch.delenv("FAKTORY_URL", raising=False)

        conn = Connection()
        assert conn.host is not None
        assert conn.port is not None

    def test_uses_empty_list_when_not_given_labels(self):
        conn = Connection()
        assert conn.labels == []

    def test_parses_server_information_from_url(self):
        conn = Connection("tcp://:Password123!@localhost:7419")
        assert conn.host == "localhost"
        assert conn.password == "Password123!"
        assert conn.port == 7419


class TestConnectionValidateConnection:
    def test_raises_handshake_error_on_bad_initial_connection(self, monkeypatch):
        payload = {"key": "value"}
        mocked_socket = MagicMock()
        mocked_get_message = MagicMock(
            return_value=iter(["A message that doesn't start with HI"])
        )

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        # Doing nothing symbolizes success
        with pytest.raises(FaktoryHandshakeError):
            conn.validate_connection()

    def test_raises_error_on_bad_handshake_types(self, monkeypatch):
        """
        Based on the existing source code, the requested data will
        have the same structure, but should raise an error on
        incorrectly typed values.
        """
        payload = {"v": "a non integer value"}
        mocked_socket = MagicMock()
        mocked_get_message = MagicMock(
            return_value=iter(["HI {}".format(json.dumps(payload))])
        )

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)

        with pytest.raises(FaktoryHandshakeError):
            conn.validate_connection()

    @pytest.mark.parametrize("server_payload", [{"v": 2}, {"v": 2, "s": str(500_000)}])
    def test_successful_handshake(self, monkeypatch, server_payload: Dict):
        """
        This test only covers the bare minimum to make the code
        succeed, and should also be fed actual values
        that the Faktory server produces.
        """
        host_name = "testing host"
        pid = 5
        mocked_socket = MagicMock()

        mocked_get_message = MagicMock(
            return_value=iter(["HI {}".format(json.dumps(server_payload))])
        )
        mocked_validate_handshake = MagicMock()

        conn = Connection(labels=["test suite"])
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        monkeypatch.setattr(conn, "_validate_handshake", mocked_validate_handshake)
        # Mocked calls based on environment
        monkeypatch.setattr(
            "faktory._proto.socket.gethostname", MagicMock(return_value=host_name)
        )
        monkeypatch.setattr("faktory._proto.os.getpid", MagicMock(return_value=pid))

        conn.validate_connection()

        handshake_response = mocked_validate_handshake.call_args[0][0]
        assert handshake_response["hostname"] == host_name
        assert handshake_response["pid"] == pid
        assert handshake_response["labels"] == conn.labels

    @pytest.mark.parametrize("send_worker_id", [True, False])
    def test_listens_to_worker_id_flag(self, send_worker_id: bool, monkeypatch):
        """This test should ensure that the `send_worker_id` flag is respected."""
        server_payload = {"v": 2}
        host_name = "testing host"
        pid = 5
        worker_id = "unit test worker"
        mocked_socket = MagicMock()

        mocked_get_message = MagicMock(
            return_value=iter(["HI {}".format(json.dumps(server_payload))])
        )
        mocked_validate_handshake = MagicMock()

        conn = Connection(labels=["test suite"], worker_id=worker_id)
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        monkeypatch.setattr(conn, "_validate_handshake", mocked_validate_handshake)
        # Mocked calls based on environment
        monkeypatch.setattr(
            "faktory._proto.socket.gethostname", MagicMock(return_value=host_name)
        )
        monkeypatch.setattr("faktory._proto.os.getpid", MagicMock(return_value=pid))

        conn.validate_connection(send_worker_id=send_worker_id)

        handshake_response = mocked_validate_handshake.call_args[0][0]
        if send_worker_id:
            assert "wid" in handshake_response
            assert handshake_response["wid"] == worker_id
        else:
            assert "wid" not in handshake_response

    @pytest.mark.parametrize("nonce", ["1234", None])
    def test_uses_nonce_if_available(self, nonce: Optional[str], monkeypatch):
        """
        This test should ensure if a nonce is present while initializing
        the connection to the server, its used as they key to sign the
        password.
        """
        server_payload = {"v": 2, "s": nonce}
        host_name = "testing host"
        pid = 5
        worker_id = "unit test worker"
        mocked_socket = MagicMock()
        password = "Password123!"
        faktory_url = "tcp://:{}@localhost:7419".format(password)

        mocked_get_message = MagicMock(
            return_value=iter(["HI {}".format(json.dumps(server_payload))])
        )
        mocked_validate_handshake = MagicMock()

        conn = Connection(faktory_url, labels=["test suite"])
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        monkeypatch.setattr(conn, "_validate_handshake", mocked_validate_handshake)
        # Mocked calls based on environment
        monkeypatch.setattr(
            "faktory._proto.socket.gethostname", MagicMock(return_value=host_name)
        )
        monkeypatch.setattr("faktory._proto.os.getpid", MagicMock(return_value=pid))

        conn.validate_connection()

        handshake_response = mocked_validate_handshake.call_args[0][0]

        if not nonce:
            assert "pwdhash" not in handshake_response
        else:
            assert "pwdhash" in handshake_response
            hashed_pw = handshake_response["pwdhash"]
            assert hashed_pw != password
            expected_password = hashlib.sha256(
                password.encode() + nonce.encode()
            ).hexdigest()
            assert expected_password == hashed_pw


class TestValidateHandshake:
    def test_valid_response_returns_none(self, monkeypatch):
        payload = {"key": "value"}
        mocked_socket = MagicMock()
        mocked_reply = MagicMock()
        mocked_get_message = MagicMock(return_value=iter(["OK"]))

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "reply", mocked_reply)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        # Doing nothing symbolizes success
        conn._validate_handshake(payload)

    def test_raises_auth_error_on_bad_password(self, monkeypatch):
        payload = {"key": "value"}
        mocked_socket = MagicMock()
        mocked_reply = MagicMock()
        # Minimum error message we need to get it to raise the error
        err_message = "ERR: invalid password"
        mocked_get_message = MagicMock(return_value=iter([err_message]))
        mocked_close = MagicMock()
        mocked_socket.close = mocked_close

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "reply", mocked_reply)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        with pytest.raises(FaktoryAuthenticationError):
            conn._validate_handshake(payload)

        mocked_close.assert_called_once()

    def test_raises_handshake_error_on_misc_error(self, monkeypatch):
        payload = {"key": "value"}
        mocked_socket = MagicMock()
        mocked_reply = MagicMock()
        mocked_get_message = MagicMock(
            return_value=iter(["ERR: A different, non auth error occurred"])
        )

        mocked_close = MagicMock()
        mocked_socket.close = mocked_close

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "reply", mocked_reply)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)
        with pytest.raises(FaktoryHandshakeError):
            conn._validate_handshake(payload)

        mocked_close.assert_called_once()

    @pytest.mark.parametrize(
        "payload",
        [
            {"key": "value"},
            {"hostname": "tests", "pid": 5, "labels": ["tests"], "wid": "test worker"},
            {"key1": "value1", "key2": "value2"},
        ],
    )
    def test_uses_payload(self, monkeypatch, payload: Dict[str, Any]):
        mocked_socket = MagicMock()
        mocked_reply = MagicMock()
        mocked_get_message = MagicMock(return_value=iter(["OK"]))

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr(conn, "reply", mocked_reply)
        monkeypatch.setattr(conn, "get_message", mocked_get_message)

        conn._validate_handshake(payload)

        mocked_reply.assert_called_once()
        mocked_get_message.assert_called_once()

        used_payload = mocked_reply.call_args[0][1]
        assert used_payload == payload


class TestConnectionSelectData:
    def test_returns_buffer_if_exists(self, monkeypatch):
        mocked_socket = MagicMock()

        mocked_select = MagicMock(return_value=[True, False, False])

        socket_received_data = b"I'm the mocked out data!"
        mocked_recv = MagicMock(return_value=socket_received_data)
        mocked_socket.recv = mocked_recv

        conn = Connection()

        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr("faktory._proto.select.select", mocked_select)

        data = conn.select_data(buffer_size=5_000)
        mocked_recv.assert_called_once()
        assert data == b"I'm the mocked out data!"

    def test_fetches_provided_buffer_size(self, monkeypatch):
        mocked_socket = MagicMock()

        mocked_select = MagicMock(return_value=[True, False, False])

        mocked_recv = MagicMock(return_value=b"I'm the mocked out data!")
        mocked_socket.recv = mocked_recv

        conn = Connection()

        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr("faktory._proto.select.select", mocked_select)

        data = conn.select_data(buffer_size=5_000)
        mocked_recv.assert_called_once_with(5_000)

    def test_raises_error_on_empty_buffer(self, monkeypatch):
        """
        Tests to make sure that a "read ready" socket that
        has an empty buffer causes the connection to close
        and the connection reset to be raised.
        """
        mocked_socket = MagicMock()
        mocked_disconnect = MagicMock()

        mocked_select = MagicMock(return_value=[True, False, False])

        socket_received_data = b""
        mocked_recv = MagicMock(return_value=socket_received_data)
        mocked_socket.recv = mocked_recv

        conn = Connection()

        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr("faktory._proto.select.select", mocked_select)
        monkeypatch.setattr(conn, "disconnect", mocked_disconnect)

        with pytest.raises(FaktoryConnectionResetError):
            data = conn.select_data(buffer_size=5_000)

        mocked_recv.assert_called_once()
        mocked_disconnect.assert_called_once()

    def test_raises_error_if_socket_not_ready(self, monkeypatch):
        mocked_socket = MagicMock()
        mocked_disconnect = MagicMock()

        mocked_select = MagicMock(return_value=[False, False, False])

        socket_received_data = b"I'm the mocked out data!"
        mocked_recv = MagicMock(return_value=socket_received_data)
        mocked_socket.recv = mocked_recv

        conn = Connection()

        monkeypatch.setattr(conn, "socket", mocked_socket)
        monkeypatch.setattr("faktory._proto.select.select", mocked_select)
        monkeypatch.setattr(conn, "disconnect", mocked_disconnect)

        with pytest.raises(FaktoryConnectionResetError):
            data = conn.select_data(buffer_size=5_000)

        mocked_recv.assert_not_called()
        mocked_disconnect.assert_called_once()


class TestConnectionFetch:
    @pytest.mark.parametrize(
        "queues",
        [["default"], ["default", "important"], ["important", "not_important"]],
    )
    def test_fetches_using_queues(self, monkeypatch, queues: List[str]):
        mock_reply = MagicMock()
        mock_get_message = MagicMock(return_value=iter([None]))
        conn = Connection()

        monkeypatch.setattr(conn, "reply", mock_reply)
        monkeypatch.setattr(conn, "get_message", mock_get_message)

        conn.fetch(queues)

        mock_reply.assert_called_once()
        mock_get_message.assert_called_once()

        reply_args = mock_reply.call_args[0][0]
        assert "FETCH" in reply_args
        for queue in queues:
            assert queue in reply_args

    def test_returns_none_if_no_job(self, monkeypatch):
        mock_reply = MagicMock()
        mock_get_message = MagicMock(return_value=iter([None]))
        conn = Connection()

        monkeypatch.setattr(conn, "reply", mock_reply)
        monkeypatch.setattr(conn, "get_message", mock_get_message)

        queues = ["default"]
        result = conn.fetch(queues)

        mock_reply.assert_called_once()
        mock_get_message.assert_called_once()
        assert result is None

    def test_returns_deserialized_job_info_if_present(self, monkeypatch):
        data = {
            "wid": "test worker",
            "pid": 500,
            "labels": ["tests"],
            "hostname": "test suite",
        }
        serialized_data = json.dumps(data)
        mock_reply = MagicMock()
        mock_get_message = MagicMock(return_value=iter([serialized_data]))
        conn = Connection()

        monkeypatch.setattr(conn, "reply", mock_reply)
        monkeypatch.setattr(conn, "get_message", mock_get_message)

        queues = ["default"]
        result = conn.fetch(queues)

        mock_reply.assert_called_once()
        mock_get_message.assert_called_once()
        assert result == data


class TestConnectionIsSupportedServerVersion:
    def test_supported_version(self):
        conn = Connection()

        result = conn.is_supported_server_version(2)
        assert result == True

    @pytest.mark.parametrize("version", [1, 3, 4, 5])
    def test_unsupported_version(self, version: int):
        conn = Connection()

        result = conn.is_supported_server_version(version)
        assert result == False


class TestConnectionReply:
    @pytest.mark.parametrize("data", [None, "string data", {"data": "dict data"}])
    def test_sends_bytes(self, monkeypatch, data):
        test_command = "test command"
        mocked_socket = MagicMock()
        mock_send = MagicMock(return_value=len(test_command) + 2)
        mocked_socket.send = mock_send

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        conn.reply(test_command)

        mock_send.assert_called_once()
        assert isinstance(mock_send.call_args[0][0], bytes)
        decoded_value = mock_send.call_args[0][0].decode()
        assert decoded_value == "test command\r\n"

    @pytest.mark.parametrize("data", [None, "string data", {"data": "dict data"}])
    def test_adds_return_and_newline_to_payload(self, monkeypatch, data):
        test_command = "test command"
        mocked_socket = MagicMock()
        mock_send = MagicMock(return_value=len(test_command) + 2)
        mocked_socket.send = mock_send

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        conn.reply(test_command)
        called_with = mock_send.call_args[0][0]
        decoded = called_with.decode()

        assert "\r\n" == decoded[-2:]

    @pytest.mark.parametrize("data", [None, "string data", {"data": "dict data"}])
    def test_sends_until_out_of_data(self, monkeypatch, data):
        mocked_socket = MagicMock()
        mock_send = MagicMock(return_value=7)
        mocked_socket.send = mock_send

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        conn.reply("test command")

        calls = [call(b"test command\r\n"), call(b"mmand\r\n")]
        mock_send.assert_has_calls(calls)

    @pytest.mark.parametrize("data", [None, "string data", {"data": "dict data"}])
    def test_send_zero_raises_error(self, monkeypatch, data):
        mocked_socket = MagicMock()
        mock_send = MagicMock(return_value=0)
        mocked_socket.send = mock_send

        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        with pytest.raises(FaktoryConnectionResetError):
            conn.reply("test command")


class TestConnectionDisconnect:
    def test_closes_socket(self, monkeypatch):
        mocked_socket = MagicMock()
        mock_close = MagicMock()
        mocked_socket.close = mock_close
        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        conn.disconnect()

        mock_close.assert_called_once()

    def test_sets_connection_flag_to_false(self, monkeypatch):
        mocked_socket = MagicMock()
        conn = Connection()
        monkeypatch.setattr(conn, "socket", mocked_socket)

        conn.is_connected = True
        conn.disconnect()

        assert conn.is_connected == False
