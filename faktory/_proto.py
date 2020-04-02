import hashlib
import json
import logging
import os
import os.path
import select
import socket
import ssl
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import urlparse

from .exceptions import (
    FaktoryAuthenticationError,
    FaktoryConnectionResetError,
    FaktoryHandshakeError,
)


class Connection:
    buffer_size = 4096
    timeout = 30
    use_tls = False
    send_heartbeat_every = 15
    labels = ["python"]
    queues = ["default"]
    debug = False

    is_connected = False
    is_connecting = False
    is_quiet = False
    is_disconnecting = False
    disconnection_requested = None
    force_disconnection_after = None

    def __init__(
        self,
        faktory: Optional[str] = None,
        timeout: int = 30,
        buffer_size: int = 4096,
        worker_id=None,
        labels: List[str] = None,
        log: logging.Logger = None,
    ):
        if not faktory:
            faktory = os.environ.get("FAKTORY_URL", "tcp://localhost:7419")

        url = urlparse(faktory)
        self.host = url.hostname
        self.port = url.port or 7419
        self.password = url.password

        if "tls" in url.scheme:
            self.use_tls = True

        self.timeout = timeout
        self.buffer_size = buffer_size

        self.labels = labels
        if not self.labels:
            self.labels = []

        self.worker_id = worker_id
        self.socket = None

        self.log = log or logging.getLogger(name="faktory.connection")

    def connect(self, worker_id=None) -> bool:
        self.log.info("Connecting to {}:{}".format(self.host, self.port))
        self.is_connecting = True

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.use_tls:
            self.log.debug("Using TLS")
            self.socket = ssl.wrap_socket(self.socket)

        self.socket.setblocking(0)
        self.socket.settimeout(self.timeout)
        try:
            self.socket.connect((self.host, self.port))
        except ssl.SSLError:
            raise

        ahoy = next(self.get_message())
        if not ahoy.startswith("HI "):
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected HI from server, but got '{}'".format(
                    ahoy
                )
            )

        response = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "labels": self.labels,
        }

        if worker_id:
            response["wid"] = self.worker_id

        try:
            handshake = json.loads(ahoy[len("HI ") :])
            version = int(handshake["v"])
            if not self.is_supported_server_version(version):
                self.socket.close()
                raise FaktoryHandshakeError(
                    "Could not connect to Faktory; unsupported server version {}".format(
                        version
                    )
                )

            nonce = handshake.get("s")
            if nonce and self.password:
                response["pwdhash"] = hashlib.sha256(
                    str.encode(self.password) + str.encode(nonce)
                ).hexdigest()
        except (ValueError, TypeError):
            self.socket.close()
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected handshake format"
            )

        self.reply("HELLO", response)

        ok = next(self.get_message())
        if ok != "OK":
            if ok.startswith("ERR") and "invalid password" in ok.lower():
                self.socket.close()
                raise FaktoryAuthenticationError(
                    "Could not connect to Faktory; wrong password"
                )
            self.socket.close()
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected OK from server, but got '{}'".format(
                    ok
                )
            )

        self.log.debug("Connected to Faktory")

        self.is_connected, self.is_connecting = True, False
        return self.is_connected

    def _validate_handshake(self, payload: Dict[str, Any]) -> None:
        """
        Helper function designed to authenticate with the Faktory
        server and validate the connection is as expected.

        Args:
            - payload (Dict[str, Any]): Response from sending the initial `Hello` message. Payload
                is expected to have the following keys: `[hostname, pid, labels]` and
                optionally have the following keys: `[worker_id, pwdhash]`

        Raises:
            - FaktoryHandshakeError: Raised when receiving an unexpected response
                in the handshake message from the server.
            - FaktoryAuthenticationError: Raised when an invalid password is supplied.

        Response:
            - None
        """
        self.reply("HELLO", payload)

        ok = next(self.get_message())
        if ok != "OK":
            if ok.startswith("ERR") and "invalid password" in ok.lower():
                self.socket.close()
                raise FaktoryAuthenticationError(
                    "Could not connect to Faktory; wrong password"
                )
            self.socket.close()
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected OK from server, but got '{}'".format(
                    ok
                )
            )

        self.log.debug("Connected to Faktory")

    def validate_connection(self, send_worker_id: bool = False) -> None:
        """
        Connects to the Faktory job server and validates the connection.

        Args:
            - send_worker_id (bool): Flag of whether to send the worker_id to the
                Faktory server.

        Raises:
            - FaktoryHandshakeError: Raised if a message with an unexpected
                format is received from the job server.
            - FaktoryHandshakeError: Raised if the job server's version
                isn't supported by this client.
            - FaktoryAuthenticationError: Raised if invalid credentials are
                used when trying to authenticate with the job server.

        Returns:
            - None
        """
        ahoy = next(self.get_message())
        if not ahoy.startswith("HI "):
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected HI from server, but got '{}'".format(
                    ahoy
                )
            )

        response = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "labels": self.labels,
        }

        if send_worker_id:
            response["wid"] = self.worker_id

        try:
            handshake = json.loads(ahoy[len("HI ") :])
            version = int(handshake["v"])
            if not self.is_supported_server_version(version):
                self.socket.close()
                raise FaktoryHandshakeError(
                    "Could not connect to Faktory; unsupported server version {}".format(
                        version
                    )
                )

            nonce = handshake.get("s")
            if nonce and self.password:
                response["pwdhash"] = hashlib.sha256(
                    str.encode(self.password) + str.encode(nonce)
                ).hexdigest()
        except (ValueError, TypeError):
            self.socket.close()
            raise FaktoryHandshakeError(
                "Could not connect to Faktory; expected handshake format"
            )
        self._validate_handshake(response)

        self.is_connected, self.is_connecting = True, False
        return self.is_connected

    def is_supported_server_version(self, v: int) -> bool:
        return v == 2

    def get_message(self) -> Iterator[str]:
        socket = self.socket
        buffer = self.select_data(self.buffer_size)
        while self.is_connected or self.is_connecting:
            buffering = True
            while buffering:
                if buffer.count(b"\r\n"):
                    (line, buffer) = buffer.split(b"\r\n", 1)
                    if len(line) == 0:
                        continue
                    elif chr(line[0]) == "+":
                        resp = line[1:].decode().strip("\r\n ")
                        if self.debug:
                            self.log.debug("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == "-":
                        resp = line[1:].decode().strip("\r\n ")
                        if self.debug:
                            self.log.debug("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == "$":
                        # read $xxx bytes of data into a buffer
                        number_of_bytes = (
                            int(line[1:]) + 2
                        )  # add 2 bytes so we read the \r\n from the end
                        if number_of_bytes <= 1:
                            if self.debug:
                                self.log.debug("> {}".format("nil"))
                            yield None
                        else:
                            if len(buffer) >= number_of_bytes:
                                # we've already got enough bytes in the buffer
                                data = buffer[:number_of_bytes]
                                buffer = buffer[number_of_bytes:]
                            else:
                                data = buffer
                                while len(data) != number_of_bytes:
                                    bytes_required = number_of_bytes - len(data)
                                    data += self.select_data(bytes_required)
                                buffer = []
                            resp = data.decode().strip("\r\n ")
                            if self.debug:
                                self.log.debug("> {}".format(resp))
                            yield resp
                else:
                    more = self.select_data(self.buffer_size)
                    if not more:
                        buffering = False
                    else:
                        buffer += more

    def select_data(self, buffer_size: int):
        s = self.socket
        ready = select.select([s], [], [], self.timeout)
        if ready[0]:
            buffer = s.recv(buffer_size)
            if len(buffer) > 0:
                return buffer
        self.disconnect()
        raise FaktoryConnectionResetError

    def fetch(self, queues: Iterable[str]) -> Optional[dict]:
        self.reply("FETCH {}".format(" ".join(queues)))
        job = next(self.get_message())
        if not job:
            return None

        data = json.loads(job)
        return data

    def reply(self, cmd: str, data: Any = None):
        if self.debug:
            self.log.debug("< {} {}".format(cmd, data or ""))
        s = cmd
        if data is not None:
            if type(data) is dict:
                s = "{} {}".format(s, json.dumps(data))
            else:
                s = "{} {}".format(s, data)
        self.socket.send(str.encode(s + "\r\n"))

    def disconnect(self):
        self.log.info("Disconnected")
        self.socket.close()
        self.is_connected = False
