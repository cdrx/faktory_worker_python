from typing import Iterator, Optional

import logging
import hashlib
import os
import os.path
import json
import socket
import ssl

from urllib.parse import urlparse

from . exceptions import FaktoryHandshakeError, FaktoryAuthenticationError


class Connection:
    buffer_size = 4096
    timeout = 30
    use_tls = False
    send_heartbeat_every = 15
    labels = ['python']
    queues = ['default']
    debug = False

    is_connected = False
    is_quiet = False
    is_disconnecting = False
    disconnection_requested = None
    force_disconnection_after = None

    def __init__(self, faktory=None, timeout=30, buffer_size=4096, worker_id=None, labels=None, log=None):
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

        self.log = log or logging.getLogger(name='faktory.connection')

    def connect(self, worker_id: str=None) -> bool:
        self.log.info("Connecting to {}:{}".format(self.host, self.password))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.use_tls:
            self.log.debug("Using TLS")
            self.socket = ssl.wrap_socket(self.socket)

        self.socket.settimeout(self.timeout)
        try:
            self.socket.connect((self.host, self.port))
        except ssl.SSLError:
            raise

        ahoy = next(self.get_message())
        if not ahoy.startswith("HI "):
            raise FaktoryHandshakeError("Could not connect to Faktory; expected HI from server, but got '{}'".format(ahoy))

        response = {
            'hostname': socket.gethostname(),
            'pid': os.getpid(),
            "labels": self.labels
        }

        if worker_id:
            response['wid'] = self.worker_id

        try:
            handshake = json.loads(ahoy[len("HI "):])
            version = int(handshake['v'])
            if not self.is_supported_server_version(version):
                self.socket.close()
                raise FaktoryHandshakeError("Could not connect to Faktory; unsupported server version {}".format(version))

            nonce = handshake.get('s')
            if nonce and self.password:
                response['pwdhash'] = hashlib.sha256(str.encode(self.password) + str.encode(nonce)).hexdigest()
        except (ValueError, TypeError):
            self.socket.close()
            raise FaktoryHandshakeError("Could not connect to Faktory; expected handshake format")

        self.reply("HELLO", response)

        ok = next(self.get_message())
        if ok != "OK":
            if ok.startswith("ERR") and "invalid password" in ok.lower():
                self.socket.close()
                raise FaktoryAuthenticationError("Could not connect to Faktory; wrong password")
            self.socket.close()
            raise FaktoryHandshakeError("Could not connect to Faktory; expected OK from server, but got '{}'".format(ok))

        self.log.debug("Connected to Faktory")

        self.is_connected = True
        return self.is_connected

    def is_supported_server_version(self, v: int):
        return v == 2

    def get_message(self) -> Iterator[str]:
        socket = self.socket
        buffer = socket.recv(self.buffer_size)
        while True:
            buffering = True
            while buffering:
                if buffer.count(b'\r\n'):
                    (line, buffer) = buffer.split(b"\r\n", 1)
                    if len(line) == 0:
                        continue
                    elif chr(line[0]) == '+':
                        resp = line[1:].decode().strip("\r\n ")
                        if self.debug: self.log.debug("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == '-':
                        resp = line[1:].decode().strip("\r\n ")
                        if self.debug: self.log.debug("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == '$':
                        # read $xxx bytes of data into a buffer
                        number_of_bytes = int(line[1:]) + 2  # add 2 bytes so we read the \r\n from the end
                        if number_of_bytes <= 1:
                            if self.debug: self.log.debug("> {}".format("nil"))
                            yield None
                        else:
                            if len(buffer) >= number_of_bytes:
                                # we've already got enough bytes in the buffer
                                data = buffer[:number_of_bytes]
                                buffer = buffer[number_of_bytes:]
                            else:
                                data = buffer
                                bytes_required = number_of_bytes - len(data)
                                data += socket.recv(bytes_required)
                                buffer = []
                            resp = data.decode().strip("\r\n ")
                            if self.debug: self.log.debug("> {}".format(resp))
                            yield resp
                else:
                    more = socket.recv(self.buffer_size)
                    if not more:
                        buffering = False
                    else:
                        buffer += more

    def fetch(self, queues) -> Optional[dict]:
        self.reply("FETCH {}".format(" ".join(queues)))
        job = next(self.get_message())
        if not job:
            return None

        data = json.loads(job)
        return data

    def reply(self, cmd, data=None):
        if self.debug: self.log.debug("< {} {}".format(cmd, data or ""))
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
