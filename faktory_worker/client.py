import hashlib
import os
import os.path
import json
import socket
import ssl
import uuid

from typing import Iterator

from datetime import datetime, timedelta

from .exceptions import HandshakeError, AuthenticationError


class Client:
    _last_heartbeat = None
    _worker_id = None

    buffer_size = 4096
    timeout = 5
    is_connected = False
    send_heartbeat_every = 15
    labels = ['python']
    queues = ['default']

    tls_keyfile = "~/.faktory/tls/private.key"
    tls_cert = "~/.faktory/tls/public.crt"

    def __init__(self, host="127.0.0.1", port=7419, password=None):
        self.host = host
        self.port = port
        self.password = password

        self._pending_acks = list()
        self._pending_fails = list()

    def connect(self) -> bool:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.password and self.tls_cert and self.tls_keyfile:
            # TODO: what does this raise?
            self.socket = ssl.wrap_socket(self.socket,
                                          keyfile=os.path.expanduser(self.tls_keyfile),
                                          certfile=os.path.expanduser(self.tls_cert))

        self.socket.settimeout(self.timeout)
        try:
            self.socket.connect((self.host, self.port))
        except ssl.SSLError:
            raise
        
        ahoy = next(self.get_message())
        if not ahoy.startswith("HI "):
            raise HandshakeError("Could not connect to Faktory; expected HI from server, but got '{}'".format(ahoy))

        response = {
            'hostname': socket.gethostname(),
            'wid': self.worker_id,
            'pid': os.getpid(),
            "labels": self.labels
        }

        try:
            handshake = json.loads(ahoy[len("HI "):])
            v = int(handshake['v'])
            if v != 1:
                raise HandshakeError("Could not connect to Faktory; need server version 1, but got '{}'".format(v))
            nonce = handshake.get('s')
            if nonce and self.password:
                response['pwdhash'] = hashlib.sha256(str.encode(self.password) + str.encode(nonce)).hexdigest()
        except (ValueError, TypeError):
            raise HandshakeError("Could not connect to Faktory; expected handshake format")

        self.reply("HELLO", response)

        ok = next(self.get_message())
        if ok != "OK":
            if ok.startswith("ERR") and "invalid password" in ok.lower():
                raise AuthenticationError("Could not connect to Faktory; wrong password")

            raise HandshakeError("Could not connect to Faktory; expected OK from server, but got '{}'".format(ok))

        self._last_heartbeat = datetime.now()
        self.is_connected = True
        return True

    def disconnect(self, force: bool=False):
        pass

    def fetch_jobs(self):
        while True:
            while len(self._pending_acks):
                jid = self._pending_acks.pop()
                self.reply("ACK", {'jid': jid})
                ok = next(self.get_message())

            while len(self._pending_fails):
                jid, exc, msg = self._pending_fails.pop()
                response = {
                    'jid': jid
                }
                if exc:
                    response['errtype'] = exc
                if msg:
                    response['message'] = msg

                self.reply("FAIL", response)
                ok = next(self.get_message())

            if self.should_send_heartbeat:
                self.heartbeat()

            # grab a job to do
            self.reply("FETCH {}".format(" ".join(self.get_queues())))
            job = next(self.get_message())
            if job:
                data = json.loads(job)
                jid = data.get('jid')
                args = data.get('args')
                f = data.get('jobtype')
                self.run_job(f, jid=jid, args=args, custom=data.get('custom', dict()))

    def run_job(self, job: str, jid: str, args: list, custom: dict):
        raise NotImplementedError

    def ack(self, jid: str):
        self._pending_acks.append(jid)

    def fail(self, jid: str, exception=None):
        if exception is not None:
            self._pending_fails.append((jid, type(exception).__name__, str(exception)))
        else:
            self._pending_fails.append((jid, None, None))

    @property
    def should_send_heartbeat(self) -> bool:
        return datetime.now() > (self._last_heartbeat + timedelta(seconds=self.send_heartbeat_every))

    def heartbeat(self):
        self.reply("BEAT", {'wid': self.worker_id})
        ok = next(self.get_message())
        if ok:
            # heartbeat failed
            pass
        self._last_heartbeat = datetime.now()

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
                        print("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == '-':
                        resp = line[1:].decode().strip("\r\n ")
                        print("> {}".format(resp))
                        yield resp
                    elif chr(line[0]) == '$':
                        # read $xxx bytes of data into a buffer
                        number_of_bytes = int(line[1:]) + 2  # add 2 bytes so we read the \r\n from the end
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
                        print("> {}".format(resp))
                        yield resp
                else:
                    more = socket.recv(self.buffer_size)
                    if not more:
                        buffering = False
                    else:
                        buffer += more

    def publish(self, job, args=None, custom=None, queue='default', reserve_for=None, at=None, retry=None) -> bool:
        request = {
            'jid': self.random_job_id(),
            'queue': queue,
            'jobtype': job
        }

        if custom is not None:
            request['custom'] = custom

        if args is not None:
            request['args'] = args

        if reserve_for is not None:
            request['reserve_for'] = reserve_for

        if at is not None:
            request['at'] = at

        if retry is not None:
            request['retry'] = retry

        self.reply("PUSH", request)
        ok = next(self.get_message())
        return ok == "OK"

    def reply(self, cmd, data=None):
        print("< {} {}".format(cmd, data or ""))
        s = cmd
        if data is not None:
            if type(data) is dict:
                s = "{} {}".format(s, json.dumps(data))
            else:
                s = "{} {}".format(s, data)
        self.socket.send(str.encode(s + "\r\n"))

    def random_job_id(self) -> str:
        return uuid.uuid4().hex

    def get_queues(self):
        return self.queues

    @property
    def worker_id(self) -> str:
        self._worker_id = self._worker_id or uuid.uuid4().hex
        return self._worker_id
