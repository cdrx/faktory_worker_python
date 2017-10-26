import json
import socket
from typing import Iterator
import uuid

import time

BROKER = "localhost:7420"


class FaktoryServer:
    host = "localhost"
    port = 7419
    password = None

    _buffer_size = 4  # 4096

    def __init__(self, server: str=None, password: str=None):
        if server:
            if ":" in server:
                host, port = server.split(":", maxsplit=2)
                self.host = host
                self.port = int(self.port)
            else:
                self.host = server
        self.password = password

    def bail(self, s):
        print(s)
        panic

    def connect(self):
        if self.password:
            # TODO: tls stuff
            pass

        self.socket = sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))

        ahoy = next(self.get_message(sock))
        if not ahoy.startswith("HI "):
            self.bail("Expected HI from server")
        else:
            # TODO: auth
            self.reply("HELLO", json.dumps({
                'hostname': "blah",
                'wid': "1",
                'pid': 123,
                "labels": ["python"]
            }))

        ok = next(self.get_message(sock))
        if ok != "OK":
            self.bail("Expected OK, got: {}".format(ok))

        while True:
            # TODO: send pending work results

            # grab a job to do
            self.reply("FETCH {}".format(" ".join(self.get_queues())))
            job = next(self.get_message(sock))
            if job:
                self.process(job)


        # for msg in self.get_message(sock):
        #     print("> {}".format(msg))
        #     if msg.startswith("HI "):
        #
        #         response = self.get_message(sock)
        #         if response != "OK":
        #             self.bail()
        #     print(msg)

        #
        # for line in sock.makefile('r', encoding='utf8'):
        #     self.cmd(line)

    def process(self, job: str):
        data = json.loads(job)
        jobid = data.get('jid')
        print("Running job {}".format(jobid))  #, json.loads(job))

        # TODO: do work
        #time.sleep(0.5)

        self.reply("ACK", json.dumps({'jid': jobid}))
        ok = False
        while not ok:
            ok = next(self.get_message(self.socket))
            if not ok:
                self.bail("skipped empty")

    def get_message(self, socket: socket) -> Iterator[str]:
        buffer = socket.recv(self._buffer_size)
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
                    more = socket.recv(self._buffer_size)
                    if not more:
                        buffering = False
                    else:
                        buffer += more

    def publish(self, job, args=None, custom=None, queue='default', reserve_for=None, at=None, retry=None):
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

    # def cmd(self, msg):
    #     msg = msg.strip()
    #     print("> {}".format(msg))
    #     if " " in msg:
    #         cmd, data = msg.split(" ", maxsplit=2)
    #     else:
    #         cmd = msg
    #
    #     if cmd == "+HI":
    #         self.reply("HELLO", json.dumps({
    #             'hostname': "blah",
    #             'wid': "1",
    #             'pid': 123,
    #             "labels": ["python"]
    #         }))
    #
    #     if cmd == "+OK" or cmd == "$0":
    #         if not self._working:
    #             self._working = True
    #             # request a job
    #             #self.publish("test", args=[1,2,3])
    #             self.reply("FETCH {}".format(" ".join(self.get_queues())))
    #         #else:
    #             #self.publish("test", args=[1, 2, 3])

    def reply(self, cmd, data=None):
        print("< {} {}".format(cmd, data or ""))
        s = cmd
        if data is not None:
            s = "{} {}".format(s, data)
        self.socket.send(str.encode(s + "\r\n"))

    def random_job_id(self) -> str:
        return str(uuid.uuid4())


w = Worker(server="localhost:7419")
w.start()

