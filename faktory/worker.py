import logging
import signal
import sys
import time
import uuid
from collections import namedtuple
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from concurrent.futures.thread import BrokenThreadPool
from datetime import datetime, timedelta
from typing import Callable, Iterable

from ._proto import Connection

Task = namedtuple("Task", ["name", "func", "bind"])


class Worker:
    send_heartbeat_every = 15  # seconds
    is_quiet = False
    is_disconnecting = False

    def __init__(self, *args, **kwargs):
        """
        Creates a Faktory worker.

        This worker will connect to the `faktory` argument by default. It should be in the standard Faktory format:
        ```
        tcp://:password@localhost:7419
        ```
        If you don't pass a faktory instance to connect to, the worker will check the `FAKTORY_URL` environment variable.
        If the environment variable is not set, then the worker will attempt to connect to Faktory on the localhost,
        without a password.

        If the URL scheme is `tcp+tls://` then the Faktory worker will establish a TLS encrypted connection to Faktory.

        You may pass a list of queues to process with the `queues` argument. If you supply no `queues`, then the worker
        will process the default queue.

        You may pass a list of labels to process with the `labels` argument. These are visible in the Faktory Web UI. If
        not supplied, it defaults to `labels=['python']`.

        :param faktory: address of the Faktory instance to connect to.
        :type faktory: string
        :param concurrency: number of worker processes to start
        :type concurrency: int
        :param disconnect_wait: number of seconds to wait when worker is interrupted before failing all jobs
        :type disconnect_wait: int
        :param log: logger to use for status, errors and connection details
        :type log: logging.Logger
        :param labels: labels to show in the Faktory webui for this worker
        :type labels: tuple
        :param use_threads: Set to True to use threads rather than multiple processes for work to be executed on
        :type use_threads: bool
        :param executor: Set the class of the process executor that will be used. By default concurrenct.futures.ProcessPoolExecutor is used.
        :type executor: class
        """
        self.concurrency = kwargs.pop("concurrency", 1)
        self.disconnect_wait = kwargs.pop("disconnect_wait", 15)
        self.log = kwargs.pop("log", logging.getLogger("faktory.worker"))

        self._queues = kwargs.pop("queues", ["default",])
        self._executor_class = kwargs.pop(
            "executor",
            ThreadPoolExecutor
            if kwargs.pop("use_threads", False)
            else ProcessPoolExecutor,
        )
        self._last_heartbeat = None
        self._tasks = dict()
        self._pending = list()
        self._disconnect_after = None
        self._executor = None

        signal.signal(signal.SIGTERM, self.handle_sigterm)

        if "labels" not in kwargs:
            kwargs["labels"] = ["python"]
        self.labels = kwargs["labels"]

        if "worker_id" not in kwargs:
            kwargs["worker_id"] = self.get_worker_id()
        self.worker_id = kwargs["worker_id"]

        self.faktory = Connection(*args, **kwargs)
        # self.faktory.debug = True

    def register(self, name: str, func: Callable, bind: bool = False) -> None:
        """
        Register a task that can be run with this worker.

        If you set bind=True, then the first argument passed to the function will always be Faktory's `jid` for this task.

        You can register a task after the worker has started.

        :param name: name of the task
        :type name: str
        :param func: function to call when the
        :type func: callable
        :param bind: pass the jid to `func`
        :type bind: bool
        :return:
        :rtype:
        """
        if not callable(func):
            raise ValueError("task func is not callable")

        self._tasks[name] = Task(name=name, func=func, bind=bind)
        self.log.info("Registered task: {}".format(name))

    def deregister(self, name: str) -> None:
        """
        Remove a task from the list of registered tasks.

        Can be called after the worker has started, any currently processing copies of `task` will continue.

        :param name: task name
        :type name: str
        :return:
        :rtype:
        """
        if name in self._tasks:
            del self._tasks[name]
            self.log.debug("Removed registered task: {}".format(name))

    def run(self):
        """
        Start the worker

        `run()` will trap signals, on the first ctrl-c it will try to gracefully shut the worker down, waiting up to 15
        seconds for in progress tasks to complete.

        If after 30 seconds tasks are still running, they are forced to terminate and the worker will close.

        This method is blocking -- it will only return when the worker has shutdown, either by control-c or by
        terminating it from the Faktory Web UI.

        :return:
        :rtype:
        """
        # create a pool of workers
        if not self.faktory.is_connected:
            self.faktory.connect(worker_id=self.worker_id)

        self.log.debug(
            "Creating a worker pool with concurrency of {}".format(self.concurrency)
        )

        self._last_heartbeat = datetime.now() + timedelta(
            seconds=self.send_heartbeat_every
        )  # schedule a heartbeat for the future

        self.log.info("Queues: {}".format(", ".join(self.get_queues())))
        self.log.info("Labels: {}".format(", ".join(self.faktory.labels)))

        while True:
            try:
                # tick runs continuously to process events from the faktory connection
                self.tick()
                if not self.faktory.is_connected:
                    break
            except KeyboardInterrupt as e:
                # 1st time through: soft close, wait 15 seconds for jobs to finish and send the work results to faktory
                # 2nd time through: force close, don't wait, fail all current jobs and quit as quickly as possible
                if self.is_disconnecting:
                    break

                self.log.info(
                    "Shutdown: waiting up to 15 seconds for workers to finish current tasks"
                )
                self.disconnect(wait=self.disconnect_wait)
            except (BrokenProcessPool, BrokenThreadPool):
                self.log.info("Shutting down due to pool failure")
                self.disconnect(force=True, wait=15)
                break

        if self.faktory.is_connected:
            self.log.warning("Forcing worker processes to shutdown...")
            self.disconnect(force=True)

        self.executor.shutdown(wait=False)
        sys.exit(1)

    def disconnect(self, force=False, wait=30):
        """
        Disconnect from the Faktory server and shutdown this worker.

        The default is to shutdown gracefully, allowing 15s for in progress tasks to complete and update Faktory.

        :param force: Immediate shutdown, cancelling running tasks
        :type force: bool
        :param wait: Graceful shutdown, allowing `wait` seconds for in progress jobs to complete
        :type wait: int
        :return:
        :rtype:
        """
        self.log.debug(
            "Disconnecting from Faktory, force={} wait={}".format(force, wait)
        )

        self.is_quiet = True
        self.is_disconnecting = True
        self._disconnect_after = datetime.now() + timedelta(seconds=wait)

        if force:
            self.fail_all_jobs()
            self.faktory.disconnect()

    def tick(self):
        if self._pending:
            self.send_status_to_faktory()

        if self.should_send_heartbeat:
            self.heartbeat()

        if self.should_fetch_job:
            # grab a job to do, and start it processing
            job = self.faktory.fetch(self.get_queues())
            if job:
                jid = job.get("jid")
                func = job.get("jobtype")
                args = job.get("args")
                self._process(jid, func, args)
        else:
            if self.is_disconnecting:
                if self.can_disconnect:
                    # can_disconnect returns True when there are no running tasks or pending ACK / FAILs to send
                    # so there is no more work to send back to Faktory
                    self.faktory.disconnect()
                    return

                if datetime.now() > self._disconnect_after:
                    self.disconnect(force=True)

            # faktory.fetch() blocks for 2s, but if we are not fetching jobs then we need to add a delay or this process will spin
            time.sleep(0.25)

    def send_status_to_faktory(self):
        for future in self._pending:
            if future.done():
                self._pending.remove(future)
                try:
                    future.result(timeout=1)
                    self._ack(future.job_id)
                except KeyboardInterrupt:
                    self._fail(future.job_id)
                except Exception as e:
                    self._fail(future.job_id, exception=e)
                    self.log.exception("Task failed: {}".format(future.job_id))

    def _process(self, jid: str, job: str, args):
        try:
            task = self.get_registered_task(job)
            if task.bind:
                # pass the jid as argument 1 if the task has bind=True
                args = [jid,] + args

            self.log.debug(
                "Running task: {}({})".format(
                    task.name, ", ".join([str(x) for x in args])
                )
            )
            future = self.executor.submit(task.func, *args)
            future.job_id = jid
            self._pending.append(future)
        except (KeyError, Exception) as e:
            self._fail(jid, exception=e)

    def _ack(self, jid: str):
        self.faktory.reply("ACK", {"jid": jid})
        ok = next(self.faktory.get_message())

    def _fail(self, jid: str, exception=None):
        response = {"jid": jid}
        if exception is not None:
            response["errtype"] = type(exception).__name__
            response["message"] = str(exception)

        self.faktory.reply("FAIL", response)
        ok = next(self.faktory.get_message())

    def fail_all_jobs(self):
        for future in self._pending:
            if future.done():
                self._ack(future.job_id)
                continue

            # force the job to fail
            future.cancel()
            self._fail(future.job_id)

    def handle_sigterm(self, signal, frame):
        raise KeyboardInterrupt

    @property
    def should_fetch_job(self) -> bool:
        return (
            not (self.is_disconnecting or self.is_quiet)
            and len(self._pending) < self.concurrency
        )

    @property
    def can_disconnect(self):
        return len(self._pending) == 0

    @property
    def should_send_heartbeat(self) -> bool:
        """
        Checks `self._last_heartbeat` and `self.send_heartbeat_every` to figure out of this worker needs to send a
        heartbeat to the Faktory server. The beat should be sent once per 60s max, and defaults to once per 15s.

        Chances are you don't want to override this in a subclass, but change the property `self.send_heartbeat_every`
        instead.

        :return: True if this worker should heartbeat
        :rtype: bool
        """
        return datetime.now() > (
            self._last_heartbeat + timedelta(seconds=self.send_heartbeat_every)
        )

    def heartbeat(self) -> None:
        """
        Send a heartbeat to the Faktory server so it knows this worker is still alive. This is sent every
        `self.send_heartbeat_every` seconds. The default is once per 15 seconds. It should not be more than 60s
        or Faktory will drop this worker from its active list.

        :return:
        :rtype:
        """
        self.log.debug("Sending heartbeat for worker {}".format(self.worker_id))
        self.faktory.reply("BEAT", {"wid": self.worker_id})
        ok = next(self.faktory.get_message())
        if "state" in ok:
            if "quiet" in ok:
                if not self.is_quiet:
                    self.log.warning(
                        "Faktory has quieted this worker, will not run any more tasks"
                    )
                self.is_quiet = True
            if "terminate" in ok:
                if not self.is_disconnecting:
                    self.log.warning(
                        "Faktory has asked this worker to shutdown, will cancel any pending tasks still running 25s time"
                    )
                self.disconnect(wait=25)
        self._last_heartbeat = datetime.now()

    @property
    def executor(self) -> Executor:
        """
        Return the concurrent.futures executor instance to use for this worker.

        Can be passed via the `executor` argument to `__init__` or set `use_threads=True` to use the Threaded executor.

        The worker will use a process based executor by default.

        :return: executor instance
        :rtype: concurrent.futures.Executor
        """
        if self._executor is None:
            self._executor = self._executor_class(max_workers=self.concurrency)
        return self._executor

    def get_queues(self) -> Iterable:
        """
        Returns a list of queues that this worker should be process. You can override this in a subclass to adjust the
        queues at runtime.

        :return: list of queues
        :rtype: list
        """
        return self._queues

    def get_worker_id(self) -> str:
        """
        Returns a unique ID for this worker. This method is called once, during setup of the connection. It should not
        change the worker_id during the lifetime of the worker.

        If you override this method, you should return a random string of at least 8 characters and avoid collisions
        with other running workers.

        :return: unique worker id
        :rtype: str
        """
        return uuid.uuid4().hex

    def get_registered_task(self, name: str) -> Task:
        try:
            return self._tasks[name]
        except KeyError:
            raise ValueError("'{}' is not a registered task".format(name)) from None
