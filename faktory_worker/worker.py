from . client import Client

from multiprocessing import Pool


class Worker(Client):
    concurrency = 1

    _tasks = {}

    def __init__(self, *args, **kwargs):
        self.concurrency = kwargs.pop('concurrency', 1)

        super().__init__(*args, **kwargs)

    def register(self, task, func):
        self._tasks[task] = func

    def deregister(self, task):
        if task in self._tasks:
            del self._tasks[task]

    def start(self):
        # create a pool of workers
        self.pool = Pool(processes=self.concurrency)
        self.fetch_jobs()

    def run_job(self, job: str, jid: str, args: list, custom: dict):
        def ack(*args):
            self.ack(jid)

        def err(err):
            self.fail(jid, exception=err)

        try:
            f = self.func_for_job(job)
            self.pool.apply_async(f, args=args,
                                  callback=ack,
                                  error_callback=err)
        except Exception as e:
            self.fail(jid, exception=e)
        # else:
        #     self.ack(jid)

    def func_for_job(self, job):
        try:
            return self._tasks[job]
        except KeyError:
            raise ValueError("'{}' is not a registered function".format(job))
