## Python worker for Faktory

This is a WIP, but so far working is:

* Pushing work to Faktory from Python (with retries, custom metadata and scheduled support)
* Creating a worker to run jobs from Faktory
* Concurrency (with multiple processes, via the `multiprocessing` module)
* Pushing exception / errors from Python back up to Faktory

The API will change. There is no Python 2 support.

### Todo

* Django integration (`./manage.py runworker` and a nice way to register tasks)
* Connection failure handling
* Gracefully wait for running tasks to finish when the worker is shutdown

### Authentication

You can pass `password="secret"` to the `Worker()` and `Client()` constructor, like this:

```
worker = Worker(host="server", port=7418, password="secret")
```

### Examples

Sample worker:
```
from faktory_worker import Worker


def do_some_work(*args):
    import time
    time.sleep(5)


if __name__ == '__main__':
    worker = Worker(host="localhost", port=7419, concurrency=1)

    # register the functions you would like to run with the worker
    worker.register("test", do_some_work)
    
    worker.connect()
    worker.start()  # control-c to end

```

To push a job to Faktory from Python:

```
from faktory_worker import Client

c = Client()
c.connect()
c.publish("test", args=[1, 2, 3])
c.publish("test", args=[4, 5, 6])
c.publish("test", args=[7, 8, 9])
```
