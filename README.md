# Python worker for Faktory

## Overview

This project is a complete worker and client implementation for the [Faktory job server](https://github.com/contribsys/faktory). You can use it to either consume jobs from Faktory or push jobs to the Faktory server to be processed. 

Requires Python 3.5+.

### Supported Faktory Versions

:x: 0.5.0 <br/>
:white_check_mark: 0.6.0+

### Features

- [x] Creating a worker to run jobs from Faktory
- [x] Concurrency (with multiple processes via the `multiprocessing` module)
- [x] Pushing work to Faktory from Python (with retries, custom metadata and scheduled support)
- [x] Pushing exception / errors from Python back up to Faktory
- [x] Sends worker status back to Faktory
- [x] Supports quiet and teminate from the Faktory web UI
- [x] Password authentication
- [x] TLS support
- [x] Graceful worker shutdown (ctrl-c will allow 15s for pending jobs to finish)

### Todo

- [ ] Documentation (in progress, help would be appreciated)
- [ ] Tests (in progress, help would be appreciated)
- [ ] Django integration (`./manage.py runworker` and `app/tasks.py` support)
- [ ] Other concurrency methods (eventlets? threads?)

### Installation

```
pip install faktory
```

### Connection to Faktory

faktory_worker_python uses this format for the Faktory URL:

`tcp://:password@localhost:7419`

or with TLS:

`tcp+tls://:password@localhost:7419`

If the environment variable `FAKTORY_URL` is set, that is used. Otherwise you can pass the server URL in to the `Worker` or `Client` constructor, like this:

```w = Worker(faktory="tcp://localhost:7419")```

### Pushing Work to Faktory

There is a client context manage that you can use like this:

```
import faktory

with faktory.connection() as client:
    client.queue('test', args=(1, 2))
	client.queue('test', args=(4, 5), queue='other')
```

`test` doesn't need to be implemented by the Python worker, it can be any of the available worker implementations.

### Worker Example

Sample worker:

```
from faktory import Worker

def your_function(x, y):
    return x + y

w = Worker(queues=['default'], concurrency=1)
w.register('test', your_function)

w.run()  # runs until control-c or worker shutdown from Faktory web UI

```

#### Logging

The worker users Python's built in logging module, which you can enable like this before calling `.run()`:

```
import logging
logging.basicConfig(level=logging.DEBUG)
```
