# Python worker for Faktory

## Overview

This project is a complete worker and client implementation for the [Faktory job server](https://github.com/contribsys/faktory). You can use it to either consume jobs from Faktory or push jobs to the Faktory server to be processed. 

Requires Python 3.7+.

#### Supported Faktory Versions

:x: 0.5.0 <br/>
:white_check_mark: 0.6 <br/>
:white_check_mark: 0.7 <br/>
:white_check_mark: 0.8 <br/>
:white_check_mark: 1.0 and up <br/>

## Features

- [x] Creating a worker to run jobs from Faktory
- [x] Concurrency (with multiple processes or threads with the `use_threads=True` option)
- [x] Pushing work to Faktory from Python (with retries, custom metadata and scheduled support)
- [x] Pushing exception / errors from Python back up to Faktory
- [x] Sends worker status back to Faktory
- [x] Supports quiet and teminate from the Faktory web UI
- [x] Password authentication
- [x] TLS support
- [x] Graceful worker shutdown (ctrl-c will allow 15s for pending jobs to finish)

#### Todo

- [ ] Documentation (in progress, help would be appreciated)
- [ ] Tests (in progress, help would be appreciated)
- [ ] Django integration (`./manage.py runworker` and `app/tasks.py` support)

## Installation

```
pip install faktory
```

## Pushing Work to Faktory

There is a client context manager that you can use like this:

```
import faktory

with faktory.connection() as client:
    client.queue('test', args=(1, 2))
    client.queue('test', args=(4, 5), queue='other')
```

`test` doesn't need to be implemented by the Python worker, it can be any of the available worker implementations.

## Worker Example

To create a faktory worker (to process jobs from the server) you'll need something like this:

```
from faktory import Worker

def your_function(x, y):
    return x + y

w = Worker(queues=['default'], concurrency=1)
w.register('test', your_function)

w.run()  # runs until control-c or worker shutdown from Faktory web UI

```
## Concurrency

The default mode of concurrency is to use a [ProcessPoolExecutor](https://devdocs.io/python~3.11/library/concurrent.futures#concurrent.futures.ProcessPoolExecutor). Multiple processes are started, the number being controlled by the `concurrency` keyword argument of the `Worker` class. New processes are started only once, and stay up, processing jobs from the queue. There is the possibility to use threads instead of processes as a concurency mechanism. This is done by using `use_threads=True` at Worker creation. As with processes, threads are started once and reused for each job. When doing so, be mindful of the consequences of using threads in your code, like global variables concurrent access, or the fact that initialization code that is run outside of the registered functions will be run only once at worker startup, not once for each thread.

#### Samples

There is very basic [example worker](examples/worker.py) and an [example producer](examples/producer.py) that you can use as a basis for your project.

#### Connection to Faktory

faktory_worker_python uses this format for the Faktory URL:

`tcp://:password@localhost:7419`

or with TLS:

`tcp+tls://:password@localhost:7419`

If the environment variable `FAKTORY_URL` is set, that is used. Otherwise you can pass the server URL in to the `Worker` or `Client` constructor, like this:

```w = Worker(faktory="tcp://localhost:7419")```

#### Logging

The worker users Python's built in logging module, which you can enable like this before calling `.run()`:

```
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Troubleshooting

### Registering decorated functions

This project uses the standard library's `pickle` module during job execution to serialize registered functions. However, one limitation of the `pickle` library is that the function serialization will fail if the given function uses a decorator. This issue only appears if the worker uses multiprocessing (the default) for concurency. A workaround is to use threads instead:

```
w = Worker(..., use_threads=True)
```
