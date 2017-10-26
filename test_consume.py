from faktory_worker import Worker


def do_some_work(*args):
    import time
    time.sleep(5)


if __name__ == '__main__':
    worker = Worker(host="faktory", port=7419, concurrency=1)

    # register the functions you would like to run with the worker
    worker.register("test", do_some_work)
    worker.connect()
    worker.start()  # control-c to end
