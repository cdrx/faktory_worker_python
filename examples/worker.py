import logging
logging.basicConfig(level=logging.INFO)


from faktory import Worker


def your_function(x, y):
    return x + y


w = Worker(faktory="tcp://localhost:7419", queues=['default'], concurrency=1)
w.register('test', your_function)

w.run()  # runs until control-c or worker shutdown from Faktory web UI


# check examples/producer.py for how to submit tasks to be run by faktory
