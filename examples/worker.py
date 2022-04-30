import logging

logging.basicConfig(level=logging.INFO)

from faktory import Worker


def add_numbers(x, y):
    calc = x + y

    print(f"add: {x} + {y} = {calc}")
    return calc


def subtract_numbers(x, y):
    calc = x - y

    print(f"subtract: {x} - {y} = {calc}")
    return calc


def multiply_numbers(x, y):
    calc = x * y

    print(f"multiply: {x} * {y} = {calc}")
    return calc


if __name__ == "__main__":
    w = Worker(faktory="tcp://localhost:7419", queues=["default"], concurrency=1)
    w.register("add", add_numbers)
    w.register("subtract", subtract_numbers)
    w.register("multiply", multiply_numbers)
    w.run()  # runs until control-c or worker shutdown from Faktory web UI

# check examples/producer.py for how to submit tasks to be run by faktory
