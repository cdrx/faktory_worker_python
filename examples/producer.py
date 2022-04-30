import time

import faktory

with faktory.connection() as client:
    while True:
        client.queue("add", args=(1, 2), queue="default")
        time.sleep(1)

        client.queue("subtract", args=(10, 5), queue="default")
        time.sleep(1)

        client.queue("multiply", args=(8, 8), queue="default")
        time.sleep(1)
