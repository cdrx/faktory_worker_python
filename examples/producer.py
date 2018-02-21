import faktory
import time

with faktory.connection() as client:
    while True:
        client.queue('test', args=(1, 2), queue='default')
        time.sleep(1)

