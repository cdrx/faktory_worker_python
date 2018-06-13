import faktory
import time

with faktory.connection() as client:
    while True:
        client.queue('test', args=(1, 2), queue='default')
        time.sleep(1)
        client.queue('test_1', args=(1, 2), queue='default')
        time.sleep(1)
