import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)


with faktory.connection() as client:
    while True:
        client.queue('test', args=(1,2), queue='default')
        time.sleep(5)
