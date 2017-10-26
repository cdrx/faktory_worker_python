import time

from faktory_worker import Client

c = Client(host="faktory")

c.connect()

while True:
    c.publish("test", args=[1, 2, 3])
    time.sleep(0.5)
