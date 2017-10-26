from faktory_worker import Client

c = Client()

c.connect()

for x in range(1, 5):
    print(c.publish("test", args=[1, 2, 3]))

c.disconnect()
