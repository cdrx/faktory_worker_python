import faktory

with faktory.connection() as client:
    client.queue('test', args=(1, 2), queue='default')

