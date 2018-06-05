import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)

def test_function(x,y):
    return x+y


def middleware_function(a, b, c):
    logging.info(a)
    logging.info(b)
    logging.info(c)

w = faktory.Worker(faktory="tcp://localhost:7419",  queues=['default'], concurrency=1)

w.register('test', test_function)
w.middleware_reg(middleware_function)
#runs worker until ctrl-c or shutdown from faktory
w.run()
