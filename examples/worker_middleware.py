import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)

def test_function(x,y):
    return x+y


def middleware_function(jobId, jobType, jobArgs):
    logging.info(jobId)
    logging.info(jobType)
    logging.info(jobArgs)

w = faktory.Worker(faktory="tcp://localhost:7419",  queues=['default'], concurrency=1)

w.register('test', test_function)
w.middleware_reg(middleware_function)
#runs worker until ctrl-c or shutdown from faktory
w.run()
