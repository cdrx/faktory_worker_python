import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)

def test_function(x,y):
    return x+y


def simple_middleware_function(job_info):
    jid = job_info[0]
    func = job_info[1]
    args = job_info[2]

    logging.info(jid)
    logging.info(func)
    logging.info(args)

def change_args(job_info: list):
    args = job_info[2]
    args[0] = 4
    job_info[2] = args
    return job_info

def server_middleware(jid, job_success, exception):
    if job_status is True:
        logging.info('job id: {} finished successfully'.format(jid))
    else:
        logging.info('job id: {} failed'.format(jid))
        if exception is not None:
            logging.info('exception: {}\nmessage: {}'.format(type(exception), str(exception)))


w = faktory.Worker(faktory="tcp://localhost:7419",  queues=['default'], concurrency=1)

w.register('test', test_function)

#middlware executes in the order it's registered
w.server_middleware_reg(server_middleware)
w.client_middleware_reg(simple_middleware_function)
w.client_middleware_reg(change_args)
#runs worker until ctrl-c or shutdown from faktory
w.run()
