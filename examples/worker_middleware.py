import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)

def test_function(x,y):
    return x+y

def simple_middleware_function(jid, func, args):
    logging.info(jid)
    logging.info(func)
    logging.info(args)

def change_args(jid, func, args):
    if func == 'test':
        args[0] = 4
        args = tuple(args)
        middleware_return = {'args': args}
        return middleware_return

def kill_jobs(jid, func, args):
    if func == 'test_1':
        return 'kill'

def pass_extra_param(jid, func, args, extra_param):
    if func == 'test':
        args[1] = extra_param
        args = tuple(args)
        middleware_return = {'args': args}
        return middleware_return

def server_middleware(jid, job_status, exception):
    if job_status is True:
        logging.info('job id: {} finished successfully'.format(jid))
    else:
        logging.info('job id: {} failed'.format(jid))
        if exception is not None:
            logging.info('exception: {}\nmessage: {}'.format(type(exception), str(exception)))


w = faktory.Worker(faktory="tcp://localhost:7419",  queues=['default'], concurrency=1)

w.register('test', test_function)
w.register('test_1', test_function)
w.server_middleware_reg(server_middleware)
w.client_middleware_reg(simple_middleware_function)
w.client_middleware_reg(change_args)
w.client_middleware_reg(kill_jobs)
w.client_middleware_reg(pass_extra_param, 20)
#runs worker until ctrl-c or shutdown from faktory
w.run()
