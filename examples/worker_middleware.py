import logging
import faktory
import time
logging.basicConfig(level=logging.INFO)

def test_function(x,y):
    return x+y
#all client middleware functions muss pass the jid, func, and args, as the first 3 parameters
def simple_middleware_function(jid, func, args):
    logging.info(jid)
    logging.info(func)
    logging.info(args)

#change the values of function arguments
#changed values must be returned as a dict with either jid, func, or args as a key
#args must be converted to a tuple before hashing
#jid and func must remain strings
def change_args(jid, func, args):
    if func == 'test':
        args[0] = 4
        args = tuple(args)
        middleware_return = {'args': args}
        return middleware_return

#jobs can be killed by returning "kill" to the worker
def kill_jobs(jid, func, args):
    if func == 'test_1':
        return 'kill'

#a variable number of arguments may be passed to middleware and used within the middleware functions
def pass_extra_param(jid, func, args, extra_param):
    if func == 'test':
        args[1] = extra_param
        args = tuple(args)
        middleware_return = {'args': args}
        return middleware_return

#server middlware functions must take the jid, job_status, and exception as the first 3 parameters
#jobs killed by middleware and jobs who've successfully executed don't pass exceptions
#similar to client middleware, server middleware can take a variable number of parameters
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

#middlware executes in the order it's registered
w.server_middleware_reg(server_middleware)
#server middleware is called after job processing
#client middleware is called before a job is submitted
w.client_middleware_reg(simple_middleware_function)
w.client_middleware_reg(change_args)
w.client_middleware_reg(kill_jobs)
w.client_middleware_reg(pass_extra_param, 20)
#runs worker until ctrl-c or shutdown from faktory
w.run()
