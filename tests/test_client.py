# -*- coding: utf-8 -*-
"""
test_client.py 

test the functions of the twisted client for nimbus.io
"""
import logging

from twisted.python import log
from twisted.internet import reactor

from twisted_client_for_nimbusio.archiver import archive

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    logging.root.setLevel(logging.DEBUG)

def _error(failure):
    log.msg("Failure %s" % (failure, ), logLevel=logging.ERROR)

def _shutdown(ignored):
    log.msg('shutting down', logLevel=logging.DEBUG)
    reactor.stop()

def _archive_result(result):
    log.msg("result = %s" % (result,), logLevel=logging.INFO)

def _start_test():
    log.msg("starting", logLevel=logging.DEBUG)

    deferred = archive()

    deferred.addCallback(_archive_result)
    deferred.addErrback(_error)
    deferred.addBoth(_shutdown)

    log.msg("returning deferred", logLevel=logging.DEBUG)
    return deferred

if __name__ == "__main__":
    _initialize_logging()
    observer = log.PythonLoggingObserver()
    observer.start()
    reactor.callLater(0, _start_test)
    reactor.run()
