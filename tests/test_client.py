# -*- coding: utf-8 -*-
"""
test_client.py 

test the functions of the twisted client for nimbus.io
"""
import argparse
import logging
from StringIO import StringIO
import sys

from twisted.python import log
from twisted.internet import reactor
from twisted.web.client import FileBodyProducer

from twisted_client_for_nimbusio.archiver import archive

from motoboto.identity import load_identity_from_file

_program_description = "Test twisted_client_for_nimbusio"

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    logging.root.setLevel(logging.DEBUG)

def _parse_commandline():
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-i", 
                        "--identity-file", 
                        dest="identity_file",
                        type=str,
                        default=None,
                        help="Path to (motoboto) nimbus.io identity file")
    return parser.parse_args()

def _error(failure):
    log.msg("Failure %s" % (failure, ), logLevel=logging.ERROR)

def _shutdown(ignored):
    log.msg('shutting down', logLevel=logging.DEBUG)
    reactor.stop()

def _archive_result(result):
    log.msg("result = %s" % (result,), logLevel=logging.INFO)

def _start_test(identity):
    log.msg("starting user_name = %s" % (identity.user_name, ), 
            logLevel=logging.DEBUG)

    collection_name = "foop"
    key = "glort"
    bodyProducer = FileBodyProducer(StringIO("test body"))

    deferred = archive(identity, collection_name, key, bodyProducer)

    deferred.addCallback(_archive_result)
    deferred.addErrback(_error)
    deferred.addBoth(_shutdown)

    log.msg("returning deferred", logLevel=logging.DEBUG)
    return deferred

if __name__ == "__main__":
    _initialize_logging()
    python_log = logging.getLogger("__main__")

    args = _parse_commandline()
    if args.identity_file is None:
        python_log.error("you must specify a nimbus.io identity file")
        sys.exit(1)
    identity = load_identity_from_file(args.identity_file)
    if args.identity_file is None:
        python_log.error("unable to load nimbus.io identity from {0}".format(
                  args.identity_file))
        sys.exit(1)

    observer = log.PythonLoggingObserver()
    observer.start()
    reactor.callLater(0, _start_test, identity)
    reactor.run()
