# -*- coding: utf-8 -*-
"""
test_client.py 

test the functions of the twisted client for nimbus.io

To run this test with the installed library, run:
cd "${HOME}/twisted_client_for_nimbusio"
sudo python2.7 setup.py install
(tests/run_test.sh \
     --identity-file=/home/dougfort/motoboto_client/motoboto_test_user_id )

To run against the source run:
sudo rm -rf rm /opt/so2.7/lib/python2.7/site-packages/twisted_client_for_nimbusio-0.1.0-py2.7.egg
cd "${HOME}/twisted_client_for_nimbusio"
(export PYTHONPATH="${HOME}/twisted_client_for_nimbusio";tests/run_test.sh \
     --identity-file=/home/dougfort/motoboto_client/motoboto_test_user_id )
"""
import logging
import sys

from twisted.python import log
from twisted.internet import reactor, defer

import motoboto
from motoboto.identity import load_identity_from_file

from commandline import parse_commandline
from test_archive import archive_complete_deferred, start_archives
from test_head import head_test_complete_deferred, start_head_tests

class SetupError(Exception):
    pass

def _create_state(args, identity, collection_name):
    return {"args"              : args,
            "identity"          : identity,
            "collection-name"   : collection_name,
            "key-data"          : dict()}

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    logging.root.setLevel(logging.DEBUG)

def _setup_test(args):
    if args.identity_file is None:
        raise SetupError("you must specify a nimbus.io identity file")
    identity = load_identity_from_file(args.identity_file)
    if args.identity_file is None:
        raise SetupError("unable to load nimbus.io identity from {0}".format(
                         args.identity_file))

    s3_connection = motoboto.connect_s3(identity)
    bucket = s3_connection.create_unique_bucket()
    s3_connection.close()

    return identity, bucket.name


def _head_test_complete(_result, state):
    """
    callback for successful completion of all HEAD tests
    """
    log.msg("all HEADs complete. %d keys for further testing" % (
            len(state["key-data"]) ,),
            logLevel=logging.INFO)

    # now we can start the next phase of the test
    reactor.stop() 

def _head_test_failure(failure, _state):
    """
    errback for failure of the overall HEAD test
    """
    log.msg("HEAD test failed: Failure %s" % (failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)
    if reactor.running:
        reactor.stop()

def _archive_complete(_result, state):
    """
    callback for completion of all archives 
    """
    log.msg("all archives complete. %d keys for further testing" % (
            len(state["key-data"]),),
            logLevel=logging.INFO)

    # now we can start the next phase of the test
    reactor.callLater(0, start_head_tests, state)

def _archive_failure(failure):
    """
    errback for failure of archives
    one archive failure is enough to abort the test
    """
    log.msg("archives failed: Failure %s" % (failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)
    if reactor.running:
        reactor.stop()

if __name__ == "__main__":
    _initialize_logging()
    python_log = logging.getLogger("__main__")

    args = parse_commandline()
    try:
        identity, collection_name = _setup_test(args)
    except Exception:
        python_log.exception("_setup_test")
        sys.exit(1)

    observer = log.PythonLoggingObserver()
    observer.start()

    state = _create_state(args, identity, collection_name)

    archive_complete_deferred.addCallback(_archive_complete, state) 
    archive_complete_deferred.addErrback(_archive_failure, state)
    head_test_complete_deferred.addCallback(_head_test_complete, state) 
    head_test_complete_deferred.addErrback(_head_test_failure, state)

    reactor.callLater(0, start_archives, state)

    reactor.run()
