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
(tests/run_test.sh \
     --identity-file=/home/dougfort/motoboto_client/motoboto_test_user_id )
"""
import logging
import sys

from twisted.python import log
from twisted.internet import reactor, defer

import motoboto
from motoboto.identity import load_identity_from_file

from commandline import parse_commandline
from test_single_part_archive import single_part_archive_complete_deferred, \
    start_single_part_archives
from test_conjoined_archive import conjoined_archive_complete_deferred, \
    start_conjoined_archives
from test_head import head_test_complete_deferred, start_head_tests
from test_list_keys import list_keys_test_complete_deferred, \
    start_list_keys_tests
from test_list_versions import list_versions_test_complete_deferred, \
    start_list_versions_tests
from test_retrieve import retrieve_test_complete_deferred, start_retrieve_tests
from test_retrieve_slice import retrieve_slice_test_complete_deferred, \
    start_retrieve_slice_tests
from test_retrieve_stream import retrieve_stream_test_complete_deferred, \
    start_retrieve_stream_tests

class SetupError(Exception):
    pass

_total_errors = 0
_total_failures = 0

def _create_state(args, identity, collection_name):
    return {"prefixes"          :  ["prefix_1", "prefix_2", "prefix_4", ],
            "separator"         : "/",
            "args"              : args,
            "identity"          : identity,
            "collection-name"   : collection_name,
            "key-data"          : dict(),
            "conjoined-data"    : dict(),
            "slice-data"        : dict()}

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

def _retrieve_stream_test_complete(result, state):
    """
    callback for successful completion of all retrieve stream tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all stream retrieves complete. %d errors %d failures" % (
            error_count, failure_count,),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    log.msg("all tests complete %d errors %d failures" % (_total_errors, 
                                                           _total_failures))
    reactor.stop() 

def _retrieve_stream_test_failure(failure, _state):
    """
    errback for failure of the retrieve stream test
    """
    global _total_failures

    log.msg("retrieve_stream test failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    log.msg("all tests complete %d errors %d failures" % (_total_errors, 
                                                           _total_failures))
    reactor.stop() 

def _retrieve_slice_test_complete(result, state):
    """
    callback for successful completion of all retrieve slice tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all slice retrieves complete. %d errors %d failures" % (
            error_count, failure_count,),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_stream_tests, state)

def _retrieve_slice_test_failure(failure, _state):
    """
    errback for failure of the retrieve slice test
    """
    global _total_failures

    log.msg("retrieve_slice test failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_stream_tests, state)

def _retrieve_test_complete(result, state):
    """
    callback for successful completion of all retrieve tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all retrieves complete. %d errors %d failures" % (
            error_count, failure_count,),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_slice_tests, state)

def _retrieve_test_failure(failure, _state):
    """
    errback for failure of the overall retrieve test
    """
    global _total_failures

    log.msg("retrieve test failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_slice_tests, state)

def _list_versions_test_complete(result, state):
    """
    callback for successful completion of all list_versions tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all list_versions complete. %d errors %d failures" % (
            error_count, failure_count,),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_tests, state)

def _list_versions_test_failure(failure, _state):
    """
    errback for failure of the overall list_versions test
    """
    global _total_failures

    log.msg("list_versions test failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_retrieve_tests, state)

def _list_keys_test_complete(result, state):
    """
    callback for successful completion of all list_keys tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all list_keys complete. %d errors %d failures" % (
            error_count, failure_count,),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_list_versions_tests, state)

def _list_keys_test_failure(failure, _state):
    """
    errback for failure of the overall list_keys test
    """
    global _total_failures

    log.msg("list_keys test failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_list_versions_tests, state)

def _head_test_complete(result, state):
    """
    callback for successful completion of all HEAD tests
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all HEADs complete. %d errors, %d failures" % (
            error_count, failure_count, ),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_list_keys_tests, state)

def _head_test_failure(failure, _state):
    """
    errback for failure of the overall HEAD test
    """
    global _total_failures

    log.msg("HEAD test failed: Failure %s" % (failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    _total_failures += 1
    
    # now we can start the next phase of the test
    reactor.callLater(0, start_list_keys_tests, state)

def _conjoined_archive_complete(result, state):
    """
    callback for completion of all conjoined archives 
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all conjoined archives complete. %d errors, %d failures" % (
            error_count, failure_count, ),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_head_tests, state)

def _conjoined_archive_failure(failure):
    """
    errback for failure of conjoined archives
    """
    global _total_failures

    log.msg("conjoined archives failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_head_tests, state)

def _single_part_archive_complete(result, state):
    """
    callback for completion of all single part archives 
    """
    global _total_errors, _total_failures

    error_count, failure_count = result
    log.msg("all single part archives complete. %d errors, %d failures" % (
            error_count, failure_count, ),
            logLevel=logging.INFO)

    _total_errors += error_count
    _total_failures += failure_count

    # now we can start the next phase of the test
    reactor.callLater(0, start_conjoined_archives, state)

def _single_part_archive_failure(failure):
    """
    errback for failure of single part archives
    """
    global _total_failures

    log.msg("single part archives failed: Failure %s" % (
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _total_failures += 1

    # now we can start the next phase of the test
    reactor.callLater(0, start_conjoined_archives, state)

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

    single_part_archive_complete_deferred.addCallback(
        _single_part_archive_complete, state) 
    single_part_archive_complete_deferred.addErrback(
        _single_part_archive_failure, state)
    conjoined_archive_complete_deferred.addCallback(
        _conjoined_archive_complete, state) 
    conjoined_archive_complete_deferred.addErrback(
        _conjoined_archive_failure, state)
    head_test_complete_deferred.addCallback(_head_test_complete, state) 
    head_test_complete_deferred.addErrback(_head_test_failure, state)
    list_keys_test_complete_deferred.addCallback(_list_keys_test_complete, 
                                                 state) 
    list_keys_test_complete_deferred.addErrback(_list_keys_test_failure, state)
    list_versions_test_complete_deferred.addCallback(
        _list_versions_test_complete, state) 
    list_versions_test_complete_deferred.addErrback(
        _list_versions_test_failure, state)
    retrieve_test_complete_deferred.addCallback(_retrieve_test_complete, state) 
    retrieve_test_complete_deferred.addErrback(_retrieve_test_failure, state)
    retrieve_slice_test_complete_deferred.addCallback(
        _retrieve_slice_test_complete, state) 
    retrieve_slice_test_complete_deferred.addErrback(
        _retrieve_slice_test_failure, state)
    retrieve_stream_test_complete_deferred.addCallback(
        _retrieve_stream_test_complete, state) 
    retrieve_stream_test_complete_deferred.addErrback(
        _retrieve_stream_test_failure, state)

    reactor.callLater(0, start_single_part_archives, state)

    reactor.run()
