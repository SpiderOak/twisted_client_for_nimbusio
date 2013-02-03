# -*- coding: utf-8 -*-
"""
test_head.py 

test the HEAD API
"""
import logging

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_head_path

from twisted_client_for_nimbusio.requester import start_collection_request

head_test_complete_deferred = defer.Deferred()
_pending_head_test_count = 0
_error_count = 0
_failure_count = 0

def _head_result(result, state, key):
    """
    callback for successful result of an individual HEAD request
    """
    global _pending_head_test_count, _error_count
    _pending_head_test_count -= 1

    if int(result["Content-Length"][0]) != state["key-data"][key]["length"]:
        log.err("HEAD: Content-length mismatch %s %s" % (
                result["Content-Length"][0],
                state["key-data"][key]["length"], ),
                logLevel=logging.ERROR)
        _error_count += 1
    else:
        log.msg("HEAD %s successful: Last-Mondified=%s, Content-Length=%s " % (
                key,
                result["Last-Modified"][0],
                result["Content-Length"][0], ), 
                logLevel=logging.INFO)

    if _pending_head_test_count == 0:
        head_test_complete_deferred.callback((_error_count, _failure_count, ))

def _head_error(failure, state, key):
    """
    errback for failure of an individual HEAD request
    """
    global _pending_head_test_count, _failure_count
    _pending_head_test_count -= 1

    log.msg("HEAD key %s Failure %s" % (key, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_head_test_count == 0:
        head_test_complete_deferred.callback((_error_count, _failure_count, ))

def start_head_tests(state):
    """
    start a deferred request for HEAD on every key
    """
    global _pending_head_test_count

    for key in state["key-data"].keys():
        log.msg("starting HEAD for %r" % (key, ), logLevel=logging.DEBUG)

        path = compute_head_path(key)

        deferred = start_collection_request(state["identity"],
                                            "HEAD", 
                                            state["collection-name"],
                                            path)
        deferred.addCallback(_head_result, state, key)
        deferred.addErrback(_head_error, state, key)

        _pending_head_test_count += 1

