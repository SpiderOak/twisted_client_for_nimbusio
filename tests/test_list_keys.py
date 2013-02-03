# -*- coding: utf-8 -*-
"""
test_list_keys.py

test listing keys, with and without specifying a prefix
"""
import json
import logging

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_list_keys_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.bufferred_response_protocol import \
    BufferredResponseProtocol

list_keys_test_complete_deferred = defer.Deferred()
_pending_list_keys_test_count = 0
_error_count = 0
_failure_count = 0

def _list_keys_result(result_buffer, state, prefix):
    """
    callback for successful result of an individual list keys request
    """
    global _pending_list_keys_test_count, _error_count
    _pending_list_keys_test_count -= 1

    result = json.loads(result_buffer)

    expected_keys = set([key for key in state["key-data"].keys() \
                         if key.startswith(prefix)])

    actual_keys = set()
    for key_entry in result["key_data"]:
        actual_keys.add(key_entry["key"])

    if actual_keys == expected_keys:
        log.msg("list_keys successful: %s " % (prefix, ))
    else:
        log.err("list_keys mismatch %s != %s" % (actual_keys, expected_keys, ),
                logLevel=logging.ERROR)
        _error_count += 1

    if _pending_list_keys_test_count == 0:
        list_keys_test_complete_deferred.callback((_error_count, 
                                                   _failure_count))

def _list_keys_error(failure, state, prefix):
    """
    errback for failure of an individual list_keys request
    """
    global _pending_list_keys_test_count, _failure_count
    _pending_list_keys_test_count -= 1

    log.msg("list_keys %s Failure %s" % (prefix, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_list_keys_test_count == 0:
        list_keys_test_complete_deferred.callback((_error_count, 
                                                   _failure_count))

def start_list_keys_tests(state):
    """
    start a deferred request for list_keys, with and without prefixes
    """
    global _pending_list_keys_test_count
    prefixes = ["", ]
    prefixes.extend(state["prefixes"])

    for prefix in prefixes:
        log.msg("listing keys for prefix '%s'" % (prefix, ), 
                logLevel=logging.DEBUG)

        path = compute_list_keys_path(prefix=prefix)

        deferred = start_collection_request(state["identity"],
                                            "GET", 
                                            state["collection-name"],
                                            path,
                                            BufferredResponseProtocol)
        deferred.addCallback(_list_keys_result, state, prefix)
        deferred.addErrback(_list_keys_error, state, prefix)

        _pending_list_keys_test_count += 1

