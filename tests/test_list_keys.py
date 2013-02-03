# -*- coding: utf-8 -*-
"""
test_list_keys.py

test listing keys, with and without specifying a prefix
"""
import logging

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_list_keys_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.json_response_protocol import \
    JSONResponseProtocol

list_keys_test_complete_deferred = defer.Deferred()
_pending_list_keys_test_count = 0

def _list_keys_result(result, state, prefix):
    """
    callback for successful result of an individual list keys request
    """
    global _pending_list_keys_test_count
    _pending_list_keys_test_count -= 1

    expected_keys = set([key for key in state["key-data"].keys() \
                         if key.startswith(prefix)])

    actual_keys = set()
    for key_entry in result["key_data"]:
        actual_keys.add(key_entry["key"])

    assert actual_keys == expected_keys

    log.msg("list_keys successful: %s " % (prefix, ))

    if _pending_list_keys_test_count == 0:
        list_keys_test_complete_deferred.callback(None)

def _list_keys_error(failure, state, prefix):
    """
    errback for failure of an individual list_keys request
    """
    global _pending_list_keys_test_count
    _pending_list_keys_test_count -= 1

    log.msg("list_keys %s Failure %s" % (prefix, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    if _pending_list_keys_test_count == 0:
        list_keys_test_complete_deferred.callback(None)

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
                                            JSONResponseProtocol)
        deferred.addCallback(_list_keys_result, state, prefix)
        deferred.addErrback(_list_keys_error, state, prefix)

        _pending_list_keys_test_count += 1

