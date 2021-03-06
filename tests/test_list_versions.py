# -*- coding: utf-8 -*-
"""
test_list_versions.py

test listing versions, with and without specifying a key prefix
"""
import json
import logging

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_list_versions_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.buffered_consumer import BufferedConsumer

list_versions_test_complete_deferred = defer.Deferred()
_pending_list_versions_test_count = 0
_error_count = 0
_failure_count = 0

def _list_versions_result(_result, state, prefix, consumer):
    """
    callback for successful result of an individual list versions request
    """
    global _pending_list_versions_test_count, _error_count
    _pending_list_versions_test_count -= 1

    result = json.loads(consumer.buffer)

    expected_versions = set([state["key-data"][key]["version-identifier"] \
                            for key in state["key-data"].keys() \
                            if key.startswith(prefix)])

    actual_versions = set()
    for key_entry in result["key_data"]:
        actual_versions.add(key_entry["version_identifier"])

    if actual_versions == expected_versions:
        log.msg("list_versions successful: %s " % (prefix, ))
    else:
        log.err("list_versions %s error %s != %s" % (prefix,
                                                     actual_versions, 
                                                     expected_versions))
        _error_count += 1

    if _pending_list_versions_test_count == 0:
        list_versions_test_complete_deferred.callback((_error_count, 
                                                       _failure_count, ))

def _list_versions_error(failure, _state, prefix):
    """
    errback for failure of an individual list_versions request
    """
    global _pending_list_versions_test_count, _failure_count
    _pending_list_versions_test_count -= 1

    log.msg("list_versions %s Failure %s" % (
            prefix, failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_list_versions_test_count == 0:
        list_versions_test_complete_deferred.callback((_error_count, 
                                                      _failure_count, ))

def start_list_versions_tests(state):
    """
    start a deferred request for list_versions, with and without key prefixes
    """
    global _pending_list_versions_test_count
    prefixes = ["", ]
    prefixes.extend(state["prefixes"])

    for prefix in prefixes:
        log.msg("listing versions for prefix '%s'" % (prefix, ), 
                logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_list_versions_path(prefix=prefix)
        deferred = start_collection_request(state["identity"],
                                            "GET", 
                                            state["collection-name"],
                                            path,
                                            response_consumer=consumer)
        deferred.addCallback(_list_versions_result, state, prefix, consumer)
        deferred.addErrback(_list_versions_error, state, prefix)

        _pending_list_versions_test_count += 1

