# -*- coding: utf-8 -*-
"""
test_retrieve_slice.py

test retrieving slices of keys
"""
from hashlib import md5
import httplib
import logging

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_retrieve_path, \
    compute_range_header_tuple

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.buffered_consumer import BufferedConsumer

retrieve_slice_test_complete_deferred = defer.Deferred()
_pending_retrieve_slice_test_count = 0
_error_count = 0
_failure_count = 0

def _retrieve_slice_data(_result, state, key, consumer):
    """
    callback for successful data of an individual retrieve slice request
    """
    global _pending_retrieve_slice_test_count, _error_count
    _pending_retrieve_slice_test_count -= 1

    data = consumer.buffer

    data_md5 = md5(data)

    if len(data) != state["slice-data"][key]["size"]:
        log.err("retrieve %s (%s, %s) size mismatch %s != %s" % (
                key, 
                state["slice-data"][key]["offset"],
                state["slice-data"][key]["size"],
                len(data), 
                state["slice-data"][key]["size"], ),
                logLevel=logging.ERROR)        
        _error_count += 1    
    elif data_md5.digest() != state["slice-data"][key]["md5"].digest():
        log.err("retrieve_slice %s (%s, %s) md5 mismatch" % (
                key, 
                state["slice-data"][key]["offset"],
                state["slice-data"][key]["size"]),
                logLevel=logging.ERROR)        
        _error_count += 1
    else:
        log.msg("retrieve_slice %s (%s, %s) successful" % (
                key, 
                state["slice-data"][key]["offset"],
                state["slice-data"][key]["size"], ) )

    if _pending_retrieve_slice_test_count == 0:
        retrieve_slice_test_complete_deferred.callback((_error_count, 
                                                        _failure_count))

def _retrieve_slice_error(failure, state, key):
    """
    errback for failure of an individual retrieve_slice request
    """
    global _pending_retrieve_slice_test_count, _failure_count
    _pending_retrieve_slice_test_count -= 1

    log.msg("retrieve_slice %s (%s, %s) Failure %s" % (
            key, 
            state["slice-data"][key]["offset"],
            state["slice-data"][key]["size"],
            failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_retrieve_slice_test_count == 0:
        retrieve_slice_test_complete_deferred.callback((_error_count, 
                                                        _failure_count))

def start_retrieve_slice_tests(state):
    """
    start a deferred request for retrieve of slices
    """
    global _pending_retrieve_slice_test_count

    for key in state["slice-data"].keys():
        log.msg("retrieving slice '%s' (%s, %s)" % (
                key, 
                state["slice-data"][key]["offset"],
                state["slice-data"][key]["size"], ), 
                logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_retrieve_path(key)
        range_header_tuple = \
            compute_range_header_tuple(state["slice-data"][key]["offset"],
                                       state["slice-data"][key]["size"])
        headers = dict([range_header_tuple])
        expected_status = frozenset([httplib.PARTIAL_CONTENT, ])

        deferred = start_collection_request(state["identity"],
                                            "GET", 
                                            state["collection-name"],
                                            path,
                                            response_consumer=consumer,
                                            additional_headers=headers,
                                            valid_http_status=expected_status)
        deferred.addCallback(_retrieve_slice_data, state, key, consumer)
        deferred.addErrback(_retrieve_slice_error, state, key)

        _pending_retrieve_slice_test_count += 1
