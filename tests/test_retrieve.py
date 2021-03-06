# -*- coding: utf-8 -*-
"""
test_retrieve.py

test retrieving entire keys to memory
"""
from hashlib import md5
import logging
import random

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_retrieve_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.buffered_consumer import BufferedConsumer

retrieve_test_complete_deferred = defer.Deferred()
_pending_retrieve_test_count = 0
_error_count = 0
_failure_count = 0

def _retrieve_data(_result, state, key, consumer):
    """
    callback for successful data of an individual retrieve request
    """
    global _pending_retrieve_test_count, _error_count
    _pending_retrieve_test_count -= 1

    data = consumer.buffer

    data_md5 = md5(data)

    if len(data) != state["key-data"][key]["length"]:
        log.err("retrieve %s size mismatch %s != %s" % (
                key, len(data), state["key-data"][key]["length"], ),
                logLevel=logging.ERROR)        
        _error_count += 1    
    elif data_md5.digest() != state["key-data"][key]["md5"].digest():
        log.err("retrieve %s md5 mismatch" % (key, ),
                logLevel=logging.ERROR)        
        _error_count += 1
    else:
        log.msg("retrieve %s successful" % (key, ))

        # choose a random slice to set up the slice test
        slice_offset = random.randint(0, len(data))
        slice_size = random.randint(1, len(data)-slice_offset)
        slice_md5 = md5(data[slice_offset:slice_offset+slice_size])
        state["slice-data"][key] = {"offset" : slice_offset,
                                    "size"   : slice_size,
                                    "md5"    : slice_md5,}

    if _pending_retrieve_test_count == 0:
        retrieve_test_complete_deferred.callback((_error_count, 
                                                   _failure_count))

def _retrieve_error(failure, _state, key):
    """
    errback for failure of an individual retrieve request
    """
    global _pending_retrieve_test_count, _failure_count
    _pending_retrieve_test_count -= 1

    log.msg("retrieve %s Failure %s" % (key, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_retrieve_test_count == 0:
        retrieve_test_complete_deferred.callback((_error_count, 
                                                   _failure_count))

def start_retrieve_tests(state):
    """
    start a deferred request for retrieve, with and without prefixes
    """
    global _pending_retrieve_test_count

    for key in state["key-data"].keys():
        log.msg("retrieving key '%s'" % (key, ), logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_retrieve_path(key)
        deferred = start_collection_request(state["identity"],
                                            "GET", 
                                            state["collection-name"],
                                            path,
                                            response_consumer=consumer)
        deferred.addCallback(_retrieve_data, state, key, consumer)
        deferred.addErrback(_retrieve_error, state, key)

        _pending_retrieve_test_count += 1
