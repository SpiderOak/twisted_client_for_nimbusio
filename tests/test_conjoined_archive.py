# -*- coding: utf-8 -*-
"""
test_conjoined_archive.py 

test archiving conjoined (multiple files)
"""
from hashlib import md5
import json
import logging
import random

from twisted.python import log
from twisted.internet import defer

from twisted_client_for_nimbusio.rest_api import compute_start_conjoined_path, \
    compute_archive_path, \
    compute_finish_conjoined_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.pass_thru_producer import PassThruProducer
from twisted_client_for_nimbusio.buffered_consumer import BufferedConsumer

conjoined_archive_complete_deferred = defer.Deferred()
_pending_start_conjoined_count = 0
_pending_archive_count = 0
_pending_finish_conjoined_count = 0
_error_count = 0
_failure_count = 0

def _finish_conjoined_result(_result, _state, key, consumer):
    """
    callback for successful completion of an individual 'finish conjoined'
    """
    global _pending_finish_conjoined_count
    _pending_finish_conjoined_count -= 1  

    result = json.loads(consumer.buffer)  

    log.msg("finish conjoined %s successful: %s %d pending" % (
            key,
            result,
            _pending_finish_conjoined_count, ), 
            logLevel=logging.INFO)

    if _pending_finish_conjoined_count == 0:
        conjoined_archive_complete_deferred.callback((_error_count, 
                                                      _failure_count, ))

def _finish_conjoined_error(failure, _state, key):
    """
    errback for failure of an individual 'finish conjoined'
    """
    global _failure_count,  _pending_finish_conjoined_count
    _failure_count += 1
    _pending_finish_conjoined_count -= 1

    log.msg("finish conjoined: key %s Failure %s" % (
            key, failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    if _pending_finish_conjoined_count == 0:
        conjoined_archive_complete_deferred.callback((_error_count, 
                                                      _failure_count, ))

def _finish_conjoined(state):
    global _pending_finish_conjoined_count

    # finish all the conjoined archives
    for key, entry in state["conjoined-data"].items():
        log.msg("finishing conjoined archive for %r %s" % (
                key, entry["conjoined-identifier"], ), 
                logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_finish_conjoined_path(key, 
                                             entry["conjoined-identifier"])

        deferred = start_collection_request(state["identity"],
                                            "POST", 
                                            state["collection-name"],
                                            path, 
                                            response_consumer=consumer)
        deferred.addCallback(_finish_conjoined_result, state, key, consumer)
        deferred.addErrback(_finish_conjoined_error, state, key)

        _pending_finish_conjoined_count += 1

def _start_conjoined_result(_result, state, key, consumer):
    """
    callback for successful completion of an individual 'start conjoined'
    """
    global _pending_start_conjoined_count
    _pending_start_conjoined_count -= 1  

    result = json.loads(consumer.buffer)  

    log.msg("start conjoined %s successful: identifier = %s %d pending" % (
            key,
            result["conjoined_identifier"],
            _pending_start_conjoined_count, ), 
            logLevel=logging.INFO)

    state["conjoined-data"][key]["conjoined-identifier"] = \
        result["conjoined_identifier"]

    if _pending_start_conjoined_count == 0:
        _finish_conjoined(state)

def _start_conjoined_error(failure, _state, key):
    """
    errback for failure of an individual start conjoined
    """
    global _failure_count,  _pending_start_conjoined_count
    _failure_count += 1
    _pending_start_conjoined_count -= 1

    log.msg("start conjoined key %s Failure %s" % (
            key, failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    if _pending_start_conjoined_count == 0:
        conjoined_archive_complete_deferred.callback((_error_count, 
                                                      _failure_count, ))

def start_conjoined_archives(state):
    """
    start a group of deferred archive requests
    """
    global _pending_start_conjoined_count

    log.msg("start conjoined user_name = %s collection = %s" % (
            state["identity"].user_name, 
            state["collection-name"], ), 
            logLevel=logging.DEBUG)

    # start all the conjoined archives
    for i in range(state["args"].number_of_conjoined_keys):
        prefix = random.choice(state["prefixes"])
        key = "".join([prefix, state["separator"], 
                       "conjoined_key_%05d" % (i+1, )])
        log.msg("starting conjoined archive for %r" % (key, ), 
                logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_start_conjoined_path(key)

        length = random.randint(state["args"].min_conjoined_file_size, 
                                state["args"].max_conjoined_file_size)

        state["conjoined-data"][key] = {"length"              : length,
                                        "md5"                 : md5(),
                                        "conjoined-identifier": None}

        deferred = start_collection_request(state["identity"],
                                            "POST", 
                                            state["collection-name"],
                                            path, 
                                            response_consumer=consumer)
        deferred.addCallback(_start_conjoined_result, state, key, consumer)
        deferred.addErrback(_start_conjoined_error, state, key)

        _pending_start_conjoined_count += 1
