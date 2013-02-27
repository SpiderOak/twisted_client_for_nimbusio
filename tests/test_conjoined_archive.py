# -*- coding: utf-8 -*-
"""
test_conjoined_archive.py 

test archiving conjoined (multiple files)
"""
from hashlib import md5
import json
import logging
import random
from string import printable

from twisted.python import log
from twisted.internet import reactor, defer

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
        log.msg("finishing conjoined archive for %r %s" % 
                (key, entry["conjoined-identifier"], ), 
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

def _data_string(length):
    """
    return a 'random' string, but not cryptographically random
    """
    return "".join([random.choice(printable) for _ in range(length)])

def _archive_result(_result, state, key, conjoined_part, consumer):
    """
    callback for successful completion of an individual archive
    """
    global _pending_archive_count
    _pending_archive_count -= 1  

    try:
        result = json.loads(consumer.buffer)
    except Exception, instance:
        log.msg("archive %s:%03d unable to parse json %s '%r'" % (
                key,
                conjoined_part,
                instance,
                consumer.buffer, ), 
                logLevel=logging.ERROR)
    else:
        log.msg("archive %s:%03d successful: version = %s %d pending" % (
                key,
                conjoined_part,
                result["version_identifier"],
                _pending_archive_count, ), 
                logLevel=logging.INFO)

        state["key-data"][key]["version-identifier"] = \
            result["version_identifier"]

    if _pending_archive_count == 0:
        _finish_conjoined(state)

def _archive_error(failure, _state, key, conjoined_part):
    """
    errback for failure of an individual archive
    """
    global _failure_count,  _pending_archive_count
    _failure_count += 1
    _pending_archive_count -= 1

    log.msg("key %s:%03d Failure %s" % (
            key, conjoined_part, failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)

    if _pending_archive_count == 0:
        conjoined_archive_complete_deferred.callback((_error_count, 
                                                      _failure_count, ))

def _feed_producer(key, conjoined_part, producer, state):
    if conjoined_archive_complete_deferred.called:
        log.msg("_feed_producer: %s:%03d completed_deferred called" % (
                key,
                conjoined_part, ), 
                logLevel=logging.WARN)
        return

    if producer.is_finished:
        log.msg("_feed_producer: %s:%03d producer is finished" % (
                key,
                conjoined_part, ), 
                logLevel=logging.WARN)
        return

    if producer.bytes_remaining_to_write == 0:
        log.msg("_feed_producer: %s:%03d producer has 0 bytes to write" % (
                key,
                conjoined_part, ), 
                logLevel=logging.WARN)
        return

    data_length = min(1024 * 1024, producer.bytes_remaining_to_write)

    data = _data_string(data_length)
    producer.feed(data)

    state["key-data"][key]["md5"].update(data)

    if producer.is_finished:
        return

    feed_delay = random.uniform(state["args"].min_feed_delay, 
                                state["args"].max_feed_delay)

    reactor.callLater(feed_delay, 
                      _feed_producer, 
                      key, 
                      conjoined_part, 
                      producer, 
                      state)

def _archive_conjoined(state):
    global _pending_archive_count

    for key, entry in state["conjoined-data"].items():

        conjoined_identifier = entry["conjoined-identifier"]

        length = random.randint(state["args"].min_conjoined_file_size, 
                                state["args"].max_conjoined_file_size)

        state["key-data"][key] = {"length"              : length,
                                  "md5"                 : md5(),
                                  "version-identifier"  : None}

        bytes_to_archive = length
        conjoined_part = 0
        while bytes_to_archive > 0:
            conjoined_part += 1

            producer_name = "%s_%03d" % (key, conjoined_part)

            if bytes_to_archive > state["args"].max_conjoined_part_size:
                producer_length = state["args"].max_conjoined_part_size
            else:
                producer_length = bytes_to_archive

            consumer = BufferedConsumer()

            producer = PassThruProducer(producer_name, producer_length)

            path = \
                compute_archive_path(key, 
                                     conjoined_identifier=conjoined_identifier,\
                                     conjoined_part=conjoined_part)

            deferred = start_collection_request(state["identity"],
                                                "POST", 
                                                state["collection-name"],
                                                path, 
                                                response_consumer=consumer, 
                                                body_producer=producer)
            deferred.addCallback(_archive_result, 
                                 state, 
                                 key, 
                                 conjoined_part, 
                                 consumer)
            deferred.addErrback(_archive_error, state, key, conjoined_part)

            _pending_archive_count += 1

            # loop on callLater until all archive is complete
            feed_delay = random.uniform(state["args"].min_feed_delay, 
                                        state["args"].max_feed_delay)

            reactor.callLater(feed_delay, 
                              _feed_producer, 
                              key, 
                              conjoined_part, 
                              producer, 
                              state)

            bytes_to_archive -= producer_length

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
        _archive_conjoined(state)

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

        state["key-data"][key] = {"length"              : length,
                                  "md5"                 : md5(),
                                  "version-identifier"  : None}

        state["conjoined-data"][key] = {"conjoined-identifier": None}

        deferred = start_collection_request(state["identity"],
                                            "POST", 
                                            state["collection-name"],
                                            path, 
                                            response_consumer=consumer)
        deferred.addCallback(_start_conjoined_result, state, key, consumer)
        deferred.addErrback(_start_conjoined_error, state, key)

        _pending_start_conjoined_count += 1
