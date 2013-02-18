# -*- coding: utf-8 -*-
"""
test_archive.py 

test archiving files
"""
from hashlib import md5
import json
import logging
import random
from string import printable

from twisted.python import log
from twisted.internet import reactor, defer

from twisted_client_for_nimbusio.rest_api import compute_archive_path

from twisted_client_for_nimbusio.requester import start_collection_request
from twisted_client_for_nimbusio.pass_thru_producer import PassThruProducer
from twisted_client_for_nimbusio.buffered_consumer import BufferedConsumer

archive_complete_deferred = defer.Deferred()
_pending_archive_count = 0
_error_count = 0
_failure_count = 0

def _data_string(length):
    """
    return a 'random' string, but not cryptographically random
    """
    return "".join([random.choice(printable) for _ in range(length)])

def _archive_result(_result, state, key, consumer):
    """
    callback for successful completion of an individual archive
    """
    global _pending_archive_count
    _pending_archive_count -= 1  

    result = json.loads(consumer.buffer)  

    log.msg("archive %s successful: version = %s %d pending" % (
            key,
            result["version_identifier"],
            _pending_archive_count, ), 
            logLevel=logging.INFO)

    state["key-data"][key]["version-identifier"] = result["version_identifier"]

    if _pending_archive_count == 0:
        archive_complete_deferred.callback((_error_count, _failure_count, ))

def _archive_error(failure, _state, key):
    """
    errback for failure of an individual archive
    """
    global _failure_count,  _pending_archive_count
    _failure_count += 1
    _pending_archive_count -= 1

    log.msg("key %s Failure %s" % (key, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    if _pending_archive_count == 0:
        archive_complete_deferred.callback((_error_count, _failure_count, ))

def _feed_random_producer(state):
    """
    produce random data for the various body providers
    """
    if archive_complete_deferred.called:
        log.msg("_feed_random_producer: archive_complete_deferred called", 
                logLevel=logging.WARN)
        return

    keys = [key for key in state["key-data"].keys() \
            if not state["key-data"][key]["producer"].is_finished]
    if len(keys) == 0:
        log.msg("_feed_random_producers: no active keyss; exiting", 
                logLevel=logging.DEBUG)
        return     

    key = random.choice(keys)
    log.msg("choosing %s from %s keys to feed" % (key, len(keys), ),
            logLevel=logging.DEBUG)
    data_length = \
        min(1024 * 1024, 
            state["key-data"][key]["producer"].bytes_remaining_to_write)
    data = _data_string(data_length)
    state["key-data"][key]["producer"].feed(data)
    state["key-data"][key]["md5"].update(data)

    # call ourselves again after a random interval
    feed_delay = random.uniform(state["args"].min_feed_delay, 
                                state["args"].max_feed_delay)
    reactor.callLater(feed_delay, _feed_random_producer, state)

def start_archives(state):
    """
    start a group of deferred archive requests
    """
    global _pending_archive_count

    log.msg("starting user_name = %s collection = %s" % (
            state["identity"].user_name, 
            state["collection-name"], ), 
            logLevel=logging.DEBUG)

    # start all the keys archiving
    for i in range(state["args"].number_of_keys):
        prefix = random.choice(state["prefixes"])
        key = "".join([prefix, state["separator"], "key_%05d" % (i+1, )])
        log.msg("starting archive for %r" % (key, ), logLevel=logging.DEBUG)

        consumer = BufferedConsumer()

        path = compute_archive_path(key)

        length = random.randint(state["args"].min_file_size, 
                                state["args"].max_file_size)
        producer = PassThruProducer(key, length)

        state["key-data"][key] = {"length"              : length,
                                  "producer"            : producer,
                                  "md5"                 : md5(),
                                  "version-identifier"  : None}

        deferred = start_collection_request(state["identity"],
                                            "POST", 
                                            state["collection-name"],
                                            path, 
                                            response_consumer=consumer, 
                                            body_producer=producer)
        deferred.addCallback(_archive_result, state, key, consumer)
        deferred.addErrback(_archive_error, state, key)

        _pending_archive_count += 1

    feed_delay = random.uniform(state["args"].min_feed_delay, 
                                state["args"].max_feed_delay)
    reactor.callLater(feed_delay, _feed_random_producer, state)
