# -*- coding: utf-8 -*-
"""
test_retrieve_stream.py

test retrieving keys to as streams
"""
from hashlib import md5
import logging
import random

from twisted.python import log
from twisted.internet import reactor, defer

from twisted_client_for_nimbusio.rest_api import compute_retrieve_path

from twisted_client_for_nimbusio.requester import start_collection_request

from zope.interface import implements
from twisted.internet.interfaces import IConsumer

class TestStreamConsumer(object):
    """
    An IConsumer that allows stops and starts
    """
    implements(IConsumer)
    def __init__(self):
        self._producer = None
        self._bytes_read = 0
        self._md5 = md5()

    @property 
    def bytes_read(self):
        return self._bytes_read

    @property 
    def md5_digest(self):
        return self._md5.digest()

    def registerProducer(self, producer, _streaming):
        self._producer = producer
        self._producer.addConsumer(self)

    def unregisterProducer(self):
        log.msg("TestStreamConsumer 'unregisterProducer'",
                logLevel=logging.DEBUG)
        self._producer = None

    def write(self, data):
#        self._producer.pauseProducing()

        # simulate some slow process
        self._bytes_read += len(data)
        self._md5.update(data)
        reactor.callLater(5, self._ready)

    def done(self):
        """
        mark this consumer a done, to break our _ready loop
        """
        self._producer.stopProducing()
        self.unregisterProducer()

    def _ready(self):
        pass
#        if self._producer is not None:
#            self._producer.resumeProducing()        

retrieve_stream_test_complete_deferred = defer.Deferred()
_pending_retrieve_stream_test_count = 0
_error_count = 0
_failure_count = 0

def _retrieve_data(_result, state, key, consumer):
    """
    callback for successful data of an individual retrieve request
    """
    global _pending_retrieve_stream_test_count, _error_count
    _pending_retrieve_stream_test_count -= 1

    consumer.done()

    if consumer.bytes_read != state["key-data"][key]["length"]:
        log.err("retrieve_stream %s size mismatch %s != %s" % (
                key, consumer.bytes_read, state["key-data"][key]["length"], ),
                logLevel=logging.ERROR)        
        _error_count += 1    
    elif consumer.md5_digest != state["key-data"][key]["md5"].digest():
        log.err("retrieve_stream %s md5 mismatch" % (key, ),
                logLevel=logging.ERROR)        
        _error_count += 1
    else:
        log.msg("retrieve %s successful" % (key, ))

    if _pending_retrieve_stream_test_count == 0:
        retrieve_stream_test_complete_deferred.callback((_error_count, 
                                                         _failure_count))

def _retrieve_error(failure, _state, key):
    """
    errback for failure of an individual retrieve request
    """
    global _pending_retrieve_stream_test_count, _failure_count
    _pending_retrieve_stream_test_count -= 1

    log.msg("retrieve_stream %s Failure %s" % (
            key, failure.getErrorMessage(),), 
            logLevel=logging.ERROR)

    _failure_count += 1

    if _pending_retrieve_stream_test_count == 0:
        retrieve_stream_test_complete_deferred.callback((_error_count, 
                                                         _failure_count))

def start_retrieve_stream_tests(state):
    """
    start a deferred request for retrieve
    """
    global _pending_retrieve_stream_test_count

    for key in state["key-data"].keys():
        log.msg("retrieving key '%s'" % (key, ), logLevel=logging.DEBUG)

        consumer = TestStreamConsumer()

        path = compute_retrieve_path(key)
        deferred = start_collection_request(state["identity"],
                                            "GET", 
                                            state["collection-name"],
                                            path,
                                            response_consumer=consumer)
        deferred.addCallback(_retrieve_data, state, key, consumer)
        deferred.addErrback(_retrieve_error, state, key)

        _pending_retrieve_stream_test_count += 1
