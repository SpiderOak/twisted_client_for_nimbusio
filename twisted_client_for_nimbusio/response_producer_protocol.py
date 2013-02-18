# -*- coding: utf-8 -*-
"""
response_producer_protocol.py

A protocol for handling the HTTP response from the twisted web client
It implments the IProducer protocol to pass the respo9nse data on to
whoever is interested.
"""
import logging

from twisted.python import log

from twisted.internet.protocol import Protocol
from zope.interface import implements
from twisted.internet.interfaces import IPushProducer

from twisted.web.client import ResponseDone

class ResponseProducerProtocol(Protocol):
    """
    A protocol for handling the HTTP response from the twisted web client
    It implments the IProducer protocol to pass the respo9nse data on to
    whoever is interested.
    """
    implements(IPushProducer)
    def __init__(self, deferred):
        self._deferred = deferred
        self._consumer = None

    def addConsumer(self, consumer):
        assert self._consumer is None
        self._consumer = consumer

    def dataReceived(self, data_bytes):
        assert self._consumer is not None
        self._consumer.write(data_bytes)

    def connectionLost(self, reason=ResponseDone):
        if reason.check(ResponseDone):
            self._deferred.callback(True)
        else:
            log.err("ResponseProducerProtocol connection lost %s" % (reason, ),
                    logLevel=logging.ERROR)
            self._deferred.errback(reason)

    def stopProducing(self):
        log.err("ResponseProducerProtocol stopProducing", 
                logLevel=logging.ERROR)
        self._deferred.errback("stopProducing")
        
    def pauseProducing(self):
        log.err("ResponseProducerProtocol pauseProducing", 
                logLevel=logging.ERROR)
        self._deferred.errback("pauseProducing")
        
    def resumeProducing(self):
        log.err("ResponseProducerProtocol resumeProducing", 
                logLevel=logging.ERROR)
        self._deferred.errback("resumeProducing")
        
