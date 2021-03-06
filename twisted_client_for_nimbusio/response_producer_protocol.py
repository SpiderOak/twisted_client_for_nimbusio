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
        self._transport = None
        self._consumer = None

    def makeConnection(self, transport):
        """
        overload Protocol.makeConnection in order to get a reference to the
        transport
        """
        log.msg("ResponseProducerProtocol makeConnection", 
                logLevel=logging.DEBUG)
        Protocol.makeConnection(self, transport)

    def connectionMade(self):
        """
        overload Protocol.connectionMade to verify that we have a connection
        """
        log.msg("ResponseProducerProtocol connectionMade", 
                logLevel=logging.DEBUG)
        Protocol.connectionMade(self)

    def dataReceived(self, data_bytes):
        """
        overload Protocol.dataReceived in order to get the data
        """
        assert self._consumer is not None
        self._consumer.write(data_bytes)

    def connectionLost(self, reason=ResponseDone):
        """
        overload Protocol.connectionLost to handle disconnect
        """
        Protocol.connectionLost(self, reason)
        if reason.check(ResponseDone):
            self._deferred.callback(True)
        else:
            log.err("ResponseProducerProtocol connection lost %s" % (reason, ),
                    logLevel=logging.ERROR)
            self._deferred.errback(reason)

    def addConsumer(self, consumer):
        assert self._consumer is None
        self._consumer = consumer

        
