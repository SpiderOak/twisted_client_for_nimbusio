# -*- coding: utf-8 -*-
"""
json_response_protocol.py

A response protocol that fires a deferred with a python object loaded from JSON
"""
import json 
import logging

from twisted.python import log

from twisted.internet.protocol import Protocol

from twisted.web.client import ResponseDone

class JSONResponseProtocol(Protocol):
    def __init__(self, deferred):
        self._deferred = deferred
        self._buffer = ""

    def dataReceived(self, data_bytes):
        self._buffer = "".join([self._buffer, data_bytes])

    def connectionLost(self, reason=ResponseDone):
        if reason.check(ResponseDone):
            try:
                result = json.loads(self._buffer)
            except Exception, instance:
                log.err(instance, logLevel=logging.ERROR)
                self._deferred.errback(instance)
            else:
                self._deferred.callback(result)
        else:
            self._deferred.errback(reason)
