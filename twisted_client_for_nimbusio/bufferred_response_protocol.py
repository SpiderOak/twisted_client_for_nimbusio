# -*- coding: utf-8 -*-
"""
buffered_response_protocol.py

A response protocol that stores received data in a buffer
and fires a deferred with the entire content when it is complete.
"""
import logging

from twisted.python import log

from twisted.internet.protocol import Protocol

from twisted.web.client import ResponseDone

class BufferredResponseProtocol(Protocol):
    """
    A response protocol that stores received data in a buffer
    and fires a deferred with the entire content when it is complete.
    """
    def __init__(self, deferred):
        self._deferred = deferred
        self._buffer = ""

    def dataReceived(self, data_bytes):
        self._buffer = "".join([self._buffer, data_bytes])

    def connectionLost(self, reason=ResponseDone):
        if reason.check(ResponseDone):
            self._deferred.callback(self._buffer)
        else:
            self._deferred.errback(reason)
