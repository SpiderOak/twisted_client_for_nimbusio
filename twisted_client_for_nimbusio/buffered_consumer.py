# -*- coding: utf-8 -*-
"""
buffered_consumer.py 

An IConsumer that accumulates data in a buffer
"""
import logging

from twisted.python import log

from zope.interface import implements
from twisted.internet.interfaces import IConsumer

class BufferedConsumer(object):
    """
    An IConsumer that accumulates data in a buffer
    """
    implements(IConsumer)

    def __init__(self):
        self._buffer = ""

    @property 
    def buffer(self):
        return self._buffer

    def registerProducer(self, producer, _streaming):
        producer.addConsumer(self)

    def unregisterProducer(self):
        log.err("BufferredConsumer unexpected 'unregisterProducer'",
                logLevel=logging.ERROR)

    def write(self, data):
        self._buffer = "".join([self._buffer, data, ])
