# -*- coding: utf-8 -*-
"""
pass_thru_producer.py 

an IBodyProducer that passes data through to the consumer
with bufferring to handle the consumer not being ready
"""
from hashlib import md5
import logging

from zope.interface import implements

from twisted.python import log

from twisted.internet import defer
from twisted.web.iweb import IBodyProducer

class PassThruProducer(object):
    implements(IBodyProducer)

    def __init__(self, name, length):
        self._name = name
        self._length = length
        self._finished_deferred = defer.Deferred()
        self._buffer = ""
        self._bytes_written = 0
        self._md5 = md5()

        self._consumer = None
        self._paused = False

    @property 
    def name(self):
        return self._name

    @property 
    def length(self):
        return self._length

    @property 
    def bytes_remaining_to_write(self):
        count = self._length - (len(self._buffer) + self._bytes_written)
        assert count >= 0
        return count

    @property
    def is_finished(self):
        return self._finished_deferred.called

    @property 
    def md5_digest(self):
        assert self.is_finished
        return self._md5.digest()

    def feed(self, data):
        """
        feed data to the consumer
        """
        if self._consumer is None or self._paused:
            self._buffer = "".join([self._buffer, data, ])
        else:
            self._write_to_consumer(data)

    def _write_to_consumer(self, data):
        log.msg("%s writing %s bytes to consumer" % (
                self._name, len(data), ), 
                logLevel=logging.DEBUG)
        self._consumer.write(data)
        self._bytes_written += len(data)
        self._md5.update(data)

        if self._bytes_written >= self._length:
            log.msg("%s finished" % (self._name, ), logLevel=logging.DEBUG)
            self._finished_deferred.callback(None) 

    def startProducing(self, consumer):
        log.msg("%s startProducing" % (self._name, ), logLevel=logging.DEBUG)
        assert self._consumer is None
        self._consumer = consumer

        if len(self._buffer) > 0:
            self._write_to_consumer(self._buffer)
            self._buffer = ""

        return self._finished_deferred

    def pauseProducing(self):
        log.msg("%s pauseProducing" % (self._name, ), logLevel=logging.DEBUG)
        self._paused = True

    def resumeProducing(self):
        log.msg("%s resumeProducing" % (self._name, ), logLevel=logging.DEBUG)
        self._paused = False
        if len(self._buffer) > 0:
            self._write_to_consumer(self._buffer)
            self._buffer = ""

    def stopProducing(self):
        log.msg("%s stopProducing" % (self._name, ), logLevel=logging.WARN)
