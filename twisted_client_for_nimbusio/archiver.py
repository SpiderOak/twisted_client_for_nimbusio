# -*- coding: utf-8 -*-
"""
archiver.py 

archive data stream to nimbus.io
"""
import httplib
import json
import logging
import os
import urllib

from twisted.python import log

from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol

from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers

from lumberyard.http_util import compute_collection_hostname, \
        compute_uri as compute_uri_path, \
        current_timestamp, \
        compute_authentication_string

class ReceiveResponseProtocol(Protocol):
    def __init__(self, deferred):
        self._deferred = deferred
        self._buffer = ""

    def dataReceived(self, bytes):
        self._buffer = "".join([self._buffer, bytes])

    def connectionLost(self, failure):
        if failure.check(ResponseDone):
            result = json.loads(self._buffer)
            self._deferred.callback(result)
        else:
            self.deferred.errback(failure)

_connection_timeout = float(os.environ.get("NIMBUSIO_CONNECTION_TIMEOUT", 
                                           "360.0"))
_service_ssl = os.environ.get("NIMBUS_IO_SERVICE_SSL", "0") != "0"
_agent_name = "Twisted Client for Nimbus.io"

def _compute_uri(collection_name, path):
    scheme = ("HTTPS" if _service_ssl else "HTTP")
    hostname = compute_collection_hostname(collection_name)
    return "".join([scheme, "://", hostname, path])

def _compute_headers(identity, method, path):
    timestamp = current_timestamp()
    authentication_string = \
        compute_authentication_string(identity.auth_key_id,
                                      identity.auth_key,
                                      identity.user_name, 
                                      method, 
                                      timestamp,
                                      urllib.unquote_plus(path))

    headers = Headers({"Authorization"         : [authentication_string, ],
                       "x-nimbus-io-timestamp" : [str(timestamp), ],
                       "agent"                 : [_agent_name, ]})

    return headers

def _post_response(response, final_deferred):
    if response.code != httplib.OK:            
        error_message = "Invalid HTTP Status: (%s) %s" % (response.code, 
                                                          response.phrase)
        log.msg("_post_response %s" % (error_message, ), 
                logLevel=logging.ERROR)
        final_deferred.errback(error_message)
    else:
        response.deliverBody(ReceiveResponseProtocol(final_deferred))

def _post_error(failure, final_deferred):
    log.msg("_post_error %s" % (failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)
    final_deferred.errback(failure)

def archive(identity, collection_name, key, bodyProducer):
    """
    return a deferred that fires with the response to an HTTP(S) PUSH request
    """
    final_deferred = defer.Deferred()
    method = "POST"
    path = compute_uri_path("data", key)
    uri = _compute_uri(collection_name, path)
    headers = _compute_headers(identity, method, path)

    agent = Agent(reactor)
    log.msg("requesting '%r" % (uri, ), logLevel=logging.DEBUG)
    request_deferred = agent.request(method, uri, headers, bodyProducer)

    request_deferred.addCallback(_post_response, final_deferred)
    request_deferred.addErrback(_post_error, final_deferred)

    return final_deferred
