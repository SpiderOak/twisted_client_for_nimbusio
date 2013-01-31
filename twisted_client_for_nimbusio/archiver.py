# -*- coding: utf-8 -*-
"""
archiver.py 

archive data stream to nimbus.io
"""
import httplib
import logging
import os
import urllib

from twisted.python import log

from twisted.internet import reactor

from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from lumberyard.http_util import compute_collection_hostname, \
        compute_uri as compute_uri_path, \
        current_timestamp, \
        compute_authentication_string

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

def _push_response(response):
    successful = True
    if response.code != httplib.OK:            
        log.msg("(%d) %s" % (response.code, response.phrase), 
                logLevel=logging.ERROR)
        successful = False
    return successful        

def archive(identity, collection_name, key, bodyProducer):
    """
    return a deferred that fires with the response to an HTTP(S) PUSH request
    """
    method = "POST"
    path = compute_uri_path("data", key)
    uri = _compute_uri(collection_name, path)
    headers = _compute_headers(identity, method, path)

    agent = Agent(reactor)
    log.msg("requesting '%r" % (uri, ), logLevel=logging.DEBUG)
    deferred = agent.request(method, uri, headers, bodyProducer)

    deferred.addCallback(_push_response)

    return deferred
