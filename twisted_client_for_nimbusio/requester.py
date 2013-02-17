# -*- coding: utf-8 -*-
"""
requester.py 

start an HTTP(S) request to nimbus.io
"""
import httplib
import logging
import os
import urllib

from twisted.python import log

from twisted.internet import reactor, defer

from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from lumberyard.http_util import compute_collection_hostname, \
        current_timestamp, \
        compute_authentication_string

_connection_timeout = float(os.environ.get("NIMBUSIO_CONNECTION_TIMEOUT", 
                                           "360.0"))
_service_ssl = os.environ.get("NIMBUS_IO_SERVICE_SSL", "0") != "0"
_agent_name = "Twisted Client for Nimbus.io"

def _compute_uri(hostname, path):
    scheme = ("HTTPS" if _service_ssl else "HTTP")
    return "".join([scheme, "://", hostname, path])

def _compute_headers(identity, method, path):
    timestamp = current_timestamp()

    headers = Headers()
    headers.addRawHeader("x-nimbus-io-timestamp", str(timestamp))
    headers.addRawHeader("agent", _agent_name)

    if identity is not None:
        authentication_string = \
            compute_authentication_string(identity.auth_key_id,
                                          identity.auth_key,
                                          identity.user_name, 
                                          method, 
                                          timestamp,
                                          urllib.unquote_plus(path))
        headers.addRawHeader("Authorization", authentication_string)

    return headers

def _request_callback(response, 
                      valid_http_status, 
                      response_protocol, 
                      final_deferred):

    if not response.code in valid_http_status:            
        error_message = "Invalid HTTP Status: (%s) %s expecting %s" % (
                        response.code, 
                        response.phrase,
                        valid_http_status)
        log.msg("_request_callback %s" % (error_message, ), 
                logLevel=logging.ERROR)
        final_deferred.errback(error_message)
    elif response_protocol is not None:
        response.deliverBody(response_protocol(final_deferred))
    else:
        headers = dict(response.headers.getAllRawHeaders())
        final_deferred.callback(headers)

def _request_errback(failure, final_deferred):
    log.msg("_request_errback %s" % (failure.getErrorMessage(), ), 
            logLevel=logging.ERROR)
    final_deferred.errback(failure)

def start_request(identity, 
                  method, 
                  hostname, 
                  path, 
                  response_protocol=None, 
                  body_producer=None,
                  additional_headers=None,
                  valid_http_status=frozenset([httplib.OK, ])):
    """
    start an HTTP(S) request
    return a deferred that fires with the response

    identity
        nimbus.io identity object

    method
        HTTP method (GET, POST, etc)

    hostname
        nimbus.io hostname. Usually computed by lumberyard function 
        compute_collection_hostname

    path
        the path prt of a URI. Usually computed by a function from
        twisted_client_for_nimbusio.rest_api

    response_protocol
        An IProtocol for receving the response from the server

    body_producer
        An IBodyProducer to produce data to upload to the nimbus.io server

    additional_headers
        A dict of key, value pairs to be added to the request headers

    valid_http_status
        A set of HTTP status code(s) that are valid for this request
        defaults to 200 (OK) 
    """
    final_deferred = defer.Deferred()
    uri = _compute_uri(hostname, path)
    headers = _compute_headers(identity, method, path)
    if additional_headers is not None:
        for key, value in additional_headers.items():
            headers.addRawHeader(key, value)

    agent = Agent(reactor)
    log.msg("requesting %s '%r" % (method, uri, ), logLevel=logging.DEBUG)
    request_deferred = agent.request(method, uri, headers, body_producer)

    request_deferred.addCallback(_request_callback, 
                                 valid_http_status, 
                                 response_protocol,
                                 final_deferred)
    request_deferred.addErrback(_request_errback, final_deferred)

    return final_deferred

def start_collection_request(identity, 
                             method, 
                             collection_name, 
                             path, 
                             response_protocol=None, 
                             body_producer=None,
                             additional_headers=None,
                             valid_http_status=frozenset([httplib.OK, ])):
    """
    start an HTTP(S) request for a specific collection
    return a deferred that fires with the response
    """
    hostname = compute_collection_hostname(collection_name)
    return start_request(identity, 
                         method, 
                         hostname, 
                         path, 
                         response_protocol, 
                         body_producer,
                         additional_headers,
                         valid_http_status)
  