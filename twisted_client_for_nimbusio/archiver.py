# -*- coding: utf-8 -*-
"""
archiver.py 

archive data stream to nimbus.io
"""
import httplib
import logging

from twisted.python import log

from twisted.internet import reactor

from twisted.web.client import Agent
from twisted.web.http_headers import Headers

def cbResponse(response):
    successful = True
    if response.code != httplib.OK:            
        log.msg("(%d) %s" % (response.code, response.phrase), 
                logLevel=logging.ERROR)
        successful = False
    return successful        

def archive():
    """
    request an archive
    """
    agent = Agent(reactor)
    d = agent.request(
        'GET',
        'http://example.com/',
        Headers({'User-Agent': ['Twisted Web Client Example']}),
        None)

    d.addCallback(cbResponse)

    return d 
