# -*- coding: utf-8 -*-
"""
rest_api.py 

support the nimbus.io REST API
"""
from lumberyard.http_util import compute_uri as compute_uri_path

def compute_archive_path(key, *args, **kwargs):
    """
    compute a path to archive the key
    """
    return compute_uri_path("data", key, *args, **kwargs)

def compute_head_path(key, *args, **kwargs):
    """
    compute a path to HEAD the key
    """
    return compute_uri_path("data", key, *args, **kwargs)
 
def compute_list_keys_path(prefix=""):
    """
    list all keys, or all keys beginning with prefix
    """ 
    kwargs = dict()
    if prefix != "" and prefix is not None:
        kwargs["prefix"] = prefix

    return compute_uri_path("data/", **kwargs)

def compute_list_versions_path(prefix=""):
    """
    list all versions of all keys, 
    or all versions of all keys beginning with prefix
    """ 
    kwargs = dict()
    if prefix != "" and prefix is not None:
        kwargs["prefix"] = prefix

    return compute_uri_path("/?versions", **kwargs)

def compute_retrieve_path(key, version_id=None):
    """
    retrieve the contents of a key
    """
    kwargs = {"version_identifier"    : version_id}

    return compute_uri_path("data", key, **kwargs)

def compute_range_header_tuple(slice_offset, slice_size):
    """
    return a (key, value) tuple used to add a 'Range:' header 
    to retrieve the specified slice
    """
    if slice_size is not None:
        if slice_offset is None:
            slice_offset = 0
        return "Range", "bytes=%d-%d" % (slice_offset, 
                                          slice_offset + slice_size - 1, )
    assert slice_offset is not None
    return "Range", "bytes=%d-" % (slice_offset, )

def compute_start_conjoined_path(key):
    """
    start a conjoined archive
    """
    kwargs = {"action" : "start"}
    return compute_uri_path("conjoined", key, **kwargs)

def compute_abort_conjoined_path(conjoined_identifier, key):
    """
    start a conjoined archive
    """
    kwargs = {"action"                : "abort",
              "conjoined_identifier"  : conjoined_identifier}

    uri = compute_uri_path("conjoined", key, **kwargs)

def compute_finish_conjoined_path(key, conjoined_identifier):
    """
    start a conjoined archive
    """
    kwargs = {"action"                : "finish",
              "conjoined_identifier"  : conjoined_identifier}

    return compute_uri_path("conjoined", key, **kwargs)
