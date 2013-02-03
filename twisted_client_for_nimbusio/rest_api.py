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
