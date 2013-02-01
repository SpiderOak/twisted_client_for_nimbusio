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
 