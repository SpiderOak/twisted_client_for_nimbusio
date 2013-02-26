# -*- coding: utf-8 -*-
"""
commandline.py 

parse the commandline for testing twisted_client_for_nimbusio
"""
import argparse

_program_description = "Test twisted_client_for_nimbusio"

def parse_commandline():
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-i", 
                        "--identity-file", 
                        dest="identity_file",
                        type=str,
                        default=None,
                        help="Path to (motoboto) nimbus.io identity file")
    parser.add_argument("--number-of-single-part-keys", 
                        dest="number_of_single_part_keys",
                        type=int,
                        default=3,
                        help="the number of single part (not conjoined) keys " \
                             "to upload during the test")
    parser.add_argument("--number-of-conjoined-keys", 
                        dest="number_of_conjoined_keys",
                        type=int,
                        default=3,
                        help="the number conjoined (multipart) keys " \
                             "to upload during the test")
    parser.add_argument("--min-single-file-size", 
                        dest="min_single_part_file_size",
                        type=int,
                        default=(1 * 1024 * 1024),
                        help="lower bound of file size for single part files")
    parser.add_argument("--max-single-part-file-size", 
                        dest="max_single_part_file_size",
                        type=int,
                        default=(10 * 1024 * 1024),
                        help="upper bound of file size")
    parser.add_argument("--min-conjoined-file-size", 
                        dest="min_conjoined_file_size",
                        type=int,
                        default=(10 * 1024 * 1024),
                        help="lower bound of file size for conjoined archives")
    parser.add_argument("--max-conjoined-file-size", 
                        dest="max_conjoined_file_size",
                        type=int,
                        default=(100 * 1024 * 1024),
                        help="upper bound of file size for conjoined archives")
    parser.add_argument("--max-conjoined-part-size", 
                        dest="max_conjoined_part_size",
                        type=int,
                        default=(5 * 1024 * 1024),
                        help="max size of single file conjoined archives")
    parser.add_argument("--min-feed-delay", 
                        dest="min_feed_delay",
                        type=float,
                        default=0.5,
                        help="minimum time (secs) to wait between feeds")
    parser.add_argument("--max-feed-delay", 
                        dest="max_feed_delay",
                        type=float,
                        default=3.0,
                        help="maximum time (secs) to wait between feeds")
    return parser.parse_args()
