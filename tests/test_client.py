# -*- coding: utf-8 -*-
"""
test_client.py 

test the functions of the twisted client for nimbus.io
"""
import argparse
import logging
import random
from string import printable
from StringIO import StringIO
import sys

from twisted.python import log
from twisted.internet import reactor, defer

import motoboto
from motoboto.identity import load_identity_from_file

from twisted_client_for_nimbusio.archiver import archive
from twisted_client_for_nimbusio.pass_thru_producer import PassThruProducer

class SetupError(Exception):
    pass

_program_description = "Test twisted_client_for_nimbusio"
_prefixes = ["prefix_1", "prefix_2", "prefix_4", ]
_separator = "/"
_archive_complete_deferred = defer.Deferred()
_pending_archive_count = 0

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    logging.root.setLevel(logging.DEBUG)

def _parse_commandline():
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-i", 
                        "--identity-file", 
                        dest="identity_file",
                        type=str,
                        default=None,
                        help="Path to (motoboto) nimbus.io identity file")
    parser.add_argument("-n", 
                        "--number-of-keys", 
                        dest="number_of_keys",
                        type=int,
                        default=3,
                        help="the number of keys to upload during the test")
    parser.add_argument("--min-file-size", 
                        dest="min_file_size",
                        type=int,
                        default=(1 * 1024 * 1024),
                        help="lower bound of file size")
    parser.add_argument("--max-file-size", 
                        dest="max_file_size",
                        type=int,
                        default=(10 * 1024 * 1024),
                        help="upper bound of file size")
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

def _setup_test(args):
    if args.identity_file is None:
        raise SetupError("you must specify a nimbus.io identity file")
    identity = load_identity_from_file(args.identity_file)
    if args.identity_file is None:
        raise SetupError("unable to load nimbus.io identity from {0}".format(
                         args.identity_file))

    s3_connection = motoboto.connect_s3(identity)
    bucket = s3_connection.create_unique_bucket()
    s3_connection.close()

    return identity, bucket.name

def _data_string(length):
    return "".join([random.choice(printable) for _ in range(length)])

def _archive_result(result, key):
    global _pending_archive_count
    _pending_archive_count -= 1

    log.msg("archive %s successful: version = %s %d pending" % (
            key,
            result["version_identifier"],
            _pending_archive_count, ), 
            logLevel=logging.INFO)

    if _pending_archive_count == 0:
        _archive_complete_deferred.callback(None)

def _archive_error(failure, key):
    global _pending_archive_count
    _pending_archive_count -= 1

    log.msg("key %s Failure %s" % (key, failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)

    if _pending_archive_count == 0:
        _archive_complete_deferred.callback(None)

def _archive_complete(result, keys):
    log.msg("all archives complete. %d keys for further testing" % (len(keys),),
            logLevel=logging.INFO)
    # now we can start the next phase of the test
    reactor.stop()

def _archive_failure(failure,):
    log.msg("archives failed: Failure %s" % (failure.getErrorMessage(), ), 
        logLevel=logging.ERROR)
    if reactor.running:
        reactor.stop()

def _feed_random_producer(args,  producers):
    if _archive_complete_deferred.called:
        log.msg("_feed_random_producer: archive_complete_deferred called", 
                logLevel=logging.WARN)
        return

    producers = [producer for producer in producers if not producer.is_finished]
    if len(producers) == 0:
        log.msg("_feed_random_producers: no active producers; exiting", 
                logLevel=logging.DEBUG)
        return     

    producer = random.choice(producers)
    log.msg("choosing %s from %s producers to feed" % (producer.name,
                                                       len(producers), ),
            logLevel=logging.DEBUG)
    data_length = min(1024 * 1024, producer.bytes_remaining_to_write)
    data = _data_string(data_length)
    producer.feed(data)

    # call ourselves again after a random interval
    feed_delay = random.uniform(args.min_feed_delay, args.max_feed_delay)
    reactor.callLater(feed_delay, _feed_random_producer, args, producers)

def _start_archives(args, identity, collection_name):
    global _pending_archive_count

    log.msg("starting user_name = %s collection = %s" % (identity.user_name, 
                                                         collection_name), 
            logLevel=logging.DEBUG)

    # start all the keys archiving
    keys = list()
    producers = list()
    for i in range(args.number_of_keys):
        prefix = random.choice(_prefixes)
        key = "".join([prefix, _separator, "key_%05d" % (i+1, )])
        log.msg("starting archive for %r" % (key, ), logLevel=logging.DEBUG)
        keys.append(key)
        length = random.randint(args.min_file_size, args.max_file_size)
        bodyProducer = PassThruProducer(key, length)
        producers.append(bodyProducer)
        deferred = archive(identity, collection_name, key, bodyProducer)
        deferred.addCallback(_archive_result, key)
        deferred.addErrback(_archive_error, key)
        _pending_archive_count += 1

    _archive_complete_deferred.addCallback(_archive_complete, keys)
    _archive_complete_deferred.addErrback(_archive_failure)

    feed_delay = random.uniform(args.min_feed_delay, args.max_feed_delay)
    reactor.callLater(feed_delay, _feed_random_producer, args, producers)

if __name__ == "__main__":
    _initialize_logging()
    python_log = logging.getLogger("__main__")

    args = _parse_commandline()
    try:
        identity, collection_name = _setup_test(args)
    except Exception:
        python_log.exception("_setup_test")
        sys.exit(1)

    observer = log.PythonLoggingObserver()
    observer.start()
    reactor.callLater(0, _start_archives, args, identity, collection_name)
    reactor.run()
