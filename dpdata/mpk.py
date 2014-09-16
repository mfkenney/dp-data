#!/usr/bin/env python
"""
.. module:: mpk
   :synopsis: read MessagePack-format data.
"""
import msgpack


def get_records(infile):
    """
    Iterate over all of the records in a MessagePack file.

    :param infile: input data file.
    """
    for secs, usecs, data in msgpack.Unpacker(infile):
        yield secs, usecs, data


def put_record(outfile, secs, usecs, data):
    """
    Append a record to a MessagePack file.

    :param outfile: output data file
    :type outfile: file-like object
    :param secs: timestamp in seconds since 1/1/1970 UTC
    :type secs: int
    :param usecs: microsecond component of the timestamp
    :type usecs: int
    :param data: data object
    """
    outfile.write(msgpack.packb([secs, usecs, data]))
