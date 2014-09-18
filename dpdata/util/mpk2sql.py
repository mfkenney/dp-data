#!/usr/bin/env python
"""
Import the contents of a Deep Profiler MessagePack archive
file into an SQL database.
"""
import sys
import argparse
from dpdata import expand_lists
from dpdata.mpk import get_records
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError


def add_record(conn, ins, data):
    conn.execute(ins, **data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('name', help='sensor name')
    parser.add_argument('db', help='SQLAlchemy database connection string')
    parser.add_argument('infiles', help='input files',
                        type=argparse.FileType('rb'),
                        nargs='+')
    args = parser.parse_args()

    eng = create_engine(args.db)
    meta = MetaData()
    meta.reflect(bind=eng)
    if args.name not in meta.tables:
        raise RuntimeError('Bad sensor name: {0}'.format(args.name))

    tbl = meta.tables[args.name]
    conn = eng.connect()
    ins = tbl.insert()
    for f in args.infiles:
        for secs, usecs, data in get_records(f):
            data['timestamp'] = int(secs * 1000000) + usecs
            try:
                add_record(conn, ins, expand_lists(data))
            except IntegrityError as e:
                sys.stderr.write(repr(e) + '\n')
                sys.stderr.write('Skipping row @[{0:d}, {1:d}]\n'.format(secs, usecs))


if __name__ == '__main__':
    main()
