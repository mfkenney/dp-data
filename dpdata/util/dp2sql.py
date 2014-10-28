#!/usr/bin/env python
"""
Subscribe to one or more Deep Profiler data streams and log
the data records to an SQL database.
"""
import sys
import argparse
import zmq
from dpdata import expand_lists
from dpdata.zmq import recv_message
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError


def subscribe(ctx, endpoint):
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt(zmq.SUBSCRIBE, b'')
    return sock


def add_record(conn, ins, data):
    conn.execute(ins, **data)


def insert_event(msg, conn, meta):
    tbl = meta.tables.get('profiles')
    if tbl is not None:
        ins = tbl.insert()
        secs, usecs = msg['t']
        attrs = msg['attrs']
        if msg['name'] == 'profile:start':
            conn.execute(ins, start=secs, pnum=attrs['pnum'],
                         mode=attrs['mode'])
        elif msg['name'] == 'profile:end':
            upd = tbl.update().where(tbl.c.pnum == attrs['pnum'])
            try:
                conn.execute(upd, end=secs)
            except IntegrityError:
                conn.execute(ins, pnum=attrs['pnum'], end=secs)


def insert_data(msg, conn, meta):
    secs, usecs = msg['t']
    tbl = meta.tables.get(msg['name'])
    if tbl is not None:
        data = msg['data']
        ins = tbl.insert()
        data['timestamp'] = int(secs * 1000000) + usecs
        try:
            add_record(conn, ins, expand_lists(data))
        except IntegrityError as e:
            sys.stderr.write(repr(e) + '\n')
            sys.stderr.write('Skipping row @[{0:d}, {1:d}]\n'.format(secs, usecs))


def monitor(socks, conn, meta, logevents=False):
    """
    Monitor a list of sockets and insert data records
    in an SQLAlchemy-managed database.
    """
    poller = zmq.Poller()
    for sock in socks:
        poller.register(sock)
    while True:
        ready = dict(poller.poll())
        for sock in ready:
            mtype, contents = recv_message(sock)
            if mtype == 'DATA':
                insert_data(contents, conn, meta)
            elif mtype == 'EVENT' and logevents:
                insert_event(contents, conn, meta)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db', help='SQLAlchemy database connection string')
    parser.add_argument('publisher', help='ZeroMQ PUB endpoints',
                        nargs='+')
    parser.add_argument('--events', help='log profile start/end events',
                        action='store_true')
    args = parser.parse_args()

    eng = create_engine(args.db)
    meta = MetaData()
    meta.reflect(bind=eng)

    ctx = zmq.Context()
    socks = [subscribe(ctx, p) for p in args.publisher]
    monitor(socks, eng.connect(), meta, logevents=args.events)


if __name__ == '__main__':
    main()
