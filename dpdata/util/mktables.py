#!/usr/bin/env python
"""
Populate an SQL database with tables for Deep Profiler data.
"""
import argparse
from dpdata import data_dictionary
from dpdata.sql import make_table
from sqlalchemy import Table, MetaData, Column, \
    Float, Text, create_engine


def make_tables(eng, data_dict):
    """
    Create all of the SQL tables for the DP database.

    :param eng: SQLAlchemy database engine
    :param data_dict: data dictionary
    """
    meta = MetaData()
    mdtable = Table('metadata', meta,
                    Column('sensor', Text),
                    Column('varname', Text),
                    Column('units', Text),
                    Column('precision', Text),
                    Column('scale', Float))
    meta.bind = eng
    meta.create_all()
    for name in data_dict:
        make_table(eng, name, meta, data_dict)
    return meta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db', help='SQLAlchemy database connection string')
    args = parser.parse_args()

    eng = create_engine(args.db)
    meta = make_tables(eng, data_dictionary())
    print 'Created tables:'
    for t in meta.sorted_tables:
        print '\t{0}'.format(t.name)


if __name__ == '__main__':
    main()
