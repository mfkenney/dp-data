#!/usr/bin/env python
"""
.. module:: sql
   :synopsis: Interface to SQLite DP database.
"""
import pandas as pd
from sqlalchemy import Table, MetaData, select, Column, Integer


def get_profiles(eng):
    """
    Return a list of profiles available in database. Each
    list entry is a dictionary containing the following
    values:

    start:: profile start time (in seconds since 1/1/1970 UTC)
    end:: profile end time
    pnum:: profile number
    mode:: profile type ('up', 'down', 'docking')
    """
    meta = MetaData()
    profiles = Table('profiles', meta,
                     Column('start', Integer),
                     Column('end', Integer),
                     autoload=True, autoload_with=eng)
    s = select([profiles.c.start, profiles.c.end, profiles.c.pnum, profiles.c.mode])
    conn = eng.connect()
    result = conn.execute(s)
    names = result.keys()
    return [dict(zip(names, row)) for row in result.fetchall()]


def get_calibration(eng, sensor):
    """
    Return the all calibration constants for a sensor.
    """
    meta = MetaData()
    cal = Table('calibration', meta, autoload=True, autoload_with=eng)
    s = select([cal.c.varname, cal.c.val]).where(cal.c.sensor == sensor)
    conn = eng.connect()
    result = conn.execute(s)
    return dict(result.fetchall())


def get_dataset(eng, table, t_start, t_end):
    """
    Return a data-set from the database.

    :param eng: SQLAlchemy database engine
    :param table: SQL table name
    :param t_start: start time (in seconds since 1/1/1970 UTC)
    :param t_end: end time
    :rtype: :class:`pandas.DataFrame`
    """
    query = 'select * from {} where timestamp between ? and ?'.format(table)
    return pd.read_sql_query(query, eng,
                             params=(t_start*1000000L, t_end*1000000L))


def put_dataset(eng, table, df):
    """
    Load a dataset into a SQL table.
    """
    return df.to_sql(table, eng,
                     if_exists='append',
                     index=False)
