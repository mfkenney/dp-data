#!/usr/bin/env python
"""
.. module:: sql
   :synopsis: Interface to DP SQL database.
"""
import pandas as pd
from sqlalchemy import Table, MetaData, select, Column, Integer,\
    Float, Text
from sqlalchemy.sql import func
from sqlalchemy.exc import NoSuchTableError
from collections import namedtuple



Profile = namedtuple('Profile', ['start', 'end', 'pnum', 'mode'])

def get_profiles(eng):
    """
    Return a list of profiles available in database. Each
    list entry is a :class:`Profile` instance.

    :param eng: SQLAlchemy database engine
    """
    meta = MetaData()
    profiles = Table('profiles', meta,
                     Column('start', Integer),
                     Column('end', Integer),
                     autoload=True, autoload_with=eng)
    s = select([profiles.c.start, profiles.c.end, profiles.c.pnum, profiles.c.mode])
    conn = eng.connect()
    result = conn.execute(s)
    return map(Profile._make, result.fetchall())


def get_calibration(eng, sensor):
    """
    Return the all calibration constants for a sensor.

    :param eng: SQLAlchemy database engine
    :param sensor: sensor name
    """
    meta = MetaData()
    cal = Table('calibration', meta, autoload=True, autoload_with=eng)
    s = select([cal.c.varname, cal.c.val]).where(cal.c.sensor == sensor)
    conn = eng.connect()
    result = conn.execute(s)
    return dict(result.fetchall())


def get_time_range(eng, table):
    """
    Return a tuple of the start and end times (in microseconds since
    1/1/1970 UTC) for the named SQL table.

    :param eng: SQLAlchemy database engine
    :param table: SQL table name
    """
    meta = MetaData()
    try:
        tbl = Table(table, meta, autoload=True, autoload_with=eng)
    except NoSuchTableError:
        t0, t1 = 0, 0
    else:
        s = select([func.min(tbl.c.timestamp), func.max(tbl.c.timestamp)])
        conn = eng.connect()
        t0, t1 = conn.execute(s).fetchone()
        if t0 is None:
            t0, t1 = 0, 0
    return t0, t1


def get_dataset(eng, table, t_start=0, t_end=0):
    """
    Return a data-set from the database. If *t_end* is 0, select
    from *t_start* to the end of the table, if both bounds are 0,
    select all rows of the table.

    :param eng: SQLAlchemy database engine
    :param table: SQL table name
    :param t_start: start time (in microseconds since 1/1/1970 UTC)
    :param t_end: end time
    :rtype: :class:`pandas.DataFrame`
    """
    if t_start == 0 and t_end == 0:
        query = 'select * from {}'.format(table)
        params = None
    elif t_end == 0:
        query = 'select * from {} where timestamp > ?'.format(table)
        params = (t_start,)
    else:
        query = 'select * from {} where timestamp between ? and ?'.format(table)
        params=(t_start, t_end)
    return pd.read_sql_query(query, eng, params=params)


def put_dataset(eng, table, df):
    """
    Load a dataset into an SQL table.

    :param eng: SQLAlchemy database engine
    :param table: SQL table name
    :param df: data-set contents
    :type df: :class:`pandas.DataFrame`
    """
    return df.to_sql(table, eng,
                     if_exists='append',
                     index=False)


def make_table(eng, sensor, meta, data_dict):
    """
    Create an SQL table for a sensor. The table is added to the
    database and returned. Metdata for the sensor variables is added
    to the *metadata* table if it is present in the database.

    :param eng: SQLAlchemy database engine
    :param sensor: sensor name
    :param meta: SQLAlchemy Metadata object
    :param data_dict: data dictionary.
    :rtype: :class:`sqlalchemy.Table`
    """
    if not sensor in data_dict:
        raise KeyError
    cols = [Column('timestamp', Integer, unique=True)]
    metadata = []
    cfg = data_dict[sensor]
    for desc in cfg['data']:
        if 'precision' in desc:
            precision = desc['precision']
            if desc['precision'] == '1':
                ctype = Integer
            else:
                ctype = Float
        else:
            if desc.get('tostr') == str:
                ctype = Text
            else:
                precision = '1'
                ctype = Integer
        nvals = desc.get('nvals', 1)
        units = desc.get('units', '')
        scale = desc.get('scale', 1.0)
        if nvals == 1:
            cols.append(Column(desc['name'], ctype))
            metadata.append((sensor, desc['name'], units, precision, scale))
        else:
            for i in range(nvals):
                colname = '{0}_{1:d}'.format(desc['name'], i)
                cols.append(Column(colname, ctype))
                metadata.append((sensor, colname, units, precision, scale))
    tbl = Table(sensor, meta, *cols)
    meta.create_all(eng)
    mdtable = meta.tables.get('metadata')
    if mdtable is not None:
        mdcols = ('sensor', 'varname', 'units', 'precision', 'scale')
        conn = eng.connect()
        conn.execute(mdtable.insert(),
                     [dict(zip(mdcols, m)) for m in metadata])

    return tbl
