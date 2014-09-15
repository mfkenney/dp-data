#!/usr/bin/env python
"""
.. module:: sql
   :synopsis: Interface to DP SQL database.
"""
import pandas as pd
from sqlalchemy import Table, MetaData, select, Column, Integer,\
    Float, Text
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
    if mdtable:
        mdcols = ('sensor', 'varname', 'units', 'precision', 'scale')
        conn = eng.connect()
        conn.execute(mdtable.insert(),
                     [dict(zip(mdcols, m)) for m in metadata])

    return tbl
