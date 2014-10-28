Deep Profiler Data Processing
=============================

This package contains modules and scripts for working with data
from the RSN Deep Profiler.

Installation
------------

This package uses setup-tools to pull in a number of dependencies
so I suggest installing in a virtualenv::

  mkvirtualenv dp
  pip install Dpdata-<VERSION>.tar.gz


Scripts
-------

Run a script with the ``--help`` command-line option to display
a usage message.

mktables
    Uses the Data Dictionary (see ``dpdata/data_dictonary.yaml``)
    to create an SQL table schema. Has been tested with SQLite
    but should work with any database supported by the
    SQLAlchemy package.

mpk2sql
    Reads one or more MessagePack format data files and stores the
    contents in an SQL database initialized by ``mktables``.

mpk2csv
    Dump the contents on one or more MessagePack format data files
    as CSV.

dp2sql
    Subscribe to a real-time Deep Profiler data stream and log the
    data to an SQL database initialized by ``mktables`` (**UNTESTED**)
