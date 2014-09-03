#!/usr/bin/env python
from distutils.core import setup
import os
import codecs
import re


here = os.path.abspath(os.path.dirname(__file__))

# Read the version number from a source file.
# Why read it, and not import?
# see https://groups.google.com/d/topic/pypa-dev/0PkjVpcxTzQ/discussion
def find_version(*file_paths):
    # Open in Latin-1 so that we avoid encoding errors.
    # Use codecs.open for Python 2 compatibility
    with codecs.open(os.path.join(here, *file_paths), 'r', 'latin1') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return str(version_match.group(1))
    raise RuntimeError("Unable to find version string.")


setup(name="Dpdata",
      version=find_version('dpdata', '__init__.py'),
      description="RSN Deep Profiler data processing software",
      author="Michael Kenney",
      author_email="mikek@apl.uw.edu",
      url="http://wavelet.apl.uw.edu/~mike/python/",
      packages=["dpdata"],
      scripts=[])
