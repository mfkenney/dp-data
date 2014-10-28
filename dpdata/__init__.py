from pkg_resources import resource_string
import yaml


__version__ = '0.5'


def expand_lists(data):
    """
    Given a dictionary of data values, return a new dictionary
    with all list values expanded, e.g:

      If data[name] == val where val is a list

        name_0: val[0], name_1: val[1], ..., name_N: val[N]
    """
    newdict = data.copy()
    for k, v in data.iteritems():
        if isinstance(v, list) or isinstance(v, tuple):
            d = dict([(k + '_' + str(i), e) for i, e in enumerate(v)])
            del newdict[k]
            newdict.update(d)
    return newdict


def data_dictionary():
    """
    Return the Deep Profiler data dictionary.
    """
    return yaml.load(resource_string(__name__, 'data_dictionary.yaml'))
