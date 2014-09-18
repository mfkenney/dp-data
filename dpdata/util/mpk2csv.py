#!/usr/bin/env python
"""
Dump the contents of a Deep Profiler MessagePack archive file
in CSV format.
"""
import sys
import argparse
from dpdata import expand_lists, data_dictionary
from dpdata.mpk import get_records
from decimal import Decimal
from functools import partial


def quantize(scale, prec, val):
    return str(Decimal(val*scale).quantize(prec))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('name', help='sensor name')
    parser.add_argument('infiles', help='input files',
                        type=argparse.FileType('rb'),
                        nargs='+')
    parser.add_argument('-o', metavar='FILE', help='output file',
                        dest='output',
                        type=argparse.FileType('wb'),
                        default=sys.stdout)
    args = parser.parse_args()

    try:
        cfg = (data_dictionary())[args.name]
    except KeyError:
        raise RuntimeError('Invalid sensor name: {0}'.format(args.name))

    varnames = []
    fcvt = {}
    for vdesc in cfg['data']:
        n = vdesc.get('nvals', 1)
        func = vdesc.get('tostr',
                         partial(quantize,
                                 vdesc.get('scale', 1.),
                                 Decimal(vdesc.get('precision', '1'))))
        if n > 1:
            for i in range(n):
                name = '{0}_{1:d}'.format(vdesc['name'], i)
                varnames.append(name)
                fcvt[name] = func
        else:
            varnames.append(vdesc['name'])
            fcvt[vdesc['name']] = func

    varnames.sort()
    args.output.write(','.join(['t_secs', 't_usecs'] + varnames) + '\n')
    for f in args.infiles:
        for secs, usecs, data in get_records(f):
            values = expand_lists(data)
            rec = [str(secs), str(usecs)] + [fcvt[v](values[v]) for v in varnames]
            args.output.write(','.join(rec) + '\n')


if __name__ == '__main__':
    main()
