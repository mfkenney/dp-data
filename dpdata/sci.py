#!/usr/bin/env python
"""
.. module:: sci
   :synopsis: Various data processing functions.
"""
import numpy as np
import gsw
import pandas as pd


def dosv(Pt, T, S, P, Pdens, fc):
    """
    Calculate dissolved oxygen from Optode bphase and
    temperature using the modified Stern-Volmer equation.

    :param Pt: Optode bphase in degrees
    :type Pt: numpy array-like
    :param T: Optode temperature in degrees-C
    :type T: numpy array-like
    :param S: in-situ salinity in psu
    :type S: numpy array-like
    :param P: in-situ pressure in dbar
    :type P: numpy array-like
    :param Pdens: potential density in kg/m^3
    :type Pdens: numpy array-like
    :param fc: Optode foil coefficients
    :returns: dissolved oxygen in umol/kg
    :rtype: numpy array-like
    """
    assert len(fc) == 7

    # Calculate dissolved-oxygen (do) using Stern-Volmer
    ksv = fc[0] + fc[1]*T + fc[2]*T*T
    p0 = fc[3] + fc[4]*T
    pc = fc[5] + fc[6]*Pt
    do = (p0 / pc - 1.) / ksv

    # Convert from volume to mass units
    do = 1000. * do / Pdens

    # Pressure correction
    pcomp = 1. + (0.032 * P) / 1000.
    do = pcomp * do

    # Salinity correction
    B = np.array([-6.24097e-3, -6.93498e-3, -6.90358e-3, -4.29155e-3],
                 dtype='f8')
    C0 = -3.11680e-7
    ts = np.log((298.15 - T)/(273.15 + T))
    btmp = B[0] + ts*(B[1] + ts*(B[2] + ts*B[3]))
    scomp = np.exp(S * btmp) + C0*S*S

    return scomp * do


def process_optode(ctd, optode, fc, lat=0, lon=0):
    """
    Calculate dissolved oxygen from processed CTD and raw
    Optode data.

    :param ctd: processed CTD data-set
    :type ctd: :class:`pandas.DataFrame`
    :param optode: raw Optode data-set
    :type ctd: :class:`pandas.DataFrame`
    :param fc: Optode foil calibration coefficients
    :type fc: numpy array
    :param lat: data-set latitude in degrees
    :param lon: data-set longitude in degrees
    :returns: processed Optode data-set
    :rtype: :class:`pandas.DataFrame`
    """
    # Interpolate CTD data onto the sample times of
    # the Optode data
    d = {}
    nan = float('NaN')
    for column in ('pracsal', 'tempwat', 'preswat'):
        d[column] = np.interp(optode['timestamp'],
                              ctd['timestamp'],
                              ctd[column],
                              left=nan,
                              right=nan)
    # Mask off any sample points that are outside of the
    # interpolation range.
    mask = ~(np.isnan(d['pracsal']))
    sa = gsw.SA_from_SP(d['pracsal'][mask], d['preswat'][mask], lon, lat)
    ct = gsw.CT_from_t(sa, d['tempwat'][mask], d['preswat'][mask])
    pdens = gsw.rho(sa, ct, np.zeros(len(sa)))
    do = dosv(optode['doconcs'][mask],
              optode['t'][mask],
              d['pracsal'][mask],
              d['preswat'][mask],
              pdens, fc)
    return pd.DataFrame({'timestamp': optode['timestamp'][mask],
                         'doxygen': do,
                         'preswat': d['preswat'][mask]})


def process_ctd(ctd, lat=0, lon=0):
    """
    Calculate practical-salinity and in-situ density from a raw
    CTD data-set.

    :param ctd: raw CTD data-set
    :type ctd: :class:`pandas.DataFrame`
    :param lat: data-set latitude in degrees
    :param lon: data-set longitude in degrees
    :returns: processed CTD data-set
    :rtype: :class:`pandas.DataFrame`
    """
    pracsal = gsw.SP_from_C(ctd['condwat'], ctd['tempwat'], ctd['preswat'])
    sa = gsw.SA_from_SP(pracsal, ctd['preswat'], lon, lat)
    ct = gsw.CT_from_t(sa, ctd['tempwat'], ctd['preswat'])
    density = gsw.rho(sa, ct, ctd['preswat'])
    return pd.DataFrame({'timestamp': ctd['timestamp'],
                         'pracsal': pracsal,
                         'tempwat': ctd['tempwat'],
                         'preswat': ctd['preswat'],
                         'density': density})


def process_flntu(ctd, flntu, c_chla, c_ntu):
    """
    Process the FLNTU data by converting from A/D counts to physical units
    and adding a pressure record.

    :param ctd: processed CTD data-set
    :type ctd: :class:`pandas.DataFrame`
    :param flntu: raw FLNTU data-set
    :type flntu: :class:`pandas.DataFrame`
    :param c_chla: coefficients for Chlorophyl-a values
    :type c_chla: 2-element numpy array (dark counts, scale factor)
    :param c_ntu: coefficients for NTU values
    :type c_ntu: 2-element numpy array (dark counts, scale factor)
    :returns: processed Optode data-set
    :rtype: :class:`pandas.DataFrame`
    """
    # Interpolate CTD pressure record onto the sample times of
    # the FLNTU data.
    nan = float('NaN')
    pr = np.interp(flntu['timestamp'],
                   ctd['timestamp'],
                   ctd['preswat'],
                   left=nan,
                   right=nan)
    # Mask off any sample points that are outside of the
    # interpolation range.
    mask = ~(np.isnan(pr))
    chla = c_chla[1] * (flntu['chlaflo'][mask] - c_chla[0])
    ntu = c_ntu[1] * (flntu['ntuaflo'][mask] - c_ntu[0])
    return pd.DataFrame({'timestamp': flntu['timestamp'][mask],
                         'chla': chla,
                         'ntu': ntu,
                         'preswat': pr[mask]})


def process_flcd(ctd, flcd, c_cdom):
    """
    Process the FLCD data by converting from A/D counts to physical units
    and adding a pressure record.

    :param ctd: processed CTD data-set
    :type ctd: :class:`pandas.DataFrame`
    :param flcd: raw FLCD data-set
    :type flcd: :class:`pandas.DataFrame`
    :param c_cdom: coefficients for CDOM values
    :type c_cdom: 2-element numpy array (dark counts, scale factor)
    :returns: processed FLCD data-set
    :rtype: :class:`pandas.DataFrame`
    """
    # Interpolate CTD pressure record onto the sample times of
    # the FLCD data.
    nan = float('NaN')
    pr = np.interp(flcd['timestamp'],
                   ctd['timestamp'],
                   ctd['preswat'],
                   left=nan,
                   right=nan)
    # Mask off any sample points that are outside of the
    # interpolation range.
    mask = ~(np.isnan(pr))
    cdom = c_cdom[1] * (flcd['cdomflo'][mask] - c_cdom[0])
    return pd.DataFrame({'timestamp': flcd['timestamp'][mask],
                         'cdom': cdom,
                         'preswat': pr[mask]})
