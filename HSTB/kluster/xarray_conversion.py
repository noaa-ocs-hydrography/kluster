import os
from glob import glob
from dask.distributed import Client, Future, progress
import webbrowser
from time import perf_counter
from sortedcontainers import SortedDict
import json
from datetime import datetime
import xarray as xr
import numpy as np
from typing import Union
import logging

from HSTB.kluster import __version__ as klustervers
from HSTB.kluster.dms import return_zone_from_min_max_long
from HSTB.kluster.fqpr_drivers import sequential_read_multibeam, fast_read_multibeam_metadata, return_offsets_from_posfile, \
    sonar_reference_point, par_sonar_translator, kmall_sonar_translator
from HSTB.kluster.fqpr_vessel import only_retain_earliest_entry
from HSTB.kluster.dask_helpers import dask_find_or_start_client
from HSTB.kluster.xarray_helpers import resize_zarr, xarr_to_netcdf, combine_xr_attributes, reload_zarr_records, slice_xarray_by_dim, fix_xarray_dataset_index
from HSTB.kluster.fqpr_helpers import seconds_to_formatted_string
from HSTB.kluster.backends._zarr import ZarrBackend, my_xarr_add_attribute
from HSTB.kluster.logging_conf import return_logger, return_log_name
from HSTB.kluster.modules.georeference import distance_between_coordinates
from HSTB.kluster import kluster_variables


sonar_translator = {'ek60': [None, 'tx', 'rx', None], 'ek80': [None, 'tx', 'rx', None],
                    'em122': [None, 'tx', 'rx', None], 'em302': [None, 'tx', 'rx', None], 'em304': [None, 'tx', 'rx', None],
                    'em710': [None, 'tx', 'rx', None], 'em712': [None, 'tx', 'rx', None], 'em2040': [None, 'tx', 'rx', None],
                    'em2040_dual_rx': [None, 'tx', 'rx_port', 'rx_stbd'],
                    'em2040_dual_tx': ['tx_port', 'tx_stbd', 'rx_port', None],
                    'em2040_dual_tx_rx': ['tx_port', 'tx_stbd', 'rx_port', 'rx_stbd'],
                    # EM2040c is represented in the .all file as em2045
                    'em2045': [None, 'txrx', None, None], 'em2045_dual': [None, 'txrx_port', 'txrx_stbd', None],
                    'em3002': [None, 'tx', 'rx', None], 'em2040p': [None, 'txrx', None, None],
                    'em3020': [None, 'tx', 'rx', None], 'em3020_dual': [None, 'txrx_port', 'txrx_stbd', None],
                    'me70': [None, 'txrx', None, None], '7125': [None, 'tx', 'rx', None], 't20': [None, 'tx', 'rx', None],
                    't50': [None, 'tx', 'rx', None], 't51': [None, 'tx', 'rx', None]}

# ensure that Kluster sonar translator supports all sonar_translators in multibeam drivers
assert all([snr in sonar_translator for snr in par_sonar_translator.keys()])
assert all([snr in sonar_translator for snr in kmall_sonar_translator.keys()])


install_parameter_modifier = {'em2040_dual_tx_rx': {'rx_port': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                                '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                                '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}},
                                                    'tx_port': {'0': {'x': 0.0, 'y': -0.0554, 'z': -0.012},
                                                                '1': {'x': 0.0, 'y': 0.0131, 'z': -0.006},
                                                                '2': {'x': 0.0, 'y': 0.0554, 'z': -0.012}},
                                                    'rx_stbd': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                                '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                                '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}},
                                                    'tx_stbd': {'0': {'x': 0.0, 'y': -0.0554, 'z': -0.012},
                                                                '1': {'x': 0.0, 'y': 0.0131, 'z': -0.006},
                                                                '2': {'x': 0.0, 'y': 0.0554, 'z': -0.012}}},
                              'em2040_dual_rx': {'tx': {'0': {'x': 0.0, 'y': -0.0554, 'z': -0.012},
                                                        '1': {'x': 0.0, 'y': 0.0131, 'z': -0.006},
                                                        '2': {'x': 0.0, 'y': 0.0554, 'z': -0.012}},
                                                 'rx_port': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}},
                                                 'rx_stbd': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}}},
                              'em2040': {'rx': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}},
                                         'tx': {'0': {'x': 0.0, 'y': -0.0554, 'z': -0.012},
                                                '1': {'x': 0.0, 'y': 0.0131, 'z': -0.006},
                                                '2': {'x': 0.0, 'y': 0.0554, 'z': -0.012}}},
                              'em2045': {'rx': {'0': {'x': -0.0455, 'y': 0.0, 'z': -0.006},
                                                '1': {'x': -0.0455, 'y': 0.0, 'z': -0.006}},
                                         'tx': {'0': {'x': 0.0038, 'y': 0.040, 'z': -0.006},
                                                '1': {'x': 0.0038, 'y': 0.040, 'z': -0.006}}},
                              'em2040p': {'rx': {'0': {'x': 0.204, 'y': 0.0, 'z': -0.0315},
                                                 '1': {'x': 0.204, 'y': 0.0, 'z': -0.0315},
                                                 '2': {'x': 0.204, 'y': 0.0, 'z': -0.0315}},
                                          'tx': {'0': {'x': 0.002, 'y': -0.1042, 'z': -0.0149},
                                                 '1': {'x': 0.002, 'y': 0.0, 'z': -0.006},
                                                 '2': {'x': 0.002, 'y': 0.1042, 'z': -0.0149}}},
                              }


def _xarr_is_bit_set(da: xr.DataArray, bitpos: int):
    """
    Check if bit is set using the Xarray DataArray and bit position.  Returns True if set.

    Parameters
    ----------
    da
        xarray DataArray containing binary flag as integer
    bitpos
        integer offset representing bit posititon (3 to check 3rd bit)

    Returns
    -------
    bool
        True if bitpos bit is set in val
    """

    try:
        # get integer value from bitpos and return True if set in num
        chk = da & (1 << (bitpos - 1))
    except TypeError:
        # won't work on dask arrays or floats, compute to get numpy
        chk = da.compute().astype(int) & (1 << (bitpos-1))

    return chk.astype(bool)


def _run_sequential_read(fildata: list):
    """
    Function for managing par.sequential_read_records.  Takes in multibeam files, outputs dict of records

    Parameters
    ----------
    fildata
        contains chunk locations, see _batch_read_chunk_generation

    Returns
    -------
    dict
        Dictionary where keys are the datagram type numbers and values are dicts of columns/rows from datagram
    """

    fil, offset, endpt = fildata
    recs = sequential_read_multibeam(fil, offset, endpt)
    return recs


def _build_serial_mask(rec: dict):
    """
    Range/Angle datagram is going to have multiple systems inside of it when dual head.  Not good for downstream processing
    that becomes much more complex tracking these.  Here we build the mask that allows us to separate into two datasets by
    serial number if required

    Parameters
    ----------
    rec
        dict as returned by sequential_read_records

    Returns
    -------
    list
        contains string identifiers for each sector, ex: [40111, 40111, 40112, 40112, 40111, 40111]
    list
        index of where each sector_id identifier shows up in the data
    """

    serial_nums = list(np.unique(rec['ping']['serial_num']))
    sector_ids = []
    id_mask = []

    for x in serial_nums:
        ser_mask = np.where(rec['ping']['serial_num'] == float(x))[0]
        sector_ids.append(str(x))
        id_mask.append(ser_mask)
    return sector_ids, id_mask


def _assign_reference_points(fileformat: str, finalraw: dict, finalatt: xr.Dataset):
    """
    Use what we've learned from reading these multibeam data definition documents to record the relevant information
    about each variable.  Logs this info as attributes in the final xarray dataset.  'reference' refers to the
    reference point of each variable.

    Parameters
    ----------
    fileformat
        multibeam file format (all, kmall supported currently)
    finalraw
        dict of xarray Datasets corresponding to each serial number in the file
    finalatt
        xarray dataset containing the attitude records

    Returns
    -------
    dict
        dict of xarray Datasets corresponding to each serial number in the file
    xr.Dataset
        finalatt dataset with new attribution
    """

    try:
        if '.' + fileformat in kluster_variables.supported_sonar:
            finalatt.attrs['reference'] = {'heading': 'reference point', 'heave': 'transmitter',
                                           'pitch': 'reference point', 'roll': 'reference point'}
            finalatt.attrs['units'] = {'heading': 'degrees (+ clockwise)', 'heave': 'meters (+ down)', 'pitch': 'degrees (+ bow up)',
                                       'roll': 'degrees (+ port up)'}
            for systemid in finalraw:
                finalraw[systemid].attrs['kluster_convention'] = {'x': '+ Forward', 'y': '+ Starboard', 'z': '+ Down',
                                                                  'roll': '+ Port Up', 'pitch': '+ Bow Up', 'gyro': '+ Clockwise'}
                finalraw[systemid].attrs['sonar_reference_point'] = sonar_reference_point['.' + fileformat]
                finalraw[systemid].attrs['reference'] = {'beampointingangle': 'receiver', 'delay': 'None', 'frequency': 'None',
                                                         'soundspeed': 'None', 'tiltangle': 'transmitter', 'reflectivity': 'None',
                                                         'traveltime': 'None', 'latitude': 'reference point',
                                                         'longitude': 'reference point', 'altitude': 'reference point'}
                finalraw[systemid].attrs['units'] = {'beampointingangle': 'degrees', 'delay': 'seconds', 'frequency': 'hertz',
                                                     'soundspeed': 'meters per second', 'tiltangle': 'degrees', 'reflectivity': 'decibels',
                                                     'traveltime': 'seconds', 'latitude': 'degrees', 'longitude': 'degrees',
                                                     'altitude': 'meters (+ up)'}
            return finalraw, finalatt
        else:
            raise ValueError('Did not recognize format "{}" during xarray conversion'.format(fileformat))
    except KeyError:
        raise KeyError('Did not find the "format" key in the sequential read output')


def _is_not_empty_sequential(rec: dict):
    """
    Sometimes we get chunks or even files without ping records, if that happens, we use this function to determine it
    is empty and we can drop the future.

    Parameters
    ----------
    rec
        as returned by sequential_read_records

    Returns
    -------
    bool
        If the chunk returned is full, this returns True (tells us to keep it)
    """

    if rec['ping']['time'].any() and rec['attitude']['time'].any():
        return True
    return False


def simplify_soundvelocity_profile(profile: np.ndarray):
    """
    Kluster currently has a memory issue when sound velocity correcting with very large cast tables.  If the profile
    has too many layers, the memory usage will blow up.  Limit the cast layers to the max_profile_length variable in
    kluster_variables.

    Profiles that are too long are interpolated here

    Parameters
    ----------
    profile

    Returns
    -------
    np.ndarray
        Either interpolated cast record or the original if the original was not too large
    """

    if profile.shape[0] > kluster_variables.max_profile_length:
        # print('WARNING: Found a sound velocity profile with {} layers, interpolating to a maximum of {} layers'.format(profile.shape[0], kluster_variables.max_profile_length))
        if profile[-1, 0] == 12000.0:  # this is an added on value by Kongsberg, linspace with the original profile depths to not throw off the step size
            new_depths = np.linspace(profile[0, 0], profile[-2, 0], num=kluster_variables.max_profile_length - 1)
            new_depths = np.concatenate([new_depths, [12000.0]])
            extended_cast = True
        else:
            new_depths = np.linspace(profile[0, 0], profile[-1, 0], num=kluster_variables.max_profile_length)
            extended_cast = False
        new_vals = np.interp(new_depths, profile[:, 0], profile[:, 1])
        profile = np.dstack([new_depths, new_vals])[0]
    return profile


def _sequential_to_xarray(rec: dict):
    """
    After running sequential read, this method will take in the dict of datagrams and return an xarray for rangeangle,
    attitude and navigation.  Three arrays for the three different time series we are interested in (full time series
    att and nav are useful later on)

    Parameters
    ----------
    rec
        as returned by sequential_read_records

    Returns
    -------
    xr.Dataset
        ping records as Dataset, timestamps as coordinates, metadata as attribution
    xr.Dataset
        attitude records as Dataset, timestamps as coordinates, metadata as attribution
    """

    if 'ping' not in rec:
        print('No ping raw range/angle record found for chunk file')
        return
    recs_to_merge = {}
    for r in rec:
        if r not in ['installation_params', 'profile', 'format']:  # These are going to be added as attributes later
            if r == 'ping':  # R&A is the only datagram we use that requires splitting by serial#
                ids, msk = _build_serial_mask(rec)  # get the identifiers and mask for each serial#
                recs_to_merge[r] = {systemid: xr.Dataset() for systemid in ids}
                for systemid in ids:
                    idx = ids.index(systemid)

                    for ky in rec[r]:
                        tim = rec['ping']['time'][msk[idx]]
                        if ky not in ['time', 'serial_num']:
                            if ky == 'counter':  # counter is 16bit in raw data, we want 64 to handle zero crossing
                                datadtype = np.int64
                            else:
                                datadtype = rec[r][ky].dtype
                            arr = np.array(rec['ping'][ky][msk[idx]])  # that part of the record for the given sect_id

                            # currently i'm getting a one rec duplicate between chunked files...
                            if tim.size > 1 and tim[-1] == tim[-2] and np.array_equal(arr[-1], arr[-2]):
                                # print('Found duplicate timestamp: {}, {}, {}'.format(r, ky, tim[-1]))
                                arr = arr[:-1]
                                tim = tim[:-1]

                            # these records are by time/beam.  Have to combine recs to build correct array shape
                            if ky in kluster_variables.subset_variable_2d:
                                beam_idx = np.arange(arr.shape[1])
                                try:
                                    recs_to_merge[r][systemid][ky] = xr.DataArray(arr.astype(datadtype), coords=[tim, beam_idx], dims=['time', 'beam'])
                                except:
                                    raise ValueError(f'_sequential_to_xarray: Found record {ky} with shape {arr.shape}, expected a two dimensional array')
                            #  everything else isn't by beam, proceed normally
                            else:
                                try:
                                    recs_to_merge[r][systemid][ky] = xr.DataArray(arr.astype(datadtype), coords=[tim], dims=['time'])
                                except:
                                    raise ValueError(f'_sequential_to_xarray: Found record {ky} with shape {arr.shape}, expected a one dimensional array')
                    try:
                        recs_to_merge[r][systemid] = recs_to_merge[r][systemid].sortby('time')
                    except:  # no records to sort
                        pass
            else:
                recs_to_merge[r] = xr.Dataset()
                for ky in rec[r]:
                    if ky in ['heading', 'heave', 'pitch', 'roll', 'altitude']:
                        recs_to_merge[r][ky] = xr.DataArray(np.float32(rec[r][ky]), coords=[rec[r]['time']], dims=['time'])
                    elif ky not in ['time', 'runtime_settings']:
                        recs_to_merge[r][ky] = xr.DataArray(rec[r][ky], coords=[rec[r]['time']], dims=['time'])
                try:
                    recs_to_merge[r] = recs_to_merge[r].sortby('time')
                except:  # no records to sort
                    pass

    for systemid in recs_to_merge['ping']:
        if 'mode' not in recs_to_merge['ping'][systemid]:
            #  .all workflow, where the mode/modetwo stuff is not in the ping record, so you have to merge the two
            # take range/angle rec and merge runtime on to that index
            chk = True
            if 'runtime_params' not in list(recs_to_merge.keys()) or (recs_to_merge['runtime_params'].time.shape[0] == 0):
                chk = False
            if not chk:
                # rec82 (runtime params) isn't mandatory, but all datasets need to have the same variables or else the
                #    combine_nested isn't going to work.  So if it isn't in rec (or it is empty) put in an empty dataset here
                #    to be interpolated through later after merge
                recs_to_merge['runtime_params'] = xr.Dataset(data_vars={'mode': (['time'], np.array([''])),
                                                                        'modetwo': (['time'], np.array([''])),
                                                                        'yawpitchstab': (['time'], np.array(['']))},
                                                             coords={'time': np.array([float(recs_to_merge['ping'][systemid].time[0])])})

            _, index = np.unique(recs_to_merge['runtime_params']['time'], return_index=True)
            recs_to_merge['runtime_params'] = recs_to_merge['runtime_params'].isel(time=index)
            recs_to_merge['runtime_params'] = recs_to_merge['runtime_params'].reindex_like(recs_to_merge['ping'][systemid], method='nearest')
            recs_to_merge['ping'][systemid] = xr.merge([recs_to_merge['ping'][systemid], recs_to_merge['runtime_params']], join='inner')

    # attitude is returned separately in its own dataset
    #  retain only unique values and recs where time != 0 (this occassionally seems to happen on read)
    #   (numpy unique seems to return indices of values=0 first, thought about using that but not sure if reliable)
    _, index = np.unique(recs_to_merge['attitude']['time'], return_index=True)
    tot_records = len(recs_to_merge['attitude']['time'])
    dups = tot_records - len(index)
    finalatt = recs_to_merge['attitude'].isel(time=index)
    zero_index = np.where(finalatt.time != 0)[0]
    zeros = len(finalatt.time) - len(zero_index)
    finalatt = finalatt.isel(time=zero_index)

    _, index = np.unique(recs_to_merge['navigation']['time'], return_index=True)
    tot_records = len(recs_to_merge['navigation']['time'])
    dups = tot_records - len(index)
    finalnav = recs_to_merge['navigation'].isel(time=index)
    for systemid in recs_to_merge['ping']:
        interp_nav = finalnav.reindex_like(recs_to_merge['ping'][systemid], method='nearest', tolerance=kluster_variables.max_nav_tolerance)
        recs_to_merge['ping'][systemid] = xr.merge([recs_to_merge['ping'][systemid], interp_nav])
        # build attributes for the navigation/attitude records
        recs_to_merge['ping'][systemid].attrs['min_lat'] = float(finalnav.latitude.min())
        recs_to_merge['ping'][systemid].attrs['max_lat'] = float(finalnav.latitude.max())
        recs_to_merge['ping'][systemid].attrs['min_lon'] = float(finalnav.longitude.min())
        recs_to_merge['ping'][systemid].attrs['max_lon'] = float(finalnav.longitude.max())

    # add a shortcut to get to georeferencing if this converted data has alongtrack/acrosstrack/depth already (see raw driver when .out/.bot files exist)
    for systemid in recs_to_merge['ping']:
        if 'depthoffset' in rec['ping']:
            recs_to_merge['ping'][systemid].attrs['skip_to_georeferencing'] = True
        break

    # Stuff that isn't of the same dimensions as the dataset are tacked on as attributes
    if 'profile' in rec:
        for t in rec['profile']['time']:
            idx = np.where(rec['profile']['time'] == t)
            profile = np.dstack([rec['profile']['depth'][idx][0], rec['profile']['soundspeed'][idx][0]])[0]
            profile = simplify_soundvelocity_profile(profile)
            for systemid in recs_to_merge['ping']:
                cst_name = 'profile_{}'.format(int(t))
                attrs_name = 'attributes_{}'.format(int(t))
                recs_to_merge['ping'][systemid].attrs[cst_name] = json.dumps(profile.tolist())
                nearestnav = recs_to_merge['ping'][systemid].sel(time=int(t), method='nearest')
                castlocation = [float(nearestnav.latitude), float(nearestnav.longitude)]
                recs_to_merge['ping'][systemid].attrs[attrs_name] = json.dumps({'location': castlocation, 'source': 'multibeam'})

    # add on attribute for installation parameters, basically the same way as you do for the ss profile, except it
    #   has no coordinate to index by.  Also, use json.dumps to avoid the issues with serializing lists/dicts with
    #   to_netcdf

    # I'm including these serial numbers for the dual/dual setup.  System is port, Secondary_system is starboard.  These
    #   are needed to identify pings and which offsets to use (TXPORT=Transducer0, TXSTARBOARD=Transducer1,
    #   RXPORT=Transducer2, RXSTARBOARD=Transudcer3)

    for systemid in recs_to_merge['ping']:
        recs_to_merge['ping'][systemid] = recs_to_merge['ping'][systemid].sortby('time')
        if 'installation_params' in rec:
            recs_to_merge['ping'][systemid].attrs['system_serial_number'] = np.unique(rec['installation_params']['serial_one'])
            recs_to_merge['ping'][systemid].attrs['secondary_system_serial_number'] = np.unique(rec['installation_params']['serial_two'])
            for t in rec['installation_params']['time']:
                idx = np.where(rec['installation_params']['time'] == t)
                recs_to_merge['ping'][systemid].attrs['installsettings_{}'.format(int(t))] = json.dumps(rec['installation_params']['installation_settings'][idx][0])
        if 'runtime_params' in rec:
            for t in rec['runtime_params']['time']:
                idx = np.where(rec['runtime_params']['time'] == t)
                if rec['runtime_params']['runtime_settings'][idx][0]:  # this might be empty dict if we trimmed it in par3/kmall for being a duplicate
                    recs_to_merge['ping'][systemid].attrs['runtimesettings_{}'.format(int(t))] = json.dumps(rec['runtime_params']['runtime_settings'][idx][0])

    # assign reference point and metadata
    finalraw = recs_to_merge['ping']
    finalraw, finalatt = _assign_reference_points(rec['format'], finalraw, finalatt)

    return finalraw, finalatt


def _divide_xarray_futs(xarrfuture: list, mode: str = 'ping'):
    """
    The return from _sequential_to_xarray is a future containing three xarrays.  Map this function to access that future
    and return the xarray specified with the mode keyword.

    Parameters
    ----------
    xarrfuture
        list of xarray Datasets from _sequential_to_xarray

    Returns
    -------
    xr.Dataset
        selected datatype specified by mode
    """

    idx = ['ping', 'attitude'].index(mode)
    return xarrfuture[idx]


def _return_xarray_mintime(xarrs: Union[xr.DataArray, xr.Dataset, dict]):
    """
    Access xarray object and return the length of the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the minimum time from

    Returns
    -------
    float
        minimum time of the array
    """
    try:
        return float(xarrs.time.min())
    except:  # dict provided, raw ping split by system identifier
        return float(list(xarrs.values())[0].time.min())


def _return_xarray_time_bounds(xarrs: Union[xr.DataArray, xr.Dataset, dict]):
    """
    Access xarray object and return the (min, max) of the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the time bounds from

    Returns
    -------
    tuple
        (min time, max time) in utc seconds
    """
    try:
        return (float(xarrs.time.min()), float(xarrs.time.max()))
    except:  # dict provided, raw ping split by system identifier
        return (float(list(xarrs.values())[0].time.min()), float(list(xarrs.values())[0].time.max()))


def _trim_to_time_bounds(xarr: Union[xr.DataArray, xr.Dataset, dict], time_bounds: tuple):
    return xarr.where(np.logical_and(xarr.time > time_bounds[0], xarr.time < time_bounds[1]), drop=True)


def _return_xarray_timelength(xarrs: xr.Dataset):
    """
    Access xarray object and return the length of the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the timelength from

    Returns
    -------
    int
        length of time dimension
    """

    return xarrs.dims['time']


def _divide_xarray_return_system(xarr: dict, sysid: str):
    """
    Take in a ping xarray Dataset and return just the secid sector

    Parameters
    ----------
    xarr
        dict of xarray Datasets
    sysid
        system identifier that you are searching for

    Returns
    -------
    xr.Dataset
        selected datatype specified by mode
    """

    if sysid not in list(xarr.keys()):
        return None

    xarr_by_sysid = xarr[sysid]

    return xarr_by_sysid


def _divide_xarray_indicate_empty_future(fut: Union[None, xr.Dataset]):
    """
    Operations that result in an empty time array, this function indicates this

    Parameters
    ----------
    fut
        Dataset if fut is valid, NoneType if empty

    Returns
    -------
    bool
        True if fut is data, False if not
    """

    if fut is None:
        return False
    elif np.array_equal(fut.time.values, np.array([])):
        return False
    else:
        return True


def _return_xarray_time(xarrs: Union[xr.DataArray, xr.Dataset]):
    """
    Access xarray object and return the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the time array from

    Returns
    -------
    xarray DataArray
        time array
    """

    return xarrs['time']


def _return_xarray_beam(xarrs: Union[xr.DataArray, xr.Dataset]):
    """
    Access xarray object and return the number of beams

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the time array from

    Returns
    -------
    int
        number of beams
    """

    return int(xarrs['beam'].shape[0])


def _return_xarray_constant_blocks(xlens: list, xarrfutures: list, rec_length: int):
    """
    Sequential read operates on a file level.  Chunks determined for what makes sense in terms of distributed
    processing.  For use with netcdf/zarr datastores, we'd ideally like to have equal length chunks across workers
    (where each worker writes to a different netcdf file/zarr group area).  This method will build out lists of
    xarray futures for each worker to combine in order to get constant chunk size equal to the rec_length parameter.

    Parameters
    ----------
    xlens
        list of int, length of the time dimension for each array, same order as xarrfutures
    xarrfutures
        list of dask futures, future represents xarray dataset for chunk
    rec_length
        int, length of time dimension for output block, equal to the chunksize of that datatype (ping, nav, etc)

    Returns
    -------
    list
        list of lists where each inner list is [start, end, xarray future].  For use with _merge_constant_blocks, start and end correspond to the time dimension
    totallen
        total number of time values across all chunks
    """

    newxarrs = []
    cur_req = rec_length
    bufr = []
    totallen = 0
    for ct, l in enumerate(xlens):
        # range angle length from this worker/future
        arrlen = l
        # Just in case you get to zero, start over
        if cur_req == 0:
            newxarrs.append(bufr)
            bufr = []
            cur_req = rec_length
        # if the length of the rec is less than the desired blocksize,
        #    add it to the buffer and move on
        if arrlen <= cur_req:
            bufr.append([0, arrlen, xarrfutures[ct]])
            totallen += arrlen
            cur_req -= arrlen
        # if the length is greater than blocksize, add enough to get blocksize
        #    and attach the rest to overflow for the next block
        elif arrlen > cur_req:
            start_idx = 0
            while arrlen > cur_req:
                bufr.append([start_idx, cur_req, xarrfutures[ct]])
                newxarrs.append(bufr)
                bufr = []
                totallen += cur_req - start_idx
                start_idx = cur_req
                cur_req += rec_length
            if start_idx:
                cur_req -= rec_length

            bufr = [[cur_req, arrlen, xarrfutures[ct]]]
            totallen += (arrlen - cur_req)
            cur_req = rec_length - (arrlen - cur_req)
    newxarrs.append(bufr)
    return newxarrs, totallen


def _return_xarray_system_ids(xarrs: dict):
    """
    Return the system ids for the given xarray object

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the sectors from

    Returns
    -------
    list
        system identifiers as string within a list
    """

    return list(xarrs.keys())


def _merge_constant_blocks(newblocks: list):
    """
    Accepts output from _return_xarray_constant_blocks and performs a nested concat/merge on given blocks.

    Parameters
    ----------
    newblocks
        [time_start, time_end, xarray Dataset] where time_start and time_end are ints for time indexes

    Returns
    -------
    xr.Dataset
        all blocks merged along time dimension
    """
    xarrs = [i[2].isel(time=slice(i[0], i[1])) for i in newblocks]
    finalarr = xr.combine_nested(xarrs, 'time')
    # can't have duplicate times in xarray dataset, apply tiny offset in time to handle this
    finalarr = finalarr.sortby('time')
    duptimes = np.where(np.diff(finalarr.time) == 0)[0]
    if duptimes.size > 0:
        newtimes = finalarr.time.values
        for dupt in duptimes:
            newtimes[dupt + 1] += 0.000001
        finalarr = finalarr.assign_coords(time=newtimes)
    return finalarr


def _assess_need_for_split_correction(cur_xarr: xr.Dataset, next_xarr: xr.Dataset):
    """
    Taking blocks from workers, if the block after the current one has a start time equal to the current end time,
    you have a ping that stretches across the block.  You can't end up with duplicate times, so flag this one as needing
    correction.

    Parameters
    ----------
    cur_xarr
        xarray Dataset, the current one in queue for assessment
    next_xarr
        xarray Dataset, the next one in time order

    Returns
    -------
    bool
        True if needing split correction
    """

    if next_xarr is not None:
        cur_rec = cur_xarr.isel(time=-1)
        try:
            next_rec = next_xarr.isel(time=0)
        except ValueError:
            # get here if you selected the first record prior to this method
            next_rec = next_xarr
        # if the first time on the next array equals the last one on the current one....you gotta fix the
        #    sector-pings
        if float(cur_rec.time) == float(next_rec.time):
            return True
        else:
            return False
    else:
        return False


def _drop_next_att_nav_blob(cur_xarr: xr.Dataset, next_xarr: xr.Dataset):
    """
    Find the duplicates in the next xarray chunk and drop them from the current xarray chunk.  Duplicates are where
    the current times are greater than the times in the next_xarr.  We only check the last 200 records, which should be
    greater than the size of any attitude/navigation blob out there.

    Parameters
    ----------
    cur_xarr
        xarray Dataset, the current one in queue for assessment
    next_xarr
        xarray Dataset, the next one in time order

    Returns
    -------
    xr.Dataset
        corrected version of cur_xarr
    """

    if next_xarr is not None:
        blob_size = min(200, min(cur_xarr.time.size, cur_xarr.time.size))  # size of the dataset to compare
        last_times_cur_xarr = cur_xarr.time.values[-blob_size:]
        first_times_next_xarr = next_xarr.time.values[:blob_size]
        keep_these = ~(last_times_cur_xarr >= first_times_next_xarr[0])
        bmask = np.ones_like(cur_xarr.time, dtype=bool)
        bmask[-blob_size:] = keep_these
        cur_xarr = cur_xarr.isel(time=bmask)
    return cur_xarr


def _correct_for_splits(cur_xarr: xr.Dataset, trim_the_array: bool):
    """
    If assess need for split correction finds that a chunk boundary has cut off a ping, remove that ping here.  The
    next chunk will have that full ping.

    Parameters
    ----------
    cur_xarr
        xarray Dataset, the current one in queue for assessment
    trim_the_array
        if True we remove the first time record (see _assess_need_for_split_correction)

    Returns
    -------
    xr.Dataset
        corrected version of cur_xarr
    """

    if trim_the_array:
        print('remove first: {} to {}'.format(len(cur_xarr.time), slice(1, len(cur_xarr.time))))
        cur_xarr = cur_xarr.isel(time=slice(1, len(cur_xarr.time)))
    return cur_xarr


def gather_dataset_attributes(dataset: Union[dict, list, xr.Dataset]):
    """
    Return the attributes within an Xarray DataSet

    Parameters
    ----------
    dataset
        dict of ping records by serial number or list of datasets or a single dataset

    Returns
    -------
    dict
        attributes within dataset
    """

    if isinstance(dataset, dict):
        serialnum = list(dataset.keys())[0]
        dset = dataset[serialnum]
    else:
        dset = dataset

    if isinstance(dset, list):
        attrs = dset[0].attrs
    else:
        attrs = dset.attrs
    return attrs


def _closest_prior_key_value(tstmps: list, key: float):
    """
    With given list of timestamps, return the one that is closest to the key but also prior to the key

    Parameters
    ----------
    tstmps
        list of floats of timestamps
    key
        UTC timestamp you want to use to search the tstmps

    Returns
    -------
    float
        timestamp that is closest and prior to key
    """

    try:
        sett_tims = np.array([float(x) for x in tstmps])
    except ValueError:
        print('Unable to generate list of floats from: {}'.format(tstmps))
        return None

    tim = float(key)
    difs = tim - sett_tims
    difs[difs < 0] = np.nan
    closest_tim = sett_tims[np.nanargmin(difs)]
    return closest_tim


def _closest_key_value(tstmps: list, key: float):
    """
    With given list of timestamps, return the one that is closest to the key

    Parameters
    ----------
    tstmps
        list of floats of timestamps
    key
        UTC timestamp you want to use to search the tstmps

    Returns
    -------
    float
        timestamp that is closest to the key
    """

    try:
        sett_tims = np.array([float(x) for x in tstmps])
    except ValueError:
        print('Unable to generate list of floats from: {}'.format(tstmps))
        return None

    tim = float(key)
    difs = abs(tim - sett_tims)
    closest_tim = sett_tims[np.nanargmin(difs)]
    return closest_tim


def batch_read_configure_options():
    """
    Generate the parameters that drive the data conversion.  Chunksize for size of zarr written chunks,
    combine_attributes as a bool to tell the system to look for attributes within that xarray object, and
    output_arrs/final_pths/final_attrs to hold the data as it is processed.

    Returns
    -------
    opts
        dict, options for batch read process
    """

    opts = {
        'ping': {'chunksize': kluster_variables.ping_chunk_size, 'chunks': kluster_variables.ping_chunks,
                 'combine_attributes': True, 'output_arrs': [], 'time_arrs': [], 'beam_shapes': [], 'final_pths': None, 'final_attrs': None},
        'attitude': {'chunksize': kluster_variables.attitude_chunk_size, 'chunks': kluster_variables.att_chunks,
                     'combine_attributes': False, 'output_arrs': [], 'time_arrs': [], 'beam_shapes': [], 'final_pths': None, 'final_attrs': None}
        }
    return opts


class BatchRead(ZarrBackend):
    """
    BatchRead - multibeam data converter using dask infrastructure and xarray data types
    Pass in multibeam files, call read(), and gain access to xarray Datasets for each data type

    NOTE: CURRENTLY ONLY ZARR BASED PROCESSING OF kluster_variables.supported_sonar FILES IS SUPPORTED

    | BatchRead is stored internally using the following conventions:
    | X = + Forward, Y = + Starboard, Z = + Down
    | roll = + Port Up, pitch = + Bow Up, gyro = + Clockwise

    | >> from xarray_conversion import BatchRead
    | >> converted = BatchRead(r'C:/data_dir/0009_20170523_181119_FA2806.all')
    | Started local cluster client...
    | <Client: 'tcp://127.0.0.1:62327' processes=4 threads=16, memory=34.27 GB>
    | >> converted.read()

    |   Running Kongsberg .all converter
    | C:/data_dir/0009_20170523_181119_FA2806.all: Using 20 chunks of size 1962957
    | Operating on sector 0, s/n 40111, freq 265000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Operating on sector 0, s/n 40111, freq 275000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Operating on sector 1, s/n 40111, freq 285000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Operating on sector 1, s/n 40111, freq 290000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Operating on sector 2, s/n 40111, freq 270000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Operating on sector 2, s/n 40111, freq 280000
    | Rebalancing 108 total ping records across 1 blocks of size 1000
    | Rebalancing 5302 total attitude records across 1 blocks of size 20000
    |   Distributed conversion complete: 5.3s****
    | Constructed offsets successfully
    | read successful

    | # examine the serial number/sector/frequency combinations
    | >> [cnv.sector_identifier for cnv in converted.raw_ping]

    | ['40111_0_265000',
    |  '40111_0_275000',
    |  '40111_1_285000',
    |  '40111_1_290000',
    |  '40111_2_270000',
    |  '40111_2_280000']

    | # display the first ping dataset (serial number 40111, sector 0, frequency 265khz)
    | >> converted.raw_ping[0]

    | <xarray.Dataset>
    | Dimensions:            (beam: 182, time: 108)
    | Coordinates:
    |   * beam               (beam) int32 0 1 2 3 4 5 6 ... 176 177 178 179 180 181
    |   * time               (time) float64 1.496e+09 1.496e+09 ... 1.496e+09
    | Data variables:
    |     beampointingangle  (time, beam) float32 dask.array<chunksize=(108, 182), meta=np.ndarray>
    |     counter            (time) uint16 dask.array<chunksize=(108,), meta=np.ndarray>
    |     detectioninfo      (time, beam) int32 dask.array<chunksize=(108, 182), meta=np.ndarray>
    |     mode               (time) <U2 dask.array<chunksize=(108,), meta=np.ndarray>
    |     modetwo            (time) <U4 dask.array<chunksize=(108,), meta=np.ndarray>
    |     ntx                (time) uint16 dask.array<chunksize=(108,), meta=np.ndarray>
    |     qualityfactor      (time, beam) int32 dask.array<chunksize=(108, 182), meta=np.ndarray>
    |     soundspeed         (time) float32 dask.array<chunksize=(108,), meta=np.ndarray>
    |     tiltangle          (time) float32 dask.array<chunksize=(108,), meta=np.ndarray>
    |     traveltime         (time, beam) float32 dask.array<chunksize=(108, 182), meta=np.ndarray>
    |     yawpitchstab       (time) <U2 dask.array<chunksize=(108,), meta=np.ndarray>
    | Attributes:
    |     _conversion_complete:            Tue Oct 20 15:53:34 2020
    |     installsettings_1495563079:      {"waterline_vertical_location": "-0.640"...
    |     multibeam_files:                 {'0009_20170523_181119_FA2806.all': [149...
    |     output_path:                     C:\\collab\\dasktest\\data_dir\\EM2040_small...
    |     profile_1495563079:              [[0.0, 1489.2000732421875], [0.32, 1489....
    |     reference:                       {'beampointingangle': 'receiver', 'tilta...
    |     runtimesettings_1495563080:      {"Counter": "61968", "SystemSerial#": "4...
    |     secondary_system_serial_number:  [0]
    |     sector_identifier:               40111_0_265000
    |     survey_number:                   ['01_Patchtest_2806']
    |     system_serial_number:            [40111]
    |     units:                           {'beampointingangle': 'degrees', 'tiltan...
    |     xyzrph:                          {'antenna_x': {'1495563079': '0.000'}, '...
    """

    def __init__(self, filfolder: Union[str, list] = None, dest: str = None, address: str = None, client: Client = None,
                 minchunksize: int = 40000000, max_chunks: int = 20, filtype: str = 'zarr', skip_dask: bool = False,
                 dashboard: bool = False, show_progress: bool = True, parallel_write: bool = True):
        """
        Parameters
        ----------
        filfolder
            Either a path to a multibeam file, a list of multibeam file paths or a path to a directory of multibeam
            files.  fqpr_convenience will set this to None in order to hotwire the data loading process without
            running conversion.
        dest
            provided path to where you want the data to be converted to, otherwise will convert to a new folder next to
            the provided data
        address
            None for setting up local cluster, IP:Port for remote dask server session
        client
            dask.distributed client if you don't want this class to autostart a LocalCluster
        minchunksize
            minimum size of chunks you want to split files in to
        max_chunks
            maximum chunks per file
        filtype
            chosen data storage object to use, ZARR IS BASICALLY THE ONLY ONE SUPPORTED at this point, I tried
            the netcdf option with mixed results
        skip_dask
            if False, will skip creating the dask client, useful if you just want to open the zarr data
            store without the overhead of dask
        dashboard
            if True, will open a web browser with the dask dashboard
        show_progress
            If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text
        parallel_write
            if True, will write in parallel to disk
        """

        super().__init__()
        self.filfolder = filfolder
        self.dest = dest
        self.filtype = filtype
        self.convert_minchunksize = minchunksize
        self.convert_maxchunks = max_chunks
        self.address = address
        self.show_progress = show_progress
        self.raw_ping = None
        self.raw_att = None

        self.parallel_write = parallel_write
        self.readsuccess = False

        self.client = client
        self.skip_dask = skip_dask
        if not skip_dask and self.client is None:
            self.client = dask_find_or_start_client(address=self.address)
        if dashboard:
            webbrowser.open_new('http://localhost:8787/status')

        # misc
        self.converted_pth = None
        self.final_paths = {}
        self.fils = None
        self.logfile = None
        self.logger = None

        # install parameters
        self.sonartype = None
        self.xyzrph = None

    @property
    def chunk_size(self):
        """
        Return the chunk size of the dataset for (time, beam)
        """
        if self.raw_ping is not None:
            return self.raw_ping[0].beampointingangle.data.chunksize
        else:
            return None

    def read(self, build_offsets: bool = True):
        """
        Run the batch_read method on all available lines, writes to datastore (netcdf/zarr depending on self.filtype),
        and loads the data back into the class as self.raw_ping, self.raw_att.

        If data loads correctly, builds out the self.xyzrph attribute and translates the runtime parameters to a usable
        form.

        Parameters
        ----------
        build_offsets
            if this is set, also build the xyzrph attribute, which is mandatory for processing later in Kluster.  Make
            it optional so that when processing chunks of files, we can just run it once at the end after read()
        """

        if self.filtype not in ['zarr']:  # NETCDF is currently not supported
            self.logger.error(self.filtype + ' is not a supported format.')
            raise NotImplementedError(self.filtype + ' is not a supported format.')

        final_pths = self.batch_read(self.filtype)

        if final_pths is not None:
            self.final_paths = final_pths
            if self.filtype == 'netcdf':
                self.read_from_netcdf_fils(final_pths['ping'], final_pths['attitude'])
                if build_offsets:
                    self.build_offsets()
            elif self.filtype == 'zarr':
                self.read_from_zarr_fils(final_pths['ping'], final_pths['attitude'][0], self.logfile)
                if build_offsets:
                    self.build_offsets(save_pths=final_pths['ping'])  # write offsets to ping rootgroup
                    self.build_additional_line_metadata(save_pths=final_pths['ping'])
        else:
            self.logger.error('Unable to start/connect to the Dask distributed cluster.')
            raise ValueError('Unable to start/connect to the Dask distributed cluster.')

        if self.raw_ping is not None:
            self.readsuccess = True
        else:
            self.logger.warning('read unsuccessful, data paths: {}\n'.format(final_pths))

    def read_from_netcdf_fils(self, ping_pths: list, attitude_pths: list):
        """
        Read from the generated netCDF files constructed with read()

        **Currently some issues with open_mfdataset that I've not resolved.  Using it with the dask distributed
        cluster active results in worker errors/hdf errors.  Using it without the distributed cluster works fine.  So
        annoying.  I'm sticking to the zarr stuff for now, distributed parallel read/writes appear to work there after
        I built my own writer.**

        Parameters
        ----------
        ping_pths
            paths to the ping netcdf files
        attitude_pths:
            path to the attitude netcdf files
        """

        # sort input pths by type, currently a list by idx
        self.raw_ping = xr.open_mfdataset(ping_pths, chunks={}, concat_dim='time', combine='nested')
        self.raw_att = xr.open_mfdataset(attitude_pths, chunks={}, concat_dim='time', combine='nested')

    def reload_pingrecords(self, skip_dask: bool = False):
        """
        After writing new data to the zarr data store, you need to refresh the xarray Dataset object so that it
        sees the changes.  We do that here by just re-running open_zarr.

        Parameters
        ----------
        skip_dask
            if True will skip the dask distributd client stuff when reloading
        """

        if 'ping' in self.final_paths:
            self.raw_ping = []
            for pth in self.final_paths['ping']:
                xarr = reload_zarr_records(pth, skip_dask=skip_dask)
                self.raw_ping.append(xarr)
        else:
            # self.logger.warning('Unable to reload ping records (normal for in memory processing), no paths found: {}'.format(self.final_paths))
            pass

    def reload_attituderecords(self, skip_dask: bool = False):
        if 'attitude' in self.final_paths:
            self.raw_att = reload_zarr_records(self.final_paths['attitude'][0], skip_dask=skip_dask)
        else:
            # self.logger.warning('Unable to reload ping records (normal for in memory processing), no paths found: {}'.format(self.final_paths))
            pass

    def return_raw_navigation(self, start_time: float = None, end_time: float = None):
        """
        Return just the navigation side of the first ping record.  If a start time and end time are provided, will
        subset to just those times.

        If this is a dual head sonar, it only returns the nav for the first head!

        Parameters
        ----------
        start_time
            if provided will allow you to only return navigation after this time.  Selects the nearest time value to
            the one provided.
        end_time
            if provided will allow you to only return navigation before this time.  Selects the nearest time value to
            the one provided.

        Returns
        -------
        xr.Dataset
            latitude/longitude/altitude pulled from the raw navigation part of the ping record
        """

        desired_vars = ['latitude', 'longitude', 'altitude']
        keep_these_attributes = ['max_lat', 'max_lon', 'min_lat', 'min_lon', 'reference', 'units', 'pos_files']
        if self.raw_ping[0]:
            drop_these = [dvar for dvar in list(self.raw_ping[0].keys()) if dvar not in desired_vars]
            subset_nav = self.raw_ping[0].drop_vars(drop_these)
            subset_nav.attrs = {ky: self.raw_ping[0].attrs[ky] for ky in keep_these_attributes if ky in self.raw_ping[0].attrs}
            rnav = slice_xarray_by_dim(subset_nav, 'time', start_time=start_time, end_time=end_time)
            return rnav
        return None

    def read_from_zarr_fils(self, ping_pth: list, attitude_pth: str, logfile_pth: str):
        """
        Read from the generated zarr datastores constructed with read()

        Parameters
        ----------
        ping_pth
            list of paths to each ping zarr group (by system)
        attitude_pth
            path to the attitude zarr group
        logfile_pth
            path to the text log file used by logging
        """

        self.raw_ping = None
        self.raw_att = None

        self.logfile = logfile_pth
        self.initialize_log()
        if self.client is None:
            skip_dask = True
        else:
            skip_dask = False

        if self.converted_pth is None:
            self.converted_pth = os.path.dirname(ping_pth[0])
            self.output_folder = self.converted_pth
        try:
            for pth in ping_pth:
                dset = reload_zarr_records(pth, skip_dask)
                # dset = sort_and_drop_duplicates(dset, pth)
                if self.raw_ping is None:
                    self.raw_ping = [dset]
                else:
                    self.raw_ping.append(dset)
        except ValueError:
            self.logger.error('Unable to read from {}'.format(ping_pth))
            return True

        if self.converted_pth is None:
            self.converted_pth = os.path.dirname(attitude_pth)
            self.output_folder = self.converted_pth
        try:
            self.raw_att = reload_zarr_records(attitude_pth, skip_dask)
            # self.raw_att = sort_and_drop_duplicates(self.raw_att, attitude_pth)
        except (ValueError, AttributeError):
            self.logger.error('Unable to read from {}'.format(attitude_pth))
            return True
        return False

    def initialize_log(self):
        """
        Initialize the logger, which writes to logfile, that is made at the root folder housing the converted data
        """

        if self.logfile is None:
            self.logfile = os.path.join(self.converted_pth, return_log_name())
        if self.logger is None:
            self.logger = return_logger(__name__, self.logfile)

    def _batch_read_file_setup(self):
        """
        With given path to folder of multibeam files (self.filfolder), return the paths to the individual multibeam
        files and the path to the output folder for the netcdf/zarr converted files.  Create this folder, or a similarly
        named folder (with appended timestamp) if it already exists.

        Also initialize the logging module (path to logfile is in self.logfile).
        """
        if type(self.filfolder) == list and len(self.filfolder) == 1:
            self.filfolder = self.filfolder[0]

        if type(self.filfolder) == list:
            fils = self.filfolder
            self.filfolder = os.path.dirname(fils[0])
        elif os.path.isdir(self.filfolder):
            fils = []
            for mext in kluster_variables.supported_sonar:
                fils = glob(os.path.join(self.filfolder, '*' + mext))
                if fils:
                    break
        elif os.path.isfile(self.filfolder):
            fils = [self.filfolder]
            self.filfolder = os.path.dirname(self.filfolder)
        else:
            raise ValueError('Only directory or file path is supported: {}'.format(self.filfolder))

        if not fils:
            raise ValueError('Directory provided, but no {} files found: {}'.format(kluster_variables.supported_sonar, self.filfolder))

        if self.dest is not None:  # path was provided, lets make sure it is an empty folder
            if not os.path.exists(self.dest):
                os.makedirs(self.dest)
            converted_pth = self.dest
        else:
            converted_pth = os.path.join(self.filfolder, 'converted')
            if os.path.exists(converted_pth):
                converted_pth = os.path.join(self.filfolder, 'converted_{}'.format(datetime.now().strftime('%H%M%S')))
            os.makedirs(converted_pth)
        self.converted_pth = converted_pth
        self.output_folder = self.converted_pth
        self.fils = fils

        self.logfile = os.path.join(converted_pth, return_log_name())
        self.initialize_log()

    def _batch_read_chunk_generation(self, fils: list):
        """
        For each multibeam file, determine a good chunksize for the distributed read/processing and build a list with
        files, start bytes and end bytes.

        Parameters
        ----------
        fils
            list of paths to multibeam files

        Returns
        -------
        list
            list of chunks given as [filepath, starting offset in bytes, end of chunk pointer in bytes]
        """

        chnks = []
        for f in fils:
            finalchunksize = determine_good_chunksize(f, self.convert_minchunksize, self.convert_maxchunks)
            chnks.append(return_chunked_fil(f, 0, finalchunksize))

        # chnks_flat is now a list of lists representing chunks of each file
        chnks_flat = [c for subc in chnks for c in subc]
        if self.skip_dask:
            self.logger.info('{} file(s), Using {} chunk(s)'.format(len(fils), len(chnks_flat)))
        else:
            self.logger.info('{} file(s), Using {} chunk(s) in parallel'.format(len(fils), len(chnks_flat)))
        for fil in fils:
            self.logger.info(fil)

        return chnks_flat

    def _gather_file_level_metadata(self, fils: list):
        """
        Most of xarray_conversion works on chunks of files.  Here we gather all the necessary information that is
        important at a file level, for instance start/end time.  This will be added later as an attribute to the
        final xarray Dataset.

        Parameters
        ----------
        fils
            strings for full file paths to multibeam files

        Returns
        -------
        dict
            dictionary of file name and start/stop time, e.g. {'test.all': [1562785404.206, 1562786165.287]}
        """

        dat = {}
        for f in fils:
            filname = os.path.split(f)[1]
            mtype, start_end_times, _ = fast_read_multibeam_metadata(f, gather_times=True, gather_serialnumber=False)
            dat[filname] = start_end_times
        return dat

    def _batch_read_validate_blocks(self, input_xarrs: list):
        if self.client is not None:
            time_bounds = self.client.gather(self.client.map(_return_xarray_time_bounds, input_xarrs))
        else:
            time_bounds = [_return_xarray_time_bounds(oa) for oa in input_xarrs]

        for cnt, t in enumerate(time_bounds):
            if cnt == 0:
                continue
            if time_bounds[cnt - 1][1] >= t[0]:
                self.print(f'Validation found overlap issue with block {cnt}, correcting for overlap between this block time bounds {t} and previous block time bounds {time_bounds[cnt - 1]}', logging.WARNING)
                if self.client is not None:
                    input_xarrs[cnt] = self.client.submit(_trim_to_time_bounds, input_xarrs[cnt], (time_bounds[cnt - 1][1], t[1]))
                else:
                    input_xarrs[cnt] = _trim_to_time_bounds(input_xarrs[cnt], (time_bounds[cnt - 1][1], t[1]))
        return input_xarrs

    def _batch_read_merge_blocks(self, input_xarrs: list, datatype: str, chunksize: int):
        """
        Take the blocks workers have been working on up to this point (from reading raw files) and reorganize them
        into equal blocks that are of a size that makes sense later down the line.  Larger blocks for processing than
        the smaller ones used during file access.

        Parameters
        ----------
        input_xarrs
            xarray objects representing data read from raw files
        datatype
            one of 'ping', 'attitude', 'navigation'
        chunksize
            size of new chunks, see batch_read_configure_options

        Returns
        -------
        list
            futures representing data merged according to balanced_data
        list
            xarray DataArrays for the time dimension of each returned future
        list
            list of the max beams in each chunk, if this is a datatype=ping.  Otherwise None.
        int
            size of chunks operated on by workers, shortened if greater than the total size
        int
            total length of blocks
        """
        if self.client is not None:
            xlens = self.client.gather(self.client.map(_return_xarray_timelength, input_xarrs))
        else:
            xlens = [_return_xarray_timelength(ix) for ix in input_xarrs]
        balanced_data, totallength = _return_xarray_constant_blocks(xlens, input_xarrs, chunksize)
        self.logger.info('Rebalancing {} total {} records across {} blocks of size {}'.format(totallength, datatype,
                                                                                              len(balanced_data), chunksize))

        # if the chunksize is greater than the total amount of records, adjust to the total amount of records.
        #   This is to prevent empty values being written to the zarr datastore
        if (len(balanced_data) == 1) and (totallength < chunksize):
            # self.logger.info('Less values found than chunk size, resizing final chunksize from {} to {}'.format(chunksize,
            #                                                                                                 totallength))
            chunksize = totallength

        # merge old arrays to get new ones of chunksize
        if self.client is not None:
            output_arrs = self.client.map(_merge_constant_blocks, balanced_data)
            output_arrs = self._batch_read_validate_blocks(output_arrs)
            time_arrs = self.client.gather(self.client.map(_return_xarray_time, output_arrs))
            if datatype == 'ping':
                beam_shapes = self.client.gather(self.client.map(_return_xarray_beam, output_arrs))
            else:
                beam_shapes = None
        else:
            output_arrs = [_merge_constant_blocks(bd) for bd in balanced_data]
            output_arrs = self._batch_read_validate_blocks(output_arrs)
            time_arrs = [_return_xarray_time(oa) for oa in output_arrs]
            if datatype == 'ping':
                beam_shapes = [_return_xarray_beam(oa) for oa in output_arrs]
            else:
                beam_shapes = None
        del balanced_data
        return output_arrs, time_arrs, beam_shapes, chunksize, totallength

    def _batch_read_sequential(self, chnks_flat: list):
        """
        Run sequential_read methods on the provided chunks

        Parameters
        ----------
        chnks_flat
            output from _batch_read_chunk_generation describing the byte offset and length of chunks

        Returns
        -------
        list
            dict for all records/datagrams in multibeam file
        """

        # recfutures is a list of futures representing dicts from sequential read
        if self.client is not None:
            recfutures = self.client.map(_run_sequential_read, chnks_flat)
            if self.show_progress:
                progress(recfutures, multi=False)
            notempty = self.client.gather(self.client.map(_is_not_empty_sequential, recfutures))
        else:
            recfutures = [_run_sequential_read(cf) for cf in chnks_flat]
            notempty = [_is_not_empty_sequential(rcf) for rcf in recfutures]
        drop_futures = []
        for cnt, isnotempty in enumerate(notempty):
            if not isnotempty:  # this is an empty chunk, no ping records
                print('No ping or attitude records found in {}, startbyte:{}, endbyte:{}'.format(chnks_flat[cnt][0], chnks_flat[cnt][1], chnks_flat[cnt][2]))
                drop_futures.append(recfutures[cnt])
        for dpf in drop_futures:
            recfutures.remove(dpf)
        return recfutures

    def _batch_read_sort_futures_by_time(self, input_xarrs: list):
        """
        Futures should retain input order (order passed to mapped function), but I've found that sorting by time will
        sometimes catch instances where futures are not sorted.

        Parameters
        ----------
        input_xarrs
            xarray Dataset object from _sequential_to_xarray

        Returns
        -------
        list
            xarray Dataset futures object from _sequential_to_xarray, sorted by time
        """

        # sort futures before doing anything else
        if self.client is not None:
            mintims = self.client.gather(self.client.map(_return_xarray_mintime, input_xarrs))
        else:
            mintims = [_return_xarray_mintime(ix) for ix in input_xarrs]
        sort_mintims = sorted(mintims)
        if mintims != sort_mintims:
            idx = [mintims.index(t) for t in sort_mintims]
            input_xarrs = [input_xarrs[i] for i in idx]
        return input_xarrs

    def _batch_read_write(self, output_mode: str, datatype: str, opts: list, converted_pth: str, sysid: str = None):
        """
        Write out the xarray Dataset(s) to the specified data storage type

        Parameters
        ----------
        output_mode
            identifies the type of data storage format
        datatype
            one of ping, attitude, navigation
        opts
            output of batch_read_configure_options, contains output arrays and options for writing
        converted_pth
            path to the output datastore
        sysid
            system name if writing by sector to zarr datastore

        Returns
        -------
        str
            path to the written data directory
        """

        fpths = ''
        if output_mode == 'netcdf':
            fpths = self._batch_read_write_netcdf(datatype, opts, converted_pth)
        elif output_mode == 'zarr':
            fpths = self._batch_read_write_zarr(datatype, opts, converted_pth, sysid=sysid)

        if fpths:
            if self.client is not None:
                fpthsout = self.client.gather(fpths)
            else:
                fpthsout = fpths
            if output_mode == 'zarr':
                # pass None here to auto trim NaN time
                # resize_zarr(fpthsout, totallen)
                resize_zarr(fpthsout, None)
            del fpths
            return fpthsout
        else:
            return ''

    def _batch_read_write_netcdf(self, datatype: str, opts: dict, converted_pth: str):
        """
        Take in list of xarray futures (output_arrs) and write them to netcdf files.  You'll get one .nc file per
        chunk which serves as a handy block size when using xarray open_mfdataset later (chunks will be one per nc file)

        Parameters
        ----------
        datatype
            one of 'ping', 'attitude', 'navigation'
        opts
            nested dictionary containing settings and input/output arrays depending on datatype, see self.batch_read
        converted_pth
            path to the directory that will contain the written netcdf files

        Returns
        -------
        list
            paths to all written netcdf files
        """

        output_pths = [converted_pth] * len(opts[datatype]['output_arrs'])
        output_fnames = [datatype + '.nc'] * len(opts[datatype]['output_arrs'])
        output_attributes = [opts[datatype]['final_attrs']] * len(opts[datatype]['output_arrs'])
        fname_idxs = [i for i in range(len(opts[datatype]['output_arrs']))]
        if self.client is not None:
            fpths = self.client.map(xarr_to_netcdf, opts[datatype]['output_arrs'], output_pths, output_fnames,
                                    output_attributes, fname_idxs)
        else:
            fpths = [xarr_to_netcdf(oa, op, of, oatt, fi) for oa, op, of, oatt, fi in zip(opts[datatype]['output_arrs'], output_pths, output_fnames,
                                                                                          output_attributes, fname_idxs)]
        return fpths

    def _batch_read_write_zarr(self, datatype: str, opts: dict, converted_pth: str, sysid: str = None):
        """
        Take in list of xarray futures (output_arrs) and write them to a single Zarr datastore.  Each array will become
        a Zarr array within the root Zarr group.  Zarr chunksize on read is determined by the chunks written, so the
        structure here is handy for generating identical, evenly spaced chunks (required by Zarr)

        Parameters
        ----------
        datatype
            one of 'ping', 'attitude', 'navigation'
        opts
            nested dictionary containing settings and input/output arrays depending on datatype, see self.batch_read
        converted_pth
            path to the directory that will contain the written netcdf files
        sysid
            system identifier

        Returns
        -------
        Future
            str path to written zarr datastore/group.  I use the first element of the list of fpths as all returned elements of fpths are identical.
        """

        if sysid is None:
            output_pth = os.path.join(converted_pth, datatype + '.zarr')
        else:
            output_pth = os.path.join(converted_pth, datatype + '_' + sysid + '.zarr')
            opts[datatype]['final_attrs']['system_identifier'] = sysid

        # correct for existing data if it exists in the zarr data store
        if datatype == 'ping':
            max_beam_size = max(opts[datatype]['beam_shapes'])
        else:
            max_beam_size = None
        _, fpths = self.write(datatype, opts[datatype]['output_arrs'], time_array=opts[datatype]['time_arrs'],
                              attributes=opts[datatype]['final_attrs'], skip_dask=self.skip_dask, sys_id=sysid,
                              max_beam_size=max_beam_size)
        fpth = fpths[0]  # Pick the first element, all are identical so it doesnt really matter
        return fpth

    def _batch_read_correct_block_boundaries(self, input_xarrs: list):
        """
        See _correct_for_splits.  Handle cases where sectors are split across worker blocks/files and
        must be repaired in order to avoid duplicate timestamps.

        Parameters
        ----------
        input_xarrs
            xarray Dataset object from _sequential_to_xarray

        Returns
        -------
        list
            xarray Dataset futures representing the input_xarrs corrected for splits between files/blocks
        """

        base_xarrfut = input_xarrs
        next_xarrfut = input_xarrs[1:] + [None]
        if self.client is not None:
            trim_arr = self.client.map(_assess_need_for_split_correction, base_xarrfut, next_xarrfut)
            input_xarrs = self.client.map(_correct_for_splits, base_xarrfut, [trim_arr[-1]] + trim_arr[:-1])
        else:
            trim_arr = [_assess_need_for_split_correction(bx, nx) for bx, nx in zip(base_xarrfut, next_xarrfut)]
            input_xarrs = [_correct_for_splits(bx, sta) for bx, sta in zip(base_xarrfut, [trim_arr[-1]] + trim_arr[:-1])]
        del trim_arr, base_xarrfut, next_xarrfut
        return input_xarrs

    def _batch_read_drop_duplicate_blobs(self, input_xarrs: list):
        """
        With the par3 driver, keep finding duplicate attitude/navigation blobs across files and/or chunks of files.
        Since all chunks are operating independently in parallel, we have to look at the neighboring chunk and drop
        all duplicates (in time) in the current chunk that are in the neighboring chunk.  Should mean that the
        returned array list contains no duplicate times.

        Parameters
        ----------
        input_xarrs
            xarray Dataset object from _sequential_to_xarray

        Returns
        -------
        list
            xarray Dataset futures representing the input_xarrs corrected for splits between files/blocks
        """

        base_xarrfut = input_xarrs
        next_xarrfut = input_xarrs[1:] + [None]
        if self.client is not None:
            input_xarrs = self.client.map(_drop_next_att_nav_blob, base_xarrfut, next_xarrfut)
        else:
            input_xarrs = [_drop_next_att_nav_blob(bx, nx) for bx, nx in zip(base_xarrfut, next_xarrfut)]
        del base_xarrfut, next_xarrfut
        return input_xarrs

    def _batch_read_ping_specific_attribution(self, combattrs: dict):
        """
        Add in the ping record specific attribution

        Parameters
        ----------
        combattrs
            dictionary of basic attribution we want to add to

        Returns
        -------
        dict
            new dict with ping specific attribution included
        """

        fil_start_end_times = self._gather_file_level_metadata(self.fils)
        combattrs['multibeam_files'] = fil_start_end_times  # override with start/end time dict
        combattrs['output_path'] = self.converted_pth
        if 'skip_to_georeferencing' in combattrs:
            combattrs.pop('skip_to_georeferencing')
            combattrs['base_processing_status'] = 3
            combattrs['current_processing_status'] = 3
        elif os.path.splitext(self.fils[0])[1] in kluster_variables.supported_singlebeam:
            combattrs['base_processing_status'] = 2
            combattrs['current_processing_status'] = 2
        else:
            combattrs['base_processing_status'] = 0
            combattrs['current_processing_status'] = 0
        combattrs['kluster_version'] = klustervers
        combattrs['_conversion_complete'] = datetime.utcnow().strftime('%c')
        combattrs['status_lookup'] = kluster_variables.status_lookup
        return combattrs

    def batch_read(self, output_mode: str = 'zarr'):
        """
        General converter for multibeam files leveraging xarray and dask.distributed
        See batch_read, same process but working on memory efficiency

        Parameters
        ----------
        output_mode
            'zarr' or 'netcdf', zarr is the only currently supported mode, alters the output datastore

        Returns
        -------
        dict
            nested dictionary for each type (ping, attitude, navigation) with path to written data and metadata
        """

        starttime = perf_counter()

        if output_mode not in ['zarr', 'netcdf']:
            msg = 'Only zarr and netcdf modes are supported at this time: {}'.format(output_mode)
            raise NotImplementedError(msg)

        if not self.skip_dask and self.client is None:
            self.client = dask_find_or_start_client()
            if self.client is None:
                return None

        self._batch_read_file_setup()
        self.logger.info('****Running multibeam converter****')

        chnks_flat = self._batch_read_chunk_generation(self.fils)
        newrecfutures = self._batch_read_sequential(chnks_flat)

        # xarrfutures is a list of futures representing xarray structures for each file chunk
        if self.client is not None:
            xarrfutures = self.client.map(_sequential_to_xarray, newrecfutures)
            if self.show_progress:
                progress(xarrfutures, multi=False)
        else:
            xarrfutures = [_sequential_to_xarray(nrf) for nrf in newrecfutures]
        del newrecfutures

        finalpths = {'ping': [], 'attitude': []}
        for datatype in ['ping', 'attitude']:
            if self.client is not None:
                input_xarrs = self.client.map(_divide_xarray_futs, xarrfutures, [datatype] * len(xarrfutures))
            else:
                input_xarrs = [_divide_xarray_futs(xf, dxf) for xf, dxf in zip(xarrfutures, [datatype] * len(xarrfutures))]
            input_xarrs = self._batch_read_sort_futures_by_time(input_xarrs)
            if self.client is not None:
                finalattrs = self.client.gather(self.client.map(gather_dataset_attributes, input_xarrs))
            else:
                finalattrs = [gather_dataset_attributes(ix) for ix in input_xarrs]
            combattrs = combine_xr_attributes(finalattrs)

            if datatype == 'ping':
                combattrs = self._batch_read_ping_specific_attribution(combattrs)
                if self.client is not None:
                    system_ids = self.client.gather(self.client.map(_return_xarray_system_ids, input_xarrs))
                else:
                    system_ids = [_return_xarray_system_ids(ix) for ix in input_xarrs]
                totalsystems = sorted(np.unique([s for system in system_ids for s in system]))
                for system in totalsystems:
                    self.logger.info('Operating on system identifier {}'.format(system))
                    input_xarrs_by_system = self._batch_read_return_xarray_by_system(input_xarrs, system)
                    opts = batch_read_configure_options()
                    opts['ping']['final_attrs'] = combattrs
                    if len(input_xarrs_by_system) > 1:
                        # rebalance to get equal chunksize in time dimension (sector/beams are constant across)
                        input_xarrs_by_system = self._batch_read_correct_block_boundaries(input_xarrs_by_system)
                    opts[datatype]['output_arrs'], opts[datatype]['time_arrs'], opts[datatype]['beam_shapes'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs_by_system, datatype, opts[datatype]['chunksize'])
                    del input_xarrs_by_system
                    finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, self.converted_pth, sysid=system))
                    del opts
            else:
                opts = batch_read_configure_options()
                opts[datatype]['final_attrs'] = combattrs
                if len(input_xarrs) > 1:
                    input_xarrs = self._batch_read_drop_duplicate_blobs(input_xarrs)
                opts[datatype]['output_arrs'], opts[datatype]['time_arrs'], opts[datatype]['beam_shapes'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs, datatype, opts[datatype]['chunksize'])
                del input_xarrs
                finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, self.converted_pth))
                del opts

        endtime = perf_counter()
        self.logger.info('****Distributed conversion complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

        return finalpths

    def return_runtime_and_installation_settings_dicts(self):
        """
        installation and runtime parameters are saved as string (json.dumps) as attributes in each raw_ping
        dataset.  Use this method to return the dicts that encompass each installation and runtime entry.
        """
        settdict = {}
        setts = [x for x in self.raw_ping[0].attrs if x[0:7] == 'install']
        for sett in setts:
            settdict[sett.split('_')[1]] = json.loads(self.raw_ping[0].attrs[sett])
        runtimesettdict = {}
        runtimesetts = [x for x in self.raw_ping[0].attrs if x[0:7] == 'runtime']
        for sett in runtimesetts:
            runtimesettdict[sett.split('_')[1]] = json.loads(self.raw_ping[0].attrs[sett])
        return settdict, runtimesettdict

    def _batch_read_return_xarray_by_system(self, input_xarrs: list, sysid: str):
        """
        Take in the system identifier sysid and only return xarray objects with that sector in them

        Parameters
        ----------
        input_xarrs
            xarray Dataset objects from _sequential_to_xarray
        sysid
            system identifier

        Returns
        -------
        list
            input_xarrs selected sector, with xarrs dropped if they didn't contain the sector
        """

        if self.client is not None:
            input_xarrs_by_sec = self.client.map(_divide_xarray_return_system, input_xarrs, [sysid] * len(input_xarrs))
            empty_mask = self.client.gather(self.client.map(_divide_xarray_indicate_empty_future, input_xarrs_by_sec))
        else:
            input_xarrs_by_sec = [_divide_xarray_return_system(ix, sysix) for ix, sysix in zip(input_xarrs, [sysid] * len(input_xarrs))]
            empty_mask = [_divide_xarray_indicate_empty_future(ix) for ix in input_xarrs_by_sec]
        valid_input_xarrs = [in_xarr for cnt, in_xarr in enumerate(input_xarrs_by_sec) if empty_mask[cnt]]
        return valid_input_xarrs

    def build_offsets(self, save_pths: str = None):
        """
        Form sorteddict for unique entries in installation parameters across all lines, retaining the xyzrph for each
        transducer/receiver.  key values depend on type of sonar, see sonar_translator

        Modifies the xyzrph attribute with timestamp dictionary of entries

        Parameters
        ----------
        save_pths
            a list of paths to zarr datastores for writing the xyzrph attribute to if provided
        """

        settdict, runtimesettdict = self.return_runtime_and_installation_settings_dicts()

        # self.logger.info('Found {} total Installation Parameters entr(y)s'.format(len(settdict)))
        if len(settdict) > 0:
            self.xyzrph = {}
            snrmodels = np.unique([settdict[x]['sonar_model_number'] for x in settdict])
            if len(snrmodels) > 1:
                self.logger.error('Found multiple sonars types in data provided: {}'.format(snrmodels))
                raise NotImplementedError('Found multiple sonars types in data provided: {}'.format(snrmodels))
            snrmodels = [snr.lower() for snr in snrmodels]
            if snrmodels[0] not in sonar_translator:
                self.logger.error('Sonar model not understood "{}"'.format(snrmodels[0]))
                raise NotImplementedError('Sonar model not understood "{}"'.format(snrmodels[0]))
            active_system = np.unique([settdict[x]['active_position_system_number'] for x in settdict])
            if len(active_system) > 1:
                self.logger.error('Found the active positioning system changed across files, found: {}'.format(active_system))
                raise NotImplementedError('Found the active positioning system changed across files, found: {}'.format(active_system))
            input_datum = np.unique([settdict[x]['position_1_datum'] for x in settdict])
            if len(input_datum) > 1:
                self.logger.error('Found the input position datum changed across files, found: {}'.format(input_datum))
                raise NotImplementedError('Found the input position datum changed across files, found: {}'.format(input_datum))

            mintime = min(list(settdict.keys()))
            minactual = np.min([rp.time.values[0] for rp in self.raw_ping])
            if float(mintime) > float(minactual):
                self.logger.warning('Installation Parameters minimum time: {}'.format(mintime))
                self.logger.warning('Actual data minimum time: {}'.format(float(minactual)))
                self.logger.warning('First Installation Parameters does not cover the start of the dataset.' +
                                    '  Extending from nearest entry...')
                settdict[str(int(minactual))] = settdict.pop(mintime)

            # translate over the offsets/angles for the transducers following the sonar_translator scheme
            self.sonartype = snrmodels[0]
            self.xyzrph = build_xyzrph(settdict, runtimesettdict, self.sonartype, logger=self.logger)

            if save_pths is not None:
                for svpth in save_pths:  # write the new attributes to disk
                    my_xarr_add_attribute({'input_datum': input_datum[0], 'xyzrph': self.xyzrph,
                                           'sonartype': self.sonartype}, svpth)
            for rp in self.raw_ping:  # set the currently loaded dataset attribution as well
                rp.attrs['input_datum'] = input_datum[0]
                rp.attrs['xyzrph'] = self.xyzrph
                rp.attrs['sonartype'] = self.sonartype
            self.logger.info('Constructed offsets successfully')

    def get_nearest_runtime_parameters(self, query_time: float):
        """
        Return the runtime parameters dict object that is nearest in time to query_time

        Parameters
        ----------
        query_time
            time in UTC seconds that you need the nearest runtime parameters to

        Returns
        -------
        dict
            runtime parameters dict object that is nearest in time to the query time
        """

        settdict, runtimesettdict = self.return_runtime_and_installation_settings_dicts()
        return get_nearest_runtime(str(query_time), runtimesettdict)

    def get_nearest_install_parameters(self, query_time: float):
        """
        Return the install parameters dict object that is nearest in time to query_time

        Parameters
        ----------
        query_time
            time in UTC seconds that you need the nearest install parameters to

        Returns
        -------
        dict
            install parameters dict object that is nearest in time to the query time
        """

        settdict, runtimesettdict = self.return_runtime_and_installation_settings_dicts()
        return get_nearest_runtime(str(query_time), settdict)

    def build_additional_line_metadata(self, save_pths: str = None):
        """
        After conversion, we run this additional step to build the line specific values to store as metadata.  The end result
        is a 'multibeam_files' attribute that stores [mintime, maxtime, start_latitude, start_longitude, end_latitude,
        end_longitude, azimuth, distance]

        Parameters
        ----------
        save_pths
            a list of paths to zarr datastores for writing the multibeam_files attribute to if provided
        """

        self.logger.info('Building additional line metadata...')
        rp = self.raw_ping[0]  # first head gets the same values basically
        line_dict = rp.attrs['multibeam_files']
        for line_name, line_times in line_dict.items():
            if len(line_times) == 2:  # this line needs the additional line metadata, otherwise it must have been computed already
                line_time_start, line_time_end = line_times[0], line_times[1]
                try:
                    current_index = None
                    try:
                        dstart = rp.interp(time=np.array([max(line_time_start, rp.time.values[0])]), method='nearest', assume_sorted=True)
                    except:
                        rp = fix_xarray_dataset_index(rp, 'time')
                        dstart = rp.interp(time=np.array([max(line_time_start, rp.time.values[0])]), method='nearest', assume_sorted=True)
                    start_position = [dstart.latitude.values, dstart.longitude.values]
                    count = 0
                    while np.isnan(start_position).any():  # first position is NaN, scroll through to find a good one
                        if not current_index:
                            current_index = np.argmin(np.abs(rp.time.values - dstart.time.values))
                        current_index += 1
                        dstart = rp.isel(time=current_index)
                        start_position = [dstart.latitude.values, dstart.longitude.values]
                        count += 1
                        if count == 100:
                            raise ValueError(f'Found bad position in the first 100 pings for line {line_name}!')
                except ValueError:
                    self.print(f'Unable to determine start position for line {line_name}', logging.ERROR)
                    continue
                try:
                    current_index = None
                    dend = rp.interp(time=np.array([min(line_time_end, rp.time.values[-1])]), method='nearest', assume_sorted=True)
                    end_position = [dend.latitude.values, dend.longitude.values]
                    count = 0
                    while np.isnan(end_position).any():  # first position is NaN, scroll through to find a good one
                        if not current_index:
                            current_index = np.argmin(np.abs(rp.time.values - dend.time.values))
                        current_index -= 1
                        dend = rp.isel(time=current_index)
                        end_position = [dend.latitude.values, dend.longitude.values]
                        count += 1
                        if count == 100:
                            raise ValueError(f'Found bad position in the last 100 pings for line {line_name}!')
                except ValueError:
                    self.print(f'Unable to determine end position for line {line_name}', logging.ERROR)
                    continue
                line_az = self.raw_att.interp(time=np.array([line_time_start + (line_time_end - line_time_start) / 2]), method='nearest', assume_sorted=True).heading.values
                try:
                    line_nav = rp.sel(time=slice(float(dstart.time.values), float(dend.time.values)))
                except:
                    rp = fix_xarray_dataset_index(rp, 'time')
                    line_nav = rp.sel(time=slice(float(dstart.time.values), float(dend.time.values)))

                if line_nav.time.size > 15 * 10:
                    samp_idx = np.arange(0, line_nav.time.size, 15).tolist()  # downsample to every 15 pings
                    if samp_idx[-1] != line_nav.time.size - 1:  # ensuring you keep the last ping
                        samp_idx += [line_nav.time.size - 1]
                    line_nav.isel(time=samp_idx)
                line_dist = np.nansum(distance_between_coordinates(line_nav.latitude[:-1], line_nav.longitude[:-1],
                                                                   line_nav.latitude[1:], line_nav.longitude[1:]))
                line_dict[line_name] += [float(start_position[0]), float(start_position[1]), float(end_position[0]),
                                         float(end_position[1]), round(float(line_az), 3), round(float(line_dist))]

        self.logger.info('Metadata build complete')
        if save_pths is not None:
            for svpth in save_pths:  # write the new attributes to disk
                my_xarr_add_attribute({'multibeam_files': line_dict}, svpth)
        for rp in self.raw_ping:  # set the currently loaded dataset attribution as well
            rp.attrs['multibeam_files'] = line_dict

    def return_tpu_parameters(self, timestamp: str):
        """
        Pull out the tpu parameters from the xyzrph installation parameters.  We need these parameters to compute tpu.
        Only pulls the values for a single timestamped entry, using the provided timestamp.

        Parameters
        ----------
        timestamp
            utc time in seconds for the entry

        Returns
        -------
        dict
            dict of tpu parameters for the timestamped entry
        """

        if self.xyzrph is None:
            raise ValueError('You must run build_offsets first, no installation parameters found.')
        kys = kluster_variables.tpu_parameter_names
        tpu_params = {}
        for ky in kys:
            if ky in self.xyzrph.keys():
                tpu_params[ky] = self.xyzrph[ky][timestamp]
        return tpu_params

    def _get_nth_chunk_indices(self, chunks: tuple, idx: int):
        """
        Take the output of Xarray DataArray chunks and produce start/end indices for chunk idx

        Parameters
        ----------
        chunks
            each element is the length of the chunk ex: ((5000, 5000, 323),)
        idx
            the index of the chunk you want the indices for

        Returns
        -------
        int
            index for start of chunk
        int
            index for end of chunk
        """

        if len(chunks) > 1:
            self.logger.error('Only supporting 1d chunks currently')
            raise NotImplementedError('Only supporting 1d chunks currently')
        chunks = chunks[0]
        start_idx = np.sum([i for i in chunks if chunks.index(i) < idx])
        end_idx = chunks[idx] + start_idx
        return start_idx, end_idx

    def _interp_nan_nearest(self, arr: xr.DataArray):
        """
        Fill nan values according to nearest.  Helper function to make this happen given apparent limitations in
        existing xarray/scipy methods.  See below

        Parameters
        ----------
        arr
            Xarray DataArray object, should contain nan values to interpolate

        Returns
        -------
        xr.DataArray
            object with nan values interpolated according to nearest val
        """

        arr = arr.where(arr != 0)
        nanvals = np.isnan(arr)
        uniquevals = np.unique(arr[~nanvals])
        if np.any(nanvals):
            if len(uniquevals) == 1:
                self.logger.info('_interp_nan_nearest: ({}) Applying simple fillna interp for array with one unique entry'.format(arr.name))
                arr = arr.fillna(uniquevals[0])
            else:
                self.logger.info('_interp_nan_nearest: ({}) Propagating previous value forward for array with multiple unique entries'.format(arr.name))
                arr = arr.ffill('time')
                new_nanvals = np.isnan(arr)
                if np.any(new_nanvals):
                    # if you get here, you must have an entire chunk without a non-nan entry.  Interp stuff won't work
                    #   unless theres at least one good entry in the chunk.  Use last value from previous chunk
                    self.logger.info('_interp_nan_nearest: ({}) Found isolated chunks without values...'.format(arr.name))
                    for ct, chunk in enumerate(arr.chunks):
                        start_idx, end_idx = self._get_nth_chunk_indices(arr.chunks, ct)
                        if np.any(new_nanvals[start_idx:end_idx]):
                            if ct == 0:
                                self.logger.info('_interp_nan_nearest: ({}) Filling chunk 0 with next values'.format(arr.name))
                                goodvalsearch = np.argwhere(nanvals[start_idx:end_idx + 1] is False)[-1][0]
                            else:
                                self.logger.info(
                                    '_interp_nan_nearest: ({}) Filling chunk {}} with previous values'.format(arr.name,
                                                                                                              ct))
                                goodvalsearch = np.argwhere(nanvals[start_idx - 1:end_idx] is False)[-1][0]
                            lastgoodval = arr[goodvalsearch]
                            arr[start_idx:end_idx] = arr[start_idx:end_idx].fillna(lastgoodval)
        return arr

    def correct_for_counter_reset(self):
        """
        Ping counter (at least with the Kongsberg systems) is a 16 bit unsigned integer that will just reset once it
        reaches 65536.  This zero crossing can happen multiple times in a kluster dataset, as it comprises multiple
        survey lines.  We need to handle this by reading it as a larger datatype (int64) and add the int16 limit
        whenever it is reached in the counter record.  This should transform a sawtooth record into a smooth,
        unique array of values.

        fqpr_generation.reform_2d_vars_across_sectors_at_time, reform_1d_vars_across_sectors_at_time will use this
        method automatically.  Having duplicate ping counters will mess up the logic we use to reform pings from
        these sector based datasets.
        """

        # first pass to find the times where we have zero crossings
        unique_zero_crossings = []
        zero_crossings_by_rawping = []
        for cnt, rp in enumerate(self.raw_ping):
            if rp.counter.dtype == np.uint16:  # expects 64bit signed to accomodate zero crossing
                rp['counter'] = rp.counter.astype(np.int64)
            zero_crossing = np.where(np.diff(rp['counter']) < 0)[0] + 1  # find the negative spike (65535 to 0) of reset
            zero_crossings_by_rawping.append(zero_crossing)  # store the zerocrossings
            for cz in zero_crossing:
                newcz = True
                cztime = float(rp['time'][cz])  # time of zero crossing
                # filter out duplicates, ping records can see the crossing at slightly different times
                for existcz in unique_zero_crossings:
                    if np.abs(existcz - cztime) < 2:
                        newcz = False
                if newcz:
                    unique_zero_crossings.append(cztime)

        # here we correct the arrays that have zero crossings and we bump up those that have times after one or more
        #  of our unique zero crossings.  This way all ping records get adjusted the same
        if unique_zero_crossings:
            self.logger.info('Found {} ping counter resets, correcting...'.format(len(unique_zero_crossings)))
            unique_zero_crossings.sort()
            for cnt, rp in enumerate(self.raw_ping):
                rp['counter'].load()  # have to load to affect the array in place
                zero_crossing = zero_crossings_by_rawping[cnt]
                needs_applying = unique_zero_crossings.copy()
                for zc in zero_crossing:
                    cztime = float(rp['time'][zc])
                    for uzc in unique_zero_crossings:
                        if np.abs(cztime - uzc) < 2 and uzc in needs_applying:
                            needs_applying.remove(uzc)
                    rp['counter'][rp.time >= cztime] += 65536
                if needs_applying:
                    for zc in needs_applying:
                        rp['counter'][rp.time >= zc] += 65536
            self.logger.info('Correction complete.')

    def is_dual_head(self):
        """
        Use the xyzrph keys to determine if sonar is dual head.  Port/Starboard identifiers will exist if dual.
        Kongsberg writes both heads to one file, only identifiable by serial number (each head will have a different
        serial number)

        Returns
        -------
        bool
            True if dual head, False if not
        """

        if self.xyzrph is None:
            self.build_offsets()
        if ('tx_port_x' in list(self.xyzrph.keys())) or ('rx_port_x' in list(self.xyzrph.keys())):
            return True
        else:
            return False

    def return_xyzrph_sorted_timestamps(self, ky: str):
        """
        Takes in key name and outputs a list of sorted timestamps that are valid for that key.

        Parameters
        ----------
        ky
            key name that you want the timestamps from (i.e. 'tx_x')

        Returns
        -------
        list
            sorted timestamps of type str, in increasing order
        """

        tstmps = list(self.xyzrph[ky].keys())
        tstmps.sort()
        return tstmps

    def return_tx_xyzrph(self, time_idx: Union[int, str, float]):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the
        transmitter offsets and angles nearest in time to the timestamp

        Parameters
        ----------
        time_idx
            UTC timestamp (accepts int/str/float)

        Returns
        -------
        dict
            key = closest timestamp and values = mounting angle/offsets for receiver
        """

        if self.xyzrph is None:
            self.build_offsets()
        if 'tx_r' in self.xyzrph:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['tx_r'].keys()), time_idx)))
            return {corr_timestmp: {'tx_roll': float(self.xyzrph['tx_r'][corr_timestmp]),
                                    'tx_pitch': float(self.xyzrph['tx_p'][corr_timestmp]),
                                    'tx_heading': float(self.xyzrph['tx_h'][corr_timestmp]),
                                    'tx_x': float(self.xyzrph['tx_x'][corr_timestmp]),
                                    'tx_y': float(self.xyzrph['tx_y'][corr_timestmp]),
                                    'tx_z': float(self.xyzrph['tx_z'][corr_timestmp])}}
        elif 'tx_port_r' in self.xyzrph:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['tx_port_r'].keys()), time_idx)))
            return {corr_timestmp: {'tx_port_roll': float(self.xyzrph['tx_port_r'][corr_timestmp]),
                                    'tx_port_pitch': float(self.xyzrph['tx_port_p'][corr_timestmp]),
                                    'tx_port_heading': float(self.xyzrph['tx_port_h'][corr_timestmp]),
                                    'tx_port_x': float(self.xyzrph['tx_port_x'][corr_timestmp]),
                                    'tx_port_y': float(self.xyzrph['tx_port_y'][corr_timestmp]),
                                    'tx_port_z': float(self.xyzrph['tx_port_z'][corr_timestmp]),
                                    'tx_stbd_roll': float(self.xyzrph['tx_stbd_r'][corr_timestmp]),
                                    'tx_stbd_pitch': float(self.xyzrph['tx_stbd_p'][corr_timestmp]),
                                    'tx_stbd_heading': float(self.xyzrph['tx_stbd_h'][corr_timestmp]),
                                    'tx_stbd_x': float(self.xyzrph['tx_stbd_x'][corr_timestmp]),
                                    'tx_stbd_y': float(self.xyzrph['tx_stbd_y'][corr_timestmp]),
                                    'tx_stbd_z': float(self.xyzrph['tx_stbd_z'][corr_timestmp])
                                    }}
        else:
            self.logger.error('Sonartype not supported: {}'.format(self.sonartype))
            raise NotImplementedError('Sonartype not supported: {}'.format(self.sonartype))

    def return_rx_xyzrph(self, time_idx: Union[int, str, float]):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the
        receiver offsets and angles nearest in time to the timestamp

        Parameters
        ----------
        time_idx
            UTC timestamp (accepts int/str/float)

        Returns
        -------
        dict
            key = closest timestamp and values = mounting angle/offsets for receiver
        """

        if self.xyzrph is None:
            self.build_offsets()
        if 'rx_r' in self.xyzrph:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['rx_r'].keys()), time_idx)))
            return {corr_timestmp: {'rx_roll': float(self.xyzrph['rx_r'][corr_timestmp]),
                                    'rx_pitch': float(self.xyzrph['rx_p'][corr_timestmp]),
                                    'rx_heading': float(self.xyzrph['rx_h'][corr_timestmp]),
                                    'rx_x': float(self.xyzrph['rx_x'][corr_timestmp]),
                                    'rx_y': float(self.xyzrph['rx_y'][corr_timestmp]),
                                    'rx_z': float(self.xyzrph['rx_z'][corr_timestmp])}}
        elif 'rx_port_r' in self.xyzrph:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['rx_port_r'].keys()), time_idx)))
            return {corr_timestmp: {'rx_port_roll': float(self.xyzrph['rx_port_r'][corr_timestmp]),
                                    'rx_port_pitch': float(self.xyzrph['rx_port_p'][corr_timestmp]),
                                    'rx_port_heading': float(self.xyzrph['rx_port_h'][corr_timestmp]),
                                    'rx_port_x': float(self.xyzrph['rx_port_x'][corr_timestmp]),
                                    'rx_port_y': float(self.xyzrph['rx_port_y'][corr_timestmp]),
                                    'rx_port_z': float(self.xyzrph['rx_port_z'][corr_timestmp]),
                                    'rx_stbd_roll': float(self.xyzrph['rx_stbd_r'][corr_timestmp]),
                                    'rx_stbd_pitch': float(self.xyzrph['rx_stbd_p'][corr_timestmp]),
                                    'rx_stbd_heading': float(self.xyzrph['rx_stbd_h'][corr_timestmp]),
                                    'rx_stbd_x': float(self.xyzrph['rx_stbd_x'][corr_timestmp]),
                                    'rx_stbd_y': float(self.xyzrph['rx_stbd_y'][corr_timestmp]),
                                    'rx_stbd_z': float(self.xyzrph['rx_stbd_z'][corr_timestmp])
                                    }}
        else:
            self.logger.error('Sonartype not supported: {}'.format(self.sonartype))
            raise NotImplementedError('Sonartype not supported: {}'.format(self.sonartype))

    def return_nearest_soundspeed_profile(self, time_idx: Union[int, str, float]):
        """
        Using the settings_xxxxx attribute in the xarray dataset and a given timestamp, return the waterline
        offset (relative to the tx) nearest in time to the timestamp.

        Parameters
        ----------
        time_idx
            UTC timestamp (accepts int/str/float)

        Returns
        -------
        dict
            key = closest timestamp and value = waterline offset
        """

        profs = [x for x in self.raw_ping[0].attrs.keys() if x[0:7] == 'profile']
        if len(profs) == 0:
            self.logger.error('No settings attributes found, possibly no install params in multibeam files')
            raise ValueError('No settings attributes found, possibly no install params in multibeam files')

        prof_tims = [float(x.split('_')[1]) for x in profs]
        closest_tim = str(int(_closest_key_value(prof_tims, time_idx)))
        return self.raw_ping[0].attrs['profile_{}'.format(closest_tim)]

    def return_ping_counters_at_time(self, tme: Union[float, np.array]):
        """
        Accepts times as float or a numpy array of times

        To rebuild the full ping at a specific time, you need to get the ping counter(s) at that time.  EM2040c
        have multiple pings at a specific time, so this will return a list of counters that is usually only one
        element long.

        Parameters
        ----------
        tme
            float or numpy array, time to find ping counters for

        Returns
        -------
        cntrs
            list of ints for ping counter numbers at that time
        """

        cntrs = None
        for ra in self.raw_ping:
            overlap = np.intersect1d(ra.time, tme)
            if np.any(overlap):
                if cntrs is None:
                    cntrs = ra.sel(time=overlap).counter.values
                else:
                    cntrs = np.concatenate([cntrs, ra.sel(time=overlap).counter.values])
        return np.unique(cntrs)

    def return_all_profiles(self):
        """
        Return dict of attribute_name/data for each sv profile in the ping dataset

        attribute name is always 'profile_timestamp' format, ex: 'profile_1503411780'

        Returns
        -------
        list
            list of profile names
        list
            list of [depth values, sv values] for each profile
        list
            list of times in utc seconds for each profile
        list
            list of [latitude, longitude] for each profile
        """

        casts = []
        casttimes = []
        castlocations = []
        prof_keys = [x for x in self.raw_ping[0].attrs.keys() if x[0:8] == 'profile_']
        if prof_keys:
            for prof in prof_keys:
                castdata = json.loads(self.raw_ping[0].attrs[prof])
                casts.append(np.array(castdata).T.tolist())
                tme = int(prof.split('_')[1])
                casttimes.append(tme)
                try:
                    matching_attribute = json.loads(self.raw_ping[0].attrs['attributes_{}'.format(tme)])
                    castlocations.append(matching_attribute['location'])
                except KeyError:
                    print('Missing attributes record for {}'.format(prof))
                    castlocations.append(None)
            return prof_keys, casts, casttimes, castlocations
        else:
            return None, None, None, None

    def return_waterline(self, time_idx: Union[int, str, float]):
        """
        Using the settings_xxxxx attribute in the xarray dataset and a given timestamp, return the waterline
        offset (relative to the tx) nearest in time to the timestamp.

        Parameters
        ----------
        time_idx
            UTC timestamp (accepts int/str/float)

        Returns
        -------
        dict
            key = closest timestamp and value = waterline offset
        """

        settrecs = [x for x in self.raw_ping[0].attrs.keys() if x[0:8] == 'settings']
        if len(settrecs) == 0:
            self.logger.error('No settings attributes found, possibly no installation parameters in source files')
            raise ValueError('No settings attributes found, possibly no installation parameters in source files')

        sett_tims = [float(x.split('_')[1]) for x in settrecs]
        closest_tim = str(int(_closest_prior_key_value(sett_tims, time_idx)))
        return float(json.loads(self.raw_ping[0].attrs['settings_' + closest_tim])['waterline_vertical_location'])

    def return_system_time_indexed_array(self, subset_time: list = None):
        """
        Most of the processing involves matching static, timestamped offsets or angles to time series data.  Given that
        we might have a list of systems for dual head sonar and a list of timestamped offsets, need to iterate through
        all of this in each processing loop.  Systems/timestamps length should be minimal, so we just loop in python.

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process

        Returns
        -------
        list
            list of indices for each system/timestamped offsets that are within the provided subset.  length of the list
            is the number of heads for this sonar.
        """

        resulting_systems = []
        prefixes = self.return_xyz_prefixes_for_systems()
        for cnt, ra in enumerate(self.raw_ping):
            if ra is None:  # get here if we turn one of the heads off but just setting the dataset to None
                resulting_systems.append(None)
                continue
            txrx = prefixes[cnt]
            tstmps = self.return_xyzrph_sorted_timestamps(txrx[0] + '_x')
            resulting_tstmps = []
            for tstmp in tstmps:
                # fudge factor for how the install param record is sometime included up to a couple seconds after the
                #    start of the file
                mintime = float(ra.time.min())
                newtstmp = tstmp
                if abs(float(tstmp) - mintime) <= 3.0:
                    # self.logger.info('adjusting the install param record from {} to {}'.format(tstmp, mintime))
                    newtstmp = mintime

                try:  # for when you have multiple timestamps in the installation parameters record, don't include the next timestamp though
                    maxtime = float(tstmps[tstmps.index(tstmp) + 1]) - 0.001
                except:  # otherwise ensure you include the whole ping record
                    maxtime = float(ra.time.max())

                # timestamps for ping records that are at or past the install param record we are using
                tx_idx = np.logical_and(ra.time >= float(newtstmp), ra.time <= float(maxtime))

                if subset_time is not None:  # only include times that fall within the subset ranges
                    final_tx_idx = np.zeros_like(ra.time, dtype=bool)
                    if type(subset_time[0]) is not list:  # fix for when user provides just a list of floats [start,end]
                        subset_time = [subset_time]
                    for minimum_time, maximum_time in subset_time:  # build the mask for each subset and add it to the final idx
                        start_msk = np.logical_and(ra.time >= float(minimum_time), ra.time <= float(maximum_time))
                        start_msk = np.logical_and(tx_idx, start_msk)
                        final_tx_idx = np.logical_or(final_tx_idx, start_msk)
                    tx_idx = final_tx_idx
                resulting_tstmps.append([tx_idx, tstmp, txrx])
            resulting_systems.append(resulting_tstmps)
        return resulting_systems

    def return_xyz_prefixes_for_systems(self):
        """
        self.raw_ping contains Datasets broken up by system.  This method will return the prefixes you need to get
        the offsets/angles from self.xyzrph depending on dual-head

        Returns
        -------
        List
            list of two element lists containing the prefixes needed for tx/rx offsets and angles
        """

        if 'tx_r' in self.xyzrph and 'rx_r' in self.xyzrph:
            leverarms = [['tx', 'rx']]
        elif 'tx_port_r' in self.xyzrph and 'rx_port_r' in self.xyzrph:
            leverarms = [['tx_port', 'rx_port'], ['tx_stbd', 'rx_stbd']]
        elif 'tx_r' in self.xyzrph and 'rx_port_r' in self.xyzrph:
            leverarms = [['tx', 'rx_port'], ['tx', 'rx_stbd']]
        elif 'tx_port_r' in self.xyzrph and 'rx_r' in self.xyzrph:
            leverarms = [['tx_port', 'rx'], ['tx_stbd', 'rx']]
            print("Warning: The dual tx / single rx configuration is not tested and could create issues with the Kluster datasets.")
        else:
            self.logger.error('Not supporting this sonartype yet.')
            raise NotImplementedError('Not supporting this sonartype yet.')

        lever_prefix = []
        for ra in self.raw_ping:
            systemident = ra.system_identifier
            # dual head logic
            if len(leverarms) > 1:
                if systemident == str(ra.system_serial_number[0]):
                    lever_prefix.append(leverarms[0])
                elif systemident == str(ra.secondary_system_serial_number[0]):
                    lever_prefix.append(leverarms[1])
                else:
                    self.logger.error('Found serial number attribute not included in sector')
                    raise NotImplementedError('Found serial number attribute not included in sector')
            else:
                lever_prefix.append(leverarms[0])
        return lever_prefix

    def return_prefix_for_rp(self):
        """
        Determine the correct prefix index based on the sonar reference point of this converted data.  For instance,
        if the sonar reference point is ['tx_x', 'tx_y', 'rx_z'], the returned prefix indices would be [0,0,1], which
        will allow you to pull the correct lever arms from the xyzrph indices. See return_system_time_indexed_array.
        """
        try:
            refpt = self.raw_ping[0].attrs['sonar_reference_point']
            refpt = [0 if lvarm[:2] == 'tx' else 1 for lvarm in refpt]
        except:  # we assume kongsberg (where the tx is the refpoint) for older data that did not have this attribute
            print('WARNING: Unable to find the sonar_reference_point attribute that was added in Kluster 0.8.5, defaulting to Kongsberg convention')
            refpt = [0, 0, 0]
        return refpt

    def return_utm_zone_number(self):
        """
        Get the minimum/maximum longitude values and return the utm zone number

        Returns
        -------
        str
            zone number, e.g. '19N' for UTM Zone 19 N
        """

        zne = return_zone_from_min_max_long(self.raw_ping[0].min_lon, self.raw_ping[0].max_lon, self.raw_ping[0].min_lat)
        return zne


def determine_good_chunksize(fil: str, minchunksize: int = 40000000, max_chunks: int = 20):
    """
    With given file, determine the best size of the chunk to read from it, given a minimum chunksize and a max
    number of chunks.

    Parameters
    ----------
    fil
        path to file we want to analyze
    minchunksize
        minimum size in bytes for the chunksize
    max_chunks
        maximum number of chunks desired

    Returns
    -------
    int
        Size in bytes for the recommended chunk size
    """

    filesize = os.path.getsize(fil)

    # get number of chunks at minchunksize
    min_chunks = filesize / minchunksize
    if filesize <= minchunksize:
        finalchunksize = int(filesize)
    elif min_chunks <= max_chunks:
        # small files can use the minchunksize and be under the maxchunks per file
        # take remainder of min_chunks and glob it on to the chunksize
        #   if rounding ends up building chunks that leave out the last byte or something, don't worry, you are
        #   retaining the file handler and searching past the chunksize anyway
        max_chunks = int(np.floor(min_chunks))
        finalchunksize = int(minchunksize + (((min_chunks % 1) * minchunksize) / max_chunks))
    else:
        # Need a higher chunksize to get less than max_chunks chunks
        # Take chunks over max_chunks and increase chunksize to get to max_chunks
        overflowchunks = min_chunks - max_chunks
        finalchunksize = int(minchunksize + ((overflowchunks * minchunksize) / max_chunks))

    return finalchunksize


def return_chunked_fil(fil: str, startoffset: int = 0, chunksize: int = 20 * 1024 * 1024):
    """
    With given file, determine the best size of the chunk to read from it, given a minimum chunksize and a max
    number of chunks.

    Parameters
    ----------
    fil
        path to file we want to analyze
    startoffset
        byte offset for the start location of the read in the file
    chunksize
        calculated chunksize desired for read operation in bytes

    Returns
    -------
    list
        List containing [filepath, starting offset in bytes, end of chunk pointer in bytes] for each chunk to be read from file
    """

    filesize = os.path.getsize(fil)
    midchunks = [(t * chunksize + chunksize - startoffset) for t in range(int(filesize / chunksize))]

    # Sometimes this results in the last element being basically equal to the filesize when running it under client.map
    #    Do a quick check and just remove the element so you don't end up with a chunk that is like 10 bytes long
    if filesize - midchunks[len(midchunks) - 1] <= 1024:
        midchunks.remove(midchunks[len(midchunks) - 1])

    chunks = [startoffset] + midchunks + [filesize]
    chnkfil = []
    for chnk in chunks:
        if chunks.index(chnk) < len(chunks) - 1:  # list is a range, skip the last one as prev ended at the last index
            chnkfil.append([fil, chnk, chunks[chunks.index(chnk) + 1]])
    return chnkfil


def get_nearest_runtime(timestamp: str, runtime_settdict: dict):
    """
    Both installation parameters and runtime parameters have timestamped entries of values.  Here we try and find the
    nearest entry to the provided timestamp.

    Parameters
    ----------
    timestamp
        utc timestamp we want to find the nearest entry for
    runtime_settdict
        timestamped entries for the installation parameters

    Returns
    -------
    dict
        runtime parameters for the provided timestamp
    """
    try:
        runtime_tstmps = np.array([float(tstmp) for tstmp in runtime_settdict])
        key_timstamp = float(timestamp)
        diff = np.abs(runtime_tstmps - key_timstamp)
        nearest_idx = np.argmin(diff)
        runtime_param = runtime_settdict[[tstmp for tstmp in runtime_settdict][nearest_idx]]
    except:
        runtime_param = {}
    return runtime_param


def build_xyzrph(settdict: dict, runtime_settdict: dict, sonartype: str, logger: logging.Logger = None):
    """
    Translate the raw settings dictionary from the multibeam file (see sequential_read_records) into a dictionary of
    timestamped entries for each sensor offset/angle.  Sector based phase center differences are included as well.

    Also attach default tpu parameters based on NOAA setup.  Assumes POS MV, vessel surveys as done by NOAA, patch test
    values commonly seen, etc.

    Parameters
    ----------
    settdict
        keys are unix timestamps, vals are json dumps containing key/record for each system
    runtime_settdict
        keys are unix timestamps, vals are json dumps containing key/record for each system
    sonartype
        sonar identifer
    logger
        optional, logger object if you want to log any text output to a particular file/handler

    Returns
    -------
    dict
        keys are translated entries, vals are dicts with timestamps:values
    """

    xyzrph = {}
    for tme in settdict:
        runtime_params = get_nearest_runtime(tme, runtime_settdict)
        opening_angle = None
        xyzrph[tme] = {}
        for val in [v for v in sonar_translator[sonartype] if v is not None]:  # tx, rx, etc.
            ky = sonar_translator[sonartype].index(val)  # 0, 1, 2, etc
            if val.find('txrx') != -1:
                # for right now, if you have a sonar like the 2040c where rx and tx are basically in the same
                #   physical container (with the same offsets), just make the tx and rx entries the same
                if val == 'txrx_port':
                    tx_ident = 'tx_port'
                    rx_ident = 'rx_port'
                elif val == 'txrx_stbd':
                    tx_ident = 'tx_stbd'
                    rx_ident = 'rx_stbd'
                else:
                    tx_ident = 'tx'
                    rx_ident = 'rx'
                xyzrph[tme][tx_ident + '_x'] = float(settdict[tme]['transducer_{}_along_location'.format(ky)])
                xyzrph[tme][tx_ident + '_y'] = float(settdict[tme]['transducer_{}_athwart_location'.format(ky)])
                xyzrph[tme][tx_ident + '_z'] = float(settdict[tme]['transducer_{}_vertical_location'.format(ky)])
                xyzrph[tme][tx_ident + '_r'] = float(settdict[tme]['transducer_{}_roll_angle'.format(ky)])
                xyzrph[tme][tx_ident + '_p'] = float(settdict[tme]['transducer_{}_pitch_angle'.format(ky)])
                xyzrph[tme][tx_ident + '_h'] = float(settdict[tme]['transducer_{}_heading_angle'.format(ky)])
                xyzrph[tme][rx_ident + '_r'] = float(settdict[tme]['transducer_{}_roll_angle'.format(ky)])
                xyzrph[tme][rx_ident + '_p'] = float(settdict[tme]['transducer_{}_pitch_angle'.format(ky)])
                xyzrph[tme][rx_ident + '_h'] = float(settdict[tme]['transducer_{}_heading_angle'.format(ky)])
                try:  # kmall workflow, rx offset is tacked on to the trans1 record
                    xyzrph[tme][rx_ident + '_x'] = float(settdict[tme]['transducer_{}_along_location'.format(ky)]) +\
                                                   float(settdict[tme]['transducer_{}_rx_forward'.format(ky)])
                    xyzrph[tme][rx_ident + '_y'] = float(settdict[tme]['transducer_{}_athwart_location'.format(ky)]) +\
                                                   float(settdict[tme]['transducer_{}_rx_starboard'.format(ky)])
                    xyzrph[tme][rx_ident + '_z'] = float(settdict[tme]['transducer_{}_vertical_location'.format(ky)]) +\
                                                   float(settdict[tme]['transducer_{}_rx_down'.format(ky)])
                except KeyError:
                    xyzrph[tme][rx_ident + '_x'] = float(settdict[tme]['transducer_{}_along_location'.format(ky)])
                    xyzrph[tme][rx_ident + '_y'] = float(settdict[tme]['transducer_{}_athwart_location'.format(ky)])
                    xyzrph[tme][rx_ident + '_z'] = float(settdict[tme]['transducer_{}_vertical_location'.format(ky)])
                try:  # kmall
                    xyzrph[tme][tx_ident + '_opening_angle'] = float(settdict[tme]['transducer_{}_sounding_size_deg'.format(ky)])
                    xyzrph[tme][rx_ident + '_opening_angle'] = float(settdict[tme]['transducer_{}_sounding_size_deg'.format(ky)])
                except KeyError:
                    try:  # .all workflow reading from runtime parameters
                        xyzrph[tme][tx_ident + '_opening_angle'] = float(runtime_params['TransmitBeamWidth'])
                        xyzrph[tme][rx_ident + '_opening_angle'] = float(runtime_params['ReceiveBeamWidth'])
                    except:
                        if logger:
                            logger.warning('build_xyzrph: Warning, unable to decode transducer beam width, using default value of {}'.format(kluster_variables.default_beam_opening_angle))
                        else:
                            print('build_xyzrph: Warning, unable to decode transducer beam width, using default value of {}'.format(kluster_variables.default_beam_opening_angle))
                        xyzrph[tme][tx_ident + '_opening_angle'] = kluster_variables.default_beam_opening_angle
                        xyzrph[tme][rx_ident + '_opening_angle'] = kluster_variables.default_beam_opening_angle
            else:
                xyzrph[tme][val + '_x'] = float(settdict[tme]['transducer_{}_along_location'.format(ky)])
                xyzrph[tme][val + '_y'] = float(settdict[tme]['transducer_{}_athwart_location'.format(ky)])
                xyzrph[tme][val + '_z'] = float(settdict[tme]['transducer_{}_vertical_location'.format(ky)])
                xyzrph[tme][val + '_r'] = float(settdict[tme]['transducer_{}_roll_angle'.format(ky)])
                xyzrph[tme][val + '_p'] = float(settdict[tme]['transducer_{}_pitch_angle'.format(ky)])
                xyzrph[tme][val + '_h'] = float(settdict[tme]['transducer_{}_heading_angle'.format(ky)])
                try:  # kmall
                    try:
                        xyzrph[tme][val + '_opening_angle'] = float(settdict[tme]['transducer_{}_sounding_size_deg'.format(ky)])
                    except:  # some files only include opening angle for the first two transducers out of four (dual head)
                        try:
                            xyzrph[tme][val + '_opening_angle'] = float(settdict[tme]['transducer_{}_sounding_size_deg'.format(ky - 2)])
                        except:  # just use the last good ky, seen this with a 2 transducer system (2040) where only the first entry has sounding_size_deg
                            found = False
                            for chkval in [v for v in sonar_translator[sonartype] if v is not None]:  # tx, rx, etc.
                                chkky = sonar_translator[sonartype].index(chkval)
                                if 'transducer_{}_sounding_size_deg'.format(chkky) in settdict[tme]:
                                    xyzrph[tme][val + '_opening_angle'] = float(settdict[tme]['transducer_{}_sounding_size_deg'.format(chkky)])
                                    found = True
                                    break
                            if not found:
                                raise KeyError
                except KeyError:
                    if val.find('tx') != -1:
                        runtimekey = 'TransmitBeamWidth'
                    else:
                        runtimekey = 'ReceiveBeamWidth'
                    try:  # .all workflow reading from runtime parameters
                        xyzrph[tme][val + '_opening_angle'] = float(runtime_params[runtimekey])
                    except:
                        if logger:
                            logger.warning('build_xyzrph: Warning, unable to decode transducer beam width, using default value of {}'.format(kluster_variables.default_beam_opening_angle))
                        else:
                            print('build_xyzrph: Warning, unable to decode transducer beam width, using default value of {}'.format(kluster_variables.default_beam_opening_angle))
                        xyzrph[tme][val + '_opening_angle'] = kluster_variables.default_beam_opening_angle

        # additional offsets based on sector
        if sonartype in install_parameter_modifier:
            for val in [v for v in install_parameter_modifier[sonartype] if v is not None]:
                for sec in install_parameter_modifier[sonartype][val]:
                    xyzrph[tme][val + '_x_' + sec] = float(install_parameter_modifier[sonartype][val][sec]['x'])
                    xyzrph[tme][val + '_y_' + sec] = float(install_parameter_modifier[sonartype][val][sec]['y'])
                    xyzrph[tme][val + '_z_' + sec] = float(install_parameter_modifier[sonartype][val][sec]['z'])

        # translate over the positioning sensor stuff using the installation parameters active identifiers
        pos_ident = settdict[tme]['active_position_system_number']  # 'position_1'
        for suffix in [['_vertical_location', '_z'], ['_along_location', '_x'],
                       ['_athwart_location', '_y']]:
            qry = pos_ident + suffix[0]
            try:
                xyzrph[tme]['imu' + suffix[1]] = float(settdict[tme][qry])
            except KeyError:
                xyzrph[tme]['imu' + suffix[1]] = 0.0
        xyzrph[tme]['latency'] = 0.0

        # do the same over motion sensor (which is still the POSMV), make assumption that its one of the motion
        #   entries
        pos_motion_ident = settdict[tme]['active_heading_sensor'].split('_')
        pos_motion_ident = pos_motion_ident[0] + '_sensor_' + pos_motion_ident[1]  # 'motion_sensor_1'

        # for suffix in [['_vertical_location', '_motionz'], ['_along_location', '_motionx'],
        #                ['_athwart_location', '_motiony'], ['_time_delay', '_motionlatency'],
        #                ['_roll_angle', '_r'], ['_pitch_angle', '_p'], ['_heading_angle', '_h']]:
        for suffix in [['_roll_angle', '_r'], ['_pitch_angle', '_p'], ['_heading_angle', '_h']]:
            qry = pos_motion_ident + suffix[0]
            try:
                xyzrph[tme]['imu' + suffix[1]] = float(settdict[tme][qry])
            except KeyError:
                xyzrph[tme]['imu' + suffix[1]] = 0.0

        # include waterline if it exists
        if 'waterline_vertical_location' in settdict[tme]:
            xyzrph[tme]['waterline'] = float(settdict[tme]['waterline_vertical_location'])

        # attach default tpu settings
        xyzrph[tme]['heave_error'] = kluster_variables.default_heave_error  # 1 sigma standard deviation for the heave data (meters)
        xyzrph[tme]['roll_sensor_error'] = kluster_variables.default_roll_sensor_error  # 1 sigma standard deviation in the roll sensor (degrees)
        xyzrph[tme]['pitch_sensor_error'] = kluster_variables.default_pitch_sensor_error  # 1 sigma standard deviation in the pitch sensor (degrees)
        xyzrph[tme]['heading_sensor_error'] = kluster_variables.default_heading_sensor_error  # 1 sigma standard deviation in the pitch sensor (degrees)
        xyzrph[tme]['tx_to_antenna_x'] = kluster_variables.default_tx_to_antenna_x  # 1 sigma standard deviation in your measurement of x lever arm (meters)
        xyzrph[tme]['tx_to_antenna_y'] = kluster_variables.default_tx_to_antenna_y  # 1 sigma standard deviation in your measurement of y lever arm (meters)
        xyzrph[tme]['tx_to_antenna_z'] = kluster_variables.default_tx_to_antenna_z  # 1 sigma standard deviation in your measurement of z lever arm (meters)
        xyzrph[tme]['x_offset_error'] = kluster_variables.default_x_offset_error  # 1 sigma standard deviation in your measurement of x lever arm (meters)
        xyzrph[tme]['y_offset_error'] = kluster_variables.default_y_offset_error  # 1 sigma standard deviation in your measurement of y lever arm (meters)
        xyzrph[tme]['z_offset_error'] = kluster_variables.default_z_offset_error  # 1 sigma standard deviation in your measurement of z lever arm (meters)
        xyzrph[tme]['surface_sv_error'] = kluster_variables.default_surface_sv_error  # 1 sigma standard deviation in surface sv sensor (meters/second)
        xyzrph[tme]['roll_patch_error'] = kluster_variables.default_roll_patch_error  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        xyzrph[tme]['pitch_patch_error'] = kluster_variables.default_pitch_patch_error  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        xyzrph[tme]['heading_patch_error'] = kluster_variables.default_heading_patch_error  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        xyzrph[tme]['latency_patch_error'] = kluster_variables.default_latency_patch_error  # 1 sigma standard deviation in your latency calculation (seconds)
        xyzrph[tme]['timing_latency_error'] = kluster_variables.default_timing_latency_error  # 1 sigma standard deviation of the timing accuracy of the system (seconds)
        xyzrph[tme]['separation_model_error'] = kluster_variables.default_separation_model_error  # 1 sigma standard deivation in the sep model (tidal, ellipsoidal, etc) (meters)
        xyzrph[tme]['waterline_error'] = kluster_variables.default_waterline_error  # 1 sigma standard deviation of the waterline (meters)
        xyzrph[tme]['vessel_speed_error'] = kluster_variables.default_vessel_speed_error  # 1 sigma standard deviation of the vessel speed (meters/second)
        xyzrph[tme]['horizontal_positioning_error'] = kluster_variables.default_horizontal_positioning_error  # 1 sigma standard deviation of the horizontal positioning (meters)
        xyzrph[tme]['vertical_positioning_error'] = kluster_variables.default_vertical_positioning_error  # 1 sigma standard deviation of the vertical positioning (meters)

    # generate dict of ordereddicts for fast searching
    newdict = {}
    for ky in xyzrph:
        for stmp in xyzrph[ky].keys():
            if stmp not in newdict:
                newdict[stmp] = SortedDict()
            newdict[stmp][ky] = xyzrph[ky][stmp]
    xyzrph = SortedDict(newdict)
    only_retain_earliest_entry(xyzrph)

    return xyzrph


def sort_and_drop_duplicates(dset: xr.Dataset, dsetpath: str):
    """
    Check for duplicates and sort if necessary.  We've picked methods here to conserve memory, using just the included
    is_unique property is not as efficient as doing it in numpy.  The isel and sortby statements will load the lazy
    loaded dataset into memory to do the reindexing, so we want to avoid those statements if at all possible.

    Duplicates will cause the is_monotonic_increasing to be False, so check for those first.

    Parameters
    ----------
    dset
        xarray dataset to sort/drop
    dsetpath
        path to the xarray dataset on disk

    Returns
    -------
    xr.Dataset
        sorted and unique dataset
    """

    _, index = np.unique(dset['time'], return_index=True)
    if dset['time'].size != index.size:
        print('Dataset {} contains duplicate times, forced to drop duplicates on reload'.format(dsetpath))
        dset = dset.isel(time=index)
    if not dset.time.indexes['time'].is_monotonic_increasing:
        print('Dataset {} is not sorted, forced to sort on reload'.format(dsetpath))
        dset = dset.sortby('time')
    return dset


def return_xyzrph_from_mbes(mbesfil: str, logger: logging.Logger = None):
    """
    Currently being used to load Vessel View with the first installation parameters from a multibeam file.  This will
    take the first installation record in the multibeam file and convert it over to the xyzrph format used by kluster.

    xyzrph is a dict of offsets/angles (ex: 'tx_x') and values organized by the time of the installation record
    (ex: SortedDict({'1583306608': '0.000'})).

    Parameters
    ----------
    mbesfil
        path to a multibeam file
    logger
        optional, logger object if you want to log any text output to a particular file/handler

    Returns
    -------
    dict
        translated installation parameter record in the format used by Kluster
    str
        sonar model number
    int
        primary system serial number
    """

    recs = sequential_read_multibeam(mbesfil, start_pointer=0, end_pointer=0, first_installation_rec=True)
    try:
        settings_dict = {str(int(recs['installation_params']['time'][0])): recs['installation_params']['installation_settings'][0]}
        runtime_dict = {}
        snrmodels = np.unique([settings_dict[x]['sonar_model_number'] for x in settings_dict])
        if len(snrmodels) > 1:
            if logger:
                logger.error('Found multiple sonars types in data provided: {}'.format(snrmodels))
            raise NotImplementedError('Found multiple sonars types in data provided: {}'.format(snrmodels))
        sonartype = snrmodels[0].lower()
        if sonartype not in sonar_translator:
            if logger:
                logger.error('Sonar model not understood "{}"'.format(snrmodels[0]))
            raise NotImplementedError('Sonar model not understood "{}"'.format(snrmodels[0]))
        serialnum = np.unique(recs['installation_params']['serial_one'])
        if len(serialnum) > 1:
            if logger:
                logger.error('Found multiple sonar serial numbers in data provided: {}'.format(snrmodels))
            raise NotImplementedError('Found multiple sonar serial numbers in data provided: {}'.format(snrmodels))
        serialnum = serialnum[0]

        # translate over the offsets/angles for the transducers following the sonar_translator scheme
        xyzrph = build_xyzrph(settings_dict, runtime_dict, sonartype, logger=logger)

        return xyzrph, sonartype, serialnum
    except IndexError:
        if logger:
            logger.error('Unable to read from {}: data not found for installation records'.format(mbesfil))
            logger.error(recs['installation_params'])
        else:
            print('Unable to read from {}: data not found for installation records'.format(mbesfil))
            print(recs['installation_params'])
        return None, None, None


def return_xyzrph_from_posmv(posfile: str):
    """
    Translate the MSG20 message in the POS File to xyzrph like sensor names.  Use this to populate an existing
    xyzrph record built by kluster to get the POSMV imu/antenna related sensors.

    Parameters
    ----------
    posfile
        path to a posmv file

    Returns
    -------
    dict
        dictionary of offset/angle names to values found in the MSG20 message
    """

    return return_offsets_from_posfile(posfile)
