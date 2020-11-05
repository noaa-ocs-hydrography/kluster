import os
from glob import glob
from dask.distributed import Client, Future, progress
import matplotlib.pyplot as plt
import webbrowser
from time import perf_counter
from sortedcontainers import SortedDict
import json
from datetime import datetime
import xarray as xr
import numpy as np
from typing import Union

from HSTB.kluster.dms import return_zone_from_min_max_long
from HSTB.drivers import par3, kmall
from HSTB.drivers import PCSio
from HSTB.kluster.dask_helpers import dask_find_or_start_client, DaskProcessSynchronizer
from HSTB.kluster.xarray_helpers import resize_zarr, xarr_to_netcdf, combine_xr_attributes, reload_zarr_records, \
                                        get_write_indices_zarr, distrib_zarr_write, my_xarr_add_attribute
from HSTB.kluster.logging_conf import return_logger


# this was a flag that I used to set when dask/xarray failed to import
batch_read_enabled = True

sonar_translator = {'em122': [None, 'tx', 'rx', None], 'em302': [None, 'tx', 'rx', None],
                    'em710': [None, 'tx', 'rx', None], 'em2040': [None, 'tx', 'rx', None],
                    'em2040_dual_rx': [None, 'tx', 'rx_port', 'rx_stbd'],
                    'em2040_dual_tx': ['tx_port', 'tx_stbd', 'rx_port', 'rx_stbd'],
                    # EM2040c is represented in the .all file as em2045
                    'em2045': [None, 'txrx', None, None], 'em2045_dual': [None, 'txrx_port', 'txrx_stbd', None],
                    'em3002': [None, 'tx', 'rx', None], 'em2040p': [None, 'txrx', None, None],
                    'me70bo': ['txrx', None, None, None]}

install_parameter_modifier = {'em2040_dual_tx': {'rx_port': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
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
    if os.path.splitext(fil)[1] == '.all':
        ar = par3.AllRead(fil, start_ptr=offset, end_ptr=endpt)
        return ar.sequential_read_records()
    else:
        km = kmall.kmall(fil)
        # kmall doesnt have ping-wise serial number in header, we have to provide it from install params
        serial_translator = km.fast_read_serial_number_translator()
        return km.sequential_read_records(start_ptr=offset, end_ptr=endpt, serial_translator=serial_translator)


def _sequential_gather_max_beams(rec: dict):
    """
    Takes in the output from read_sequential, outputs the maximum beam count from the beampointingangle dataset.  Used
    later for trimming to the max beam dimension across all datasets

    Parameters
    ----------
    rec
        dict as returned by sequential_read_records

    Returns
    -------
    int
        max of indices of the last beam across all records
    """

    last_real_beam = np.argmax(rec['ping']['beampointingangle'] == 999, axis=1)
    return np.max(last_real_beam)


def _sequential_trim_to_max_beam_number(rec: dict, max_beam_num: int):
    """
    Takes in the output from read_sequential, returns the records trimmed to the max beam number provided.

    Parameters
    ----------
    rec
        dict as returned by sequential_read_records
    max_beam_num
        int, max beam number across all records/workers

    Returns
    -------
    dict
        original rec with beam dimension trimmed to max_beam_num
    """

    for dgram in rec:
        if dgram == 'ping':
            for rectype in rec[dgram]:
                if rec[dgram][rectype].ndim == 2:
                    rec[dgram][rectype] = rec[dgram][rectype][:, 0:max_beam_num]
    return rec


def _build_sector_mask(rec: dict):
    """
    Range/Angle datagram is going to have duplicate times, which are no good for our Xarray dataset.  Duplicates are
    found at the same time but with different sectors, serial numbers (dualheadsystem) and/or frequency.  Split up the
    records by these three elements and use them as the sector identifier

    Parameters
    ----------
    rec
        dict as returned by sequential_read_records

    Returns
    -------
    list
        contains string identifiers for each sector, ex: ['40111_0_265000', '40111_0_275000', '40111_1_285000',
        '40111_1_290000', '40111_2_270000', '40111_2_280000']
    list
        index of where each sector_id identifier shows up in the data
    """

    # serial_nums = [rec['73'][x][0] for x in ['serial#', 'serial#2'] if rec['73'][x][0] != 0]
    serial_nums = list(np.unique(rec['ping']['serial_num']))
    sector_nums = [i for i in range(rec['ping']['ntx'][0] + 1)]
    freqs = [f for f in np.unique(rec['ping']['frequency'])]
    sector_ids = []
    id_mask = []

    for x in serial_nums:
        ser_mask = np.where(rec['ping']['serial_num'] == float(x))[0]
        for y in sector_nums:
            sec_mask = np.where(rec['ping']['txsectorid'] == float(y))[0]
            for z in freqs:
                f_mask = np.where(rec['ping']['frequency'] == z)[0]
                totmask = [x for x in ser_mask if (x in sec_mask) and (x in f_mask)]
                if len(totmask) > 0:
                    # z (freq identifier) must be length 6 or else you end up with dtype=object that messes up
                    #    serialization later.  Zero pad to solve this
                    if len(str(int(z))) < 6:
                        z = '0' + str(int(z))
                    else:
                        z = str(int(z))
                    sector_ids.append(str(x) + '_' + str(y) + '_' + z)
                    id_mask.append(totmask)
    return sector_ids, id_mask


def _assign_reference_points(fileformat: str, finalraw: xr.Dataset, finalatt: xr.Dataset, finalnav: xr.Dataset):
    """
    Use what we've learned from reading these multibeam data definition documents to record the relevant information
    about each variable.  Logs this info as attributes in the final xarray dataset.  'reference' refers to the
    reference point of each variable.

    Parameters
    ----------
    fileformat
        multibeam file format (all, kmall supported currently)
    finalraw
        xarray dataset containing the ping records
    finalatt
        xarray dataset containing the attitude records
    finalnav
        xarray dataset containing the navigation records

    Returns
    -------
    xr.Dataset
        finalraw dataset with new attribution
    xr.Dataset
        finalatt dataset with new attribution
    xr.Dataset
        finalnav dataset with new attribution
    """

    try:
        if fileformat in ['all', 'kmall']:
            finalnav.attrs['reference'] = {'latitude': 'reference point', 'longitude': 'reference point',
                                           'altitude': 'reference point'}
            finalnav.attrs['units'] = {'latitude': 'degrees', 'longitude': 'degrees',
                                       'altitude': 'meters (+ down from ellipsoid)'}
            finalatt.attrs['reference'] = {'heading': 'reference point', 'heave': 'transmitter',
                                           'pitch': 'reference point', 'roll': 'reference point'}
            finalatt.attrs['units'] = {'heading': 'degrees', 'heave': 'meters (+ down)', 'pitch': 'degrees',
                                       'roll': 'degrees'}
            finalraw.attrs['reference'] = {'beampointingangle': 'receiver', 'tiltangle': 'transmitter',
                                           'traveltime': 'None'}
            finalraw.attrs['units'] = {'beampointingangle': 'degrees', 'tiltangle': 'degrees', 'traveltime': 'seconds'}
            return finalraw, finalatt, finalnav
        else:
            raise ValueError('Did not recognize format "{}" during xarray conversion'.format(fileformat))
    except KeyError:
        raise KeyError('Did not find the "format" key in the sequential read output')


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
    xr.Dataset
        navigation records as Dataset, timestamps as coordinates, metadata as attribution
    """

    if 'ping' not in rec:
        print('No ping raw range/angle record found for chunk file')
        return
    recs_to_merge = {}
    alltims = np.unique(rec['ping']['time'])  # after mask/splitting data, should get something for each unique time
    for r in rec:
        if r not in ['installation_params', 'profile', 'format']:  # These are going to be added as attributes later
            recs_to_merge[r] = xr.Dataset()
            if r == 'ping':  # R&A is the only datagram we use that requires splitting by sector/serial#/freq
                ids, msk = _build_sector_mask(rec)  # get the identifiers and mask for each sector/serial#/freq
                for ky in rec[r]:
                    if ky not in ['time', 'serial_num', 'frequency', 'txsectorid']:
                        combined_sectors = []
                        for secid in ids:
                            idx = ids.index(secid)

                            if ky == 'counter':  # counter is 16bit in raw data, we want 32 to handle zero crossing
                                datadtype = np.int64
                            else:
                                datadtype = rec[r][ky].dtype

                            arr = np.array(rec['ping'][ky][msk[idx]])  # that part of the record for the given sect_id
                            tim = rec['ping']['time'][msk[idx]]

                            # currently i'm getting a one rec duplicate between chunked files...
                            if tim[-1] == tim[-2] and np.array_equal(arr[-1], arr[-2]):
                                # print('Found duplicate timestamp: {}, {}, {}'.format(r, ky, tim[-1]))
                                arr = arr[:-1]
                                tim = tim[:-1]

                            try:
                                arr = np.squeeze(np.array(arr), axis=1)  # Tx_data is handled this way to get 1D
                            except ValueError:
                                pass

                            # use the mask to build a zero-padded array with zeros for all times that have no data
                            paddedshp = list(arr.shape)
                            paddedshp[0] = alltims.shape[0]
                            padded_sector = np.zeros(paddedshp, dtype=datadtype)
                            padded_sector[np.where(np.isin(alltims, tim))] = arr
                            combined_sectors.append(padded_sector)

                        combined_sectors = np.array(combined_sectors)

                        # these records are by time/sector/beam.  Have to combine recs to build correct array shape
                        if ky in ['beampointingangle', 'txsector_beam', 'detectioninfo', 'qualityfactor',
                                  'qualityfactor_percent', 'traveltime']:
                            beam_idx = [i for i in range(combined_sectors.shape[2])]
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[ids, alltims, beam_idx],
                                                                dims=['sector', 'time', 'beam'])
                        #  everything else isn't by beam, proceed normally
                        else:
                            if combined_sectors.ndim == 3:
                                combined_sectors = np.squeeze(combined_sectors, axis=2)
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[ids, alltims],
                                                                dims=['sector', 'time'])
            else:
                for ky in rec[r]:
                    if ky in ['heading', 'heave', 'pitch', 'roll', 'altitude']:
                        recs_to_merge[r][ky] = xr.DataArray(np.float32(rec[r][ky]), coords=[rec[r]['time']], dims=['time'])
                    elif ky not in ['time', 'runtime_settings']:
                        recs_to_merge[r][ky] = xr.DataArray(rec[r][ky], coords=[rec[r]['time']], dims=['time'])

    # occasionally get duplicate time values, possibly from overlap in parallel read
    _, index = np.unique(recs_to_merge['ping']['time'], return_index=True)
    recs_to_merge['ping'] = recs_to_merge['ping'].isel(time=index)

    if 'mode' not in recs_to_merge['ping']:
        #  .all workflow, where the mode/modetwo stuff is not in the ping record, so you have to merge the two
        # take range/angle rec and merge runtime on to that index
        chk = True
        if 'runtime_params' not in list(recs_to_merge.keys()):
            chk = False
        elif not np.any(rec['runtime_params']['time']):
            chk = False
        if not chk:
            # rec82 (runtime params) isn't mandatory, but all datasets need to have the same variables or else the
            #    combine_nested isn't going to work.  So if it isn't in rec (or it is empty) put in an empty dataset here
            #    to be interpolated through later after merge
            recs_to_merge['runtime_params'] = xr.Dataset(data_vars={'mode': (['time'], np.array([''])),
                                                                    'modetwo': (['time'], np.array([''])),
                                                                    'yawpitchstab': (['time'], np.array(['']))},
                                                         coords={'time': np.array([float(recs_to_merge['ping'].time[0])])})

        _, index = np.unique(recs_to_merge['runtime_params']['time'], return_index=True)
        recs_to_merge['runtime_params'] = recs_to_merge['runtime_params'].isel(time=index)
        recs_to_merge['runtime_params'] = recs_to_merge['runtime_params'].reindex_like(recs_to_merge['ping'], method='nearest')
        finalraw = xr.merge([recs_to_merge[r] for r in recs_to_merge if r in ['ping', 'runtime_params']], join='inner')
    else:
        finalraw = recs_to_merge['ping']

    # attitude and nav are returned separately in their own datasets
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
    zero_index = np.where(finalnav.time != 0)[0]
    zeros = len(finalnav.time) - len(zero_index)
    finalnav = finalnav.isel(time=np.where(finalnav.time != 0)[0])

    # build attributes for the navigation/attitude records
    finalnav.attrs['min_lat'] = float(finalnav.latitude.min())
    finalnav.attrs['max_lat'] = float(finalnav.latitude.max())
    finalnav.attrs['min_lon'] = float(finalnav.longitude.min())
    finalnav.attrs['max_lon'] = float(finalnav.longitude.max())

    # Stuff that isn't of the same dimensions as the dataset are tacked on as attributes
    if 'profile' in rec:
        for t in rec['profile']['time']:
            idx = np.where(rec['profile']['time'] == t)
            profile = np.dstack([rec['profile']['depth'][idx][0], rec['profile']['soundspeed'][idx][0]])[0]
            finalraw.attrs['profile_{}'.format(int(t))] = json.dumps(profile.tolist())

    # add on attribute for installation parameters, basically the same way as you do for the ss profile, except it
    #   has no coordinate to index by.  Also, use json.dumps to avoid the issues with serializing lists/dicts with
    #   to_netcdf

    # I'm including these serial numbers for the dual/dual setup.  System is port, Secondary_system is starboard.  These
    #   are needed to identify pings and which offsets to use (TXPORT=Transducer0, TXSTARBOARD=Transducer1,
    #   RXPORT=Transducer2, RXSTARBOARD=Transudcer3)

    if 'installation_params' in rec:
        finalraw.attrs['system_serial_number'] = np.unique(rec['installation_params']['serial_one'])
        finalraw.attrs['secondary_system_serial_number'] = np.unique(rec['installation_params']['serial_two'])
        for t in rec['installation_params']['time']:
            idx = np.where(rec['installation_params']['time'] == t)
            finalraw.attrs['installsettings_{}'.format(int(t))] = json.dumps(rec['installation_params']['installation_settings'][idx][0])
    if 'runtime_params' in rec:
        for t in rec['runtime_params']['time']:
            idx = np.where(rec['runtime_params']['time'] == t)
            finalraw.attrs['runtimesettings_{}'.format(int(t))] = json.dumps(rec['runtime_params']['runtime_settings'][idx][0])

    # get the order of dimensions right
    finalraw = finalraw.transpose('time', 'sector', 'beam')
    # assign reference point and metadata
    finalraw, finalatt, finalnav = _assign_reference_points(rec['format'], finalraw, finalatt, finalnav)

    return finalraw, finalatt, finalnav


def _divide_xarray_futs(xarrfuture: xr.Dataset, mode: str = 'ping'):
    """
    The return from _sequential_to_xarray is a future containing three xarrays.  Map this function to access that future
    and return the xarray specified with the mode keyword.

    Parameters
    ----------
    xarrfuture
        xarray Dataset from _sequential_to_xarray

    Returns
    -------
    xr.Dataset
        selected datatype specified by mode
    """

    idx = ['ping', 'attitude', 'navigation'].index(mode)
    return xarrfuture[idx]


def _divide_xarray_return_sector(xarrfuture: xr.Dataset, secid: str):
    """
    Take in a ping xarray Dataset and return just the secid sector

    Parameters
    ----------
    xarrfuture
        xarray Dataset from _sequential_to_xarray
    secid
        sector identifier that you are searching for (will be in format serialnumber_sector_frequency

    Returns
    -------
    xr.Dataset
        selected datatype specified by mode
    """

    if 'sector' not in xarrfuture.dims:
        return None
    if secid not in xarrfuture.sector.values:
        return None

    xarr_by_sec = xarrfuture.sel(sector=secid).drop('sector')

    # the where statement i do next seems to drop all the variable dtype info.  I seem to have to reset that
    varnames = list(xarr_by_sec.variables.keys())
    xarr_valid_pings = xarr_by_sec.where(xarr_by_sec.ntx != 0, drop=True)
    for v in varnames:
        xarr_valid_pings[v] = xarr_valid_pings[v].astype(xarr_by_sec[v].dtype)

    return xarr_valid_pings


def _divide_xarray_indicate_empty_future(fut: xr.Dataset):
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


def _return_xarray_mintime(xarrs: Union[xr.DataArray, xr.Dataset]):
    """
    Access xarray object and return the length of the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the minimum time from

    Returns
    -------
    bool
        True if fut is data, False if not
    """

    return float(xarrs.time.min())


def _return_xarray_sectors(xarrs: Union[xr.DataArray, xr.Dataset]):
    """
    Return the sector names for the given xarray object

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the sectors from

    Returns
    -------
    list
        sector identifiers as string within a list
    """

    return xarrs.sector.values


def _return_xarray_timelength(xarrs: Union[xr.DataArray, xr.Dataset]):
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
    return finalarr


def gather_dataset_attributes(dataset: xr.Dataset):
    """
    Return the attributes within an Xarray DataSet

    Parameters
    ----------
    dataset
        Xarray DataSet with attribution

    Returns
    -------
    dict
        attributes within dataset
    """

    attrs = dataset.attrs
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
    difs = tim - sett_tims
    closest_tim = sett_tims[np.nanargmin(difs)]
    return closest_tim


def batch_read_configure_options(ping_chunksize: int, nav_chunksize: int, att_chunksize: int):
    """
    Generate the parameters that drive the data conversion.  Chunksize for size of zarr written chunks,
    combine_attributes as a bool to tell the system to look for attributes within that xarray object, and
    output_arrs/final_pths/final_attrs to hold the data as it is processed.

    Parameters
    ----------
    ping_chunksize
        array chunksize used when writing to zarr, each ping record chunk will be this size
    nav_chunksize
        array chunksize used when writing to zarr, each navigation array chunk will be this size
    att_chunksize
        array chunksize used when writing to zarr, each attitude array chunk will be this size

    Returns
    -------
    opts
        dict, options for batch read process
    """

    ping_chunks = {'time': (ping_chunksize,), 'beam': (400,), 'xyz': (3,),
                   'beampointingangle': (ping_chunksize, 400), 'counter': (ping_chunksize,),
                   'detectioninfo': (ping_chunksize,), 'mode': (ping_chunksize,), 'nrx': (ping_chunksize,),
                   'ntx': (ping_chunksize,), 'qualityfactor': (ping_chunksize, 400), 'samplerate': (ping_chunksize,),
                   'soundspeed': (ping_chunksize,), 'modetwo': (ping_chunksize,), 'tiltangle': (ping_chunksize,),
                   'traveltime': (ping_chunksize, 400), 'waveformid': (ping_chunksize,),
                   'yawpitchstab': (ping_chunksize,), 'rxid': (ping_chunksize,), 'qualityfactor_percent': (ping_chunksize,)}
    att_chunks = {'time': (att_chunksize,), 'heading': (att_chunksize,), 'heave': (att_chunksize,),
                  'pitch': (att_chunksize,), 'roll': (att_chunksize,)}
    nav_chunks = {'time': (nav_chunksize,), 'alongtrackvelocity': (nav_chunksize,), 'altitude': (nav_chunksize,),
                  'latitude': (nav_chunksize,), 'longitude': (nav_chunksize,)}
    opts = {
        'ping': {'chunksize': ping_chunksize, 'chunks': ping_chunks, 'combine_attributes': True, 'output_arrs': [],
                 'time_arrs': [], 'final_pths': None, 'final_attrs': None},
        'attitude': {'chunksize': att_chunksize, 'chunks': att_chunks, 'combine_attributes': False, 'output_arrs': [],
                     'time_arrs': [], 'final_pths': None, 'final_attrs': None},
        'navigation': {'chunksize': nav_chunksize, 'chunks': nav_chunks, 'combine_attributes': False, 'output_arrs': [],
                       'time_arrs': [], 'final_pths': None, 'final_attrs': None}}
    return opts


class BatchRead:
    """
    BatchRead - multibeam data converter using dask infrastructure and xarray data types
    Pass in multibeam files, call read(), and gain access to xarray Datasets for each data type

    NOTE: CURRENTLY ONLY ZARR BASED PROCESSING OF KONGSBERG .ALL AND .KMALL FILES IS SUPPORTED

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
    | Rebalancing 10640 total navigation records across 1 blocks of size 50000
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
    |     output_path:                     C:\collab\dasktest\data_dir\EM2040_small...
    |     profile_1495563079:              [[0.0, 1489.2000732421875], [0.32, 1489....
    |     reference:                       {'beampointingangle': 'receiver', 'tilta...
    |     runtimesettings_1495563080:      {"Counter": "61968", "SystemSerial#": "4...
    |     secondary_system_serial_number:  [0]
    |     sector_identifier:               40111_0_265000
    |     survey_number:                   ['01_Patchtest_2806']
    |     system_serial_number:            [40111]
    |     tpu_parameters:                  {'dynamic_draft': 0.1, 'heading_patch': ...
    |     units:                           {'beampointingangle': 'degrees', 'tiltan...
    |     xyzrph:                          {'antenna_x': {'1495563079': '0.000'}, '...
    """

    def __new__(cls, filfolder, dest=None, address=None, client=None, minchunksize=40000000, max_chunks=20,
                filtype='zarr', skip_dask=False, dashboard=False):
        if not batch_read_enabled:
            print('Dask and Xarray are required dependencies to run BatchRead.  Please ensure you have these modules first.')
            return None
        else:
            return super(BatchRead, cls).__new__(cls)

    def __init__(self, filfolder: Union[str, list] = None, dest: str = None, address: str = None, client: Client = None,
                 minchunksize: int = 40000000, max_chunks: int = 20, filtype: str = 'zarr', skip_dask: bool = False,
                 dashboard: bool = False):
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
        """

        self.filfolder = filfolder
        self.dest = dest
        self.filtype = filtype
        self.convert_minchunksize = minchunksize
        self.convert_maxchunks = max_chunks
        self.address = address
        self.raw_ping = None
        self.raw_att = None
        self.raw_nav = None

        self.readsuccess = False

        self.client = client
        if not skip_dask and self.client is None:
            self.client = dask_find_or_start_client(address=self.address)
        if dashboard:
            webbrowser.open_new('http://localhost:8787/status')

        # misc
        self.converted_pth = None
        self.final_paths = {}
        self.fils = None
        self.extents = None
        self.logfile = None
        self.logger = None
        self.ping_chunksize = 1000
        self.nav_chunksize = 50000
        self.att_chunksize = 20000
        self.chunksize_lkup = {'ping': self.ping_chunksize, 'attitude': self.att_chunksize, 'navigation': self.nav_chunksize}

        # install parameters
        self.sonartype = None
        self.xyzrph = None

        # tpu parameters
        self.tpu_parameters = None

    def read(self):
        """
        Run the batch_read method on all available lines, writes to datastore (netcdf/zarr depending on self.filtype),
        and loads the data back into the class as self.raw_ping, self.raw_att, self.raw_nav.

        If data loads correctly, builds out the self.xyzrph attribute and translates the runtime parameters to a usable
        form.
        """

        if self.filtype not in ['zarr']:  # NETCDF is currently not supported
            self.logger.error(self.filtype + ' is not a supported format.')
            raise NotImplementedError(self.filtype + ' is not a supported format.')

        final_pths = self.batch_read_by_sector(self.filtype)

        if final_pths is not None:
            self.final_paths = final_pths
            if self.filtype == 'netcdf':
                self.read_from_netcdf_fils(final_pths['ping'], final_pths['attitude'], final_pths['navigation'])
                self.build_offsets()
            elif self.filtype == 'zarr':
                # kind of dumb, right now I read to get the data, build offsets, save them to the datastore and then
                #   read again.  It isn't that bad though, read is just opening metadata, so it takes less than a second
                self.read_from_zarr_fils(final_pths['ping'], final_pths['attitude'], final_pths['navigation'], self.logfile)
                self.build_offsets(save_pths=final_pths['ping'])  # write offsets to ping rootgroup
                self.read_from_zarr_fils(final_pths['ping'], final_pths['attitude'], final_pths['navigation'], self.logfile)
                self.xyzrph = self.raw_ping[0].xyzrph  # read offsets back to populate self.xyzrph
        else:
            self.logger.error('Unable to start/connect to the Dask distributed cluster.')
            raise ValueError('Unable to start/connect to the Dask distributed cluster.')

        if self.raw_ping is not None:
            self.readsuccess = True
            self.logger.info('read successful\n')
        else:
            self.logger.warning('read unsuccessful, data paths: {}\n'.format(final_pths))

    def read_from_netcdf_fils(self, ping_pths: list, attitude_pths: list, navigation_pths: list):
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
        navigation_pths:
            path to the navigation netcdf files
        """

        # sort input pths by type, currently a list by idx
        self.raw_ping = xr.open_mfdataset(ping_pths, chunks={}, concat_dim='time', combine='nested')
        self.raw_att = xr.open_mfdataset(attitude_pths, chunks={}, concat_dim='time', combine='nested')
        self.raw_nav = xr.open_mfdataset(navigation_pths, chunks={}, concat_dim='time', combine='nested')

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

    def read_from_zarr_fils(self, ping_pth: str, attitude_pth: str, navigation_pth: str, logfile_pth: str):
        """
        Read from the generated zarr datastore constructed with read()

        All the keyword arguments set to False are there to correctly read the saved zarr arrays.  Mask_and_scale i've
        yet to configure properly, it will replace values equal to the fill_value attribute with NaN.  Even when
        fill_value is non-zero, it seems to replace zeros with NaN.  Setting it to false prevents this.  You can read
        more here:  http://xarray.pydata.org/en/stable/generated/xarray.open_zarr.html

        If you are running this outside of the normal dask-enabled workflow, self.client will be None and you will not
        have the distributed sync object.  I do this with reading attributes from the zarr datastore where I just need
        to open for a minute to get the attributes.

        Parameters
        ----------
        ping_pth
            path to the ping zarr group
        attitude_pth
            path to the attitude zarr group
        navigation_pth
            path to the navigation zarr group
        logfile_pth
            path to the text log file used by logging
        """

        self.raw_ping = []
        self.raw_nav = None
        self.raw_att = None

        self.logfile = logfile_pth
        self.initialize_log()

        if type(ping_pth) != list:
            ping_pth = [ping_pth]
        if type(attitude_pth) != list:
            attitude_pth = [attitude_pth]
        if type(navigation_pth) != list:
            navigation_pth = [navigation_pth]

        for ra in ping_pth:
            if self.converted_pth is None:
                self.converted_pth = os.path.dirname(ra)
            if self.client is None:
                skip_dask = True
            else:
                skip_dask = False
            if ra is not None:
                try:
                    xarr = reload_zarr_records(ra, skip_dask)
                    self.raw_ping.append(xarr)
                except ValueError:
                    self.logger.error('Unable to read from {}'.format(ra))
            else:
                self.logger.error('No raw_ping paths found')

        if len(attitude_pth) > 1:
            self.logger.error('Only one attitude data store can be read at a time at this time.')
            raise NotImplementedError('Only one attitude data store can be read at a time at this time.')
        elif len(attitude_pth) == 0:
            self.logger.error('No attitude paths found')
        for at in attitude_pth:
            if self.converted_pth is None:
                self.converted_pth = os.path.dirname(at)
            if self.client is None:
                skip_dask = True
            else:
                skip_dask = False
            if at is not None:
                try:
                    self.raw_att = reload_zarr_records(at, skip_dask)
                    self.raw_att = self.raw_att.isel(time=np.unique(self.raw_att.time, return_index=True)[1])
                except (ValueError, AttributeError):
                    self.logger.error('Unable to read from {}'.format(at))
            else:
                self.logger.error('No attitude paths found')

        if len(navigation_pth) > 1:
            self.logger.error('Only one navigation data store can be read at a time at this time.')
            raise NotImplementedError('Only one navigation data store can be read at a time at this time.')
        elif len(navigation_pth) == 0:
            self.logger.error('No navigation paths found')
        for nv in navigation_pth:
            if self.converted_pth is None:
                self.converted_pth = os.path.dirname(nv)
            if self.client is None:
                skip_dask = True
            else:
                skip_dask = False
            if nv is not None:
                try:
                    self.raw_nav = reload_zarr_records(nv, skip_dask)
                    # this kind of sucks, but for some reason I have yet to figure out, I occassionally get a duplicate entry
                    #    after all the parallel reading and merging in nav.
                    self.raw_nav = self.raw_nav.isel(time=np.unique(self.raw_nav.time, return_index=True)[1])
                except (ValueError, AttributeError):
                    self.logger.error('Unable to read from {}'.format(nv))
            else:
                self.logger.error('No navigation paths found')

    def initialize_log(self):
        """
        Initialize the logger, which writes to logfile, that is made at the root folder housing the converted data
        """

        if self.logfile is None:
            self.logfile = os.path.join(self.converted_pth, 'logfile_{}.txt'.format(datetime.now().strftime('%H%M%S')))
        if self.logger is None:
            self.logger = return_logger(__name__, self.logfile)

    def _batch_read_file_setup(self):
        """
        With given path to folder of kongsberg .all files (self.filfolder), return the paths to the individual .all
        files and the path to the output folder for the netcdf/zarr converted files.  Create this folder, or a similarly
        named folder (with appended timestamp) if it already exists.

        Also initialize the logging module (path to logfile is in self.logfile).
        """

        if type(self.filfolder) == list:
            fils = self.filfolder
            self.filfolder = os.path.dirname(fils[0])
        elif os.path.isdir(self.filfolder):
            fils = glob(os.path.join(self.filfolder, '*.all'))
            if not fils:
                fils = glob(os.path.join(self.filfolder, '*.kmall'))
        elif os.path.isfile(self.filfolder):
            fils = [self.filfolder]
            self.filfolder = os.path.dirname(self.filfolder)
        else:
            raise ValueError('Only directory or file path is supported: {}'.format(self.filfolder))
        if not fils:
            raise ValueError('Directory provided, but no .all or .kmall files found: {}'.format(self.filfolder))

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
        self.fils = fils

        self.logfile = os.path.join(converted_pth, 'logfile_{}.txt'.format(datetime.now().strftime('%H%M%S')))
        self.initialize_log()

    def _batch_read_chunk_generation(self, fils: list):
        """
        For each .all file, determine a good chunksize for the distributed read/processing and build a list with
        files, start bytes and end bytes.

        Parameters
        ----------
        fils
            list of paths to .all files

        Returns
        -------
        list
            list of chunks given as [filepath, starting offset in bytes, end of chunk pointer in bytes]
        """

        chnks = []
        for f in fils:
            finalchunksize = determine_good_chunksize(f, self.convert_minchunksize, self.convert_maxchunks)
            chnks.append(return_chunked_fil(f, 0, finalchunksize))
            self.logger.info('{}: Using {} chunks of size {}'.format(f, self.convert_maxchunks, finalchunksize))

        # chnks_flat is now a list of lists representing chunks of each file
        chnks_flat = [c for subc in chnks for c in subc]
        return chnks_flat

    def _gather_file_level_metadata(self, fils: list):
        """
        Most of xarray_conversion works on chunks of files.  Here we gather all the necessary information that is
        important at a file level, for instance start/end time.  This will be added later as an attribute to the
        final xarray Dataset.

        Parameters
        ----------
        fils
            strings for full file paths to .all files

        Returns
        -------
        dict
            dictionary of file name and start/stop time, e.g. {'test.all': [1562785404.206, 1562786165.287]}
        """

        dat = {}
        for f in fils:
            filname = os.path.split(f)[1]
            if os.path.splitext(f)[1] == '.all':
                dat[filname] = par3.AllRead(f).fast_read_start_end_time()
            else:
                dat[filname] = kmall.kmall(f).fast_read_start_end_time()
        return dat

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
        int
            size of chunks operated on by workers, shortened if greater than the total size
        int
            total length of blocks
        """

        xlens = self.client.gather(self.client.map(_return_xarray_timelength, input_xarrs))
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
        output_arrs = self.client.map(_merge_constant_blocks, balanced_data)
        time_arrs = self.client.gather(self.client.map(_return_xarray_time, output_arrs))
        del balanced_data
        return output_arrs, time_arrs, chunksize, totallength

    def _batch_read_sequential_and_trim(self, chnks_flat: list):
        """
        Kongsberg sonars have dynamic sectors, where the number of beams in each sector varies over time.  As a result,
        we pad the beam arrays to a total size of 400.  At this point though, since we can now learn max beams across
        sectors/time, we want to trim to just the max beam count in each sector.

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
        recfutures = self.client.map(_run_sequential_read, chnks_flat)
        progress(recfutures)
        maxnums = self.client.map(_sequential_gather_max_beams, recfutures)
        maxnum = np.max(self.client.gather(maxnums))
        newrecfutures = self.client.map(_sequential_trim_to_max_beam_number, recfutures, [maxnum] * len(recfutures))
        del recfutures, maxnums, maxnum
        return newrecfutures

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
        trim_arr = self.client.map(_assess_need_for_split_correction, base_xarrfut, next_xarrfut)
        input_xarrs = self.client.map(_correct_for_splits, base_xarrfut, [trim_arr[-1]] + trim_arr[:-1])
        del trim_arr, base_xarrfut, next_xarrfut
        return input_xarrs

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
        mintims = self.client.gather(self.client.map(_return_xarray_mintime, input_xarrs))
        sort_mintims = sorted(mintims)
        if mintims != sort_mintims:
            self.logger.info('Resorting futures to time index: {}'.format(sort_mintims))
            idx = [mintims.index(t) for t in sort_mintims]
            input_xarrs = [input_xarrs[i] for i in idx]
        return input_xarrs

    def _batch_read_return_xarray_by_sector(self, input_xarrs: list, sec: str):
        """
        Take in the sector identifier (sec) and only return xarray objects with that sector in them, also selecting
        only that sector.

        Parameters
        ----------
        input_xarrs
            xarray Dataset objects from _sequential_to_xarray
        sec
            sector identifier

        Returns
        -------
        list
            input_xarrs selected sector, with xarrs dropped if they didn't contain the sector
        """

        list_of_secs = [sec] * len(input_xarrs)
        input_xarrs_by_sec = self.client.map(_divide_xarray_return_sector, input_xarrs, list_of_secs)
        empty_mask = self.client.gather(self.client.map(_divide_xarray_indicate_empty_future, input_xarrs_by_sec))
        valid_input_xarrs = [in_xarr for cnt, in_xarr in enumerate(input_xarrs_by_sec) if empty_mask[cnt]]
        return valid_input_xarrs

    def _batch_read_write(self, output_mode: str, datatype: str, opts: list, converted_pth: str, secid: str = None):
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
        secid
            sector name if writing by sector to zarr datastore

        Returns
        -------
        str
            path to the written data directory
        """

        fpths = ''
        if output_mode == 'netcdf':
            fpths = self._batch_read_write_netcdf(datatype, opts, converted_pth)
        elif output_mode == 'zarr':
            fpths = self._batch_read_write_zarr(datatype, opts, converted_pth, secid=secid)

        if fpths:
            fpthsout = self.client.gather(fpths)
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
        fpths = self.client.map(xarr_to_netcdf, opts[datatype]['output_arrs'], output_pths, output_fnames,
                                output_attributes, fname_idxs)
        return fpths

    def _batch_read_write_zarr(self, datatype: str, opts: dict, converted_pth: str, secid: str = None):
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
        secid
            sector identifier

        Returns
        -------
        Future
            str path to written zarr datastore/group.  I use the first element of the list of fpths as all returned elements of fpths are identical.
        """

        if secid is None:
            output_pth = os.path.join(converted_pth, datatype + '.zarr')
        else:
            output_pth = os.path.join(converted_pth, datatype + '_' + secid + '.zarr')
            opts[datatype]['final_attrs']['sector_identifier'] = secid

        # correct for existing data if it exists in the zarr data store
        data_locs, finalsize = get_write_indices_zarr(output_pth, opts[datatype]['time_arrs'])
        sync = DaskProcessSynchronizer(output_pth)
        fpths = distrib_zarr_write(output_pth, opts[datatype]['output_arrs'], opts[datatype]['final_attrs'],
                                   opts[datatype]['chunks'], data_locs, finalsize, sync, self.client)
        fpth = fpths[0]  # Pick the first element, all are identical so it doesnt really matter
        return fpth

    def batch_read_by_sector(self, output_mode: str = 'zarr'):
        """
        General converter for .all files leveraging xarray and dask.distributed
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

        if self.client is None:
            self.client = dask_find_or_start_client()

        if self.client is not None:
            self._batch_read_file_setup()
            self.logger.info('****Running Kongsberg .all converter****')

            fil_start_end_times = self._gather_file_level_metadata(self.fils)
            chnks_flat = self._batch_read_chunk_generation(self.fils)
            newrecfutures = self._batch_read_sequential_and_trim(chnks_flat)

            # xarrfutures is a list of futures representing xarray structures for each file chunk
            xarrfutures = self.client.map(_sequential_to_xarray, newrecfutures)
            progress(xarrfutures)
            del newrecfutures

            finalpths = {'ping': [], 'attitude': [], 'navigation': []}
            for datatype in ['ping', 'attitude', 'navigation']:
                input_xarrs = self.client.map(_divide_xarray_futs, xarrfutures, [datatype] * len(xarrfutures))
                input_xarrs = self._batch_read_sort_futures_by_time(input_xarrs)
                finalattrs = self.client.gather(self.client.map(gather_dataset_attributes, input_xarrs))
                combattrs = combine_xr_attributes(finalattrs)

                if datatype == 'ping':
                    combattrs['multibeam_files'] = fil_start_end_times  # override with start/end time dict
                    combattrs['output_path'] = self.converted_pth
                    combattrs['_conversion_complete'] = datetime.utcnow().strftime('%c')
                    sectors = self.client.gather(self.client.map(_return_xarray_sectors, input_xarrs))
                    totalsecs = sorted(np.unique([s for secs in sectors for s in secs]))
                    for sec in totalsecs:
                        self.logger.info('Operating on sector {}, s/n {}, freq {}'.format(sec.split('_')[1], sec.split('_')[0],
                                                                                      sec.split('_')[2]))
                        input_xarrs_by_sec = self._batch_read_return_xarray_by_sector(input_xarrs, sec)
                        opts = batch_read_configure_options(self.ping_chunksize, self.nav_chunksize, self.att_chunksize)
                        opts['ping']['final_attrs'] = combattrs
                        if len(input_xarrs_by_sec) > 1:
                            # rebalance to get equal chunksize in time dimension (sector/beams are constant across)
                            input_xarrs_by_sec = self._batch_read_correct_block_boundaries(input_xarrs_by_sec)
                        opts[datatype]['output_arrs'], opts[datatype]['time_arrs'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs_by_sec, datatype, opts[datatype]['chunksize'])
                        del input_xarrs_by_sec
                        finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, self.converted_pth, secid=sec))
                        del opts
                else:
                    opts = batch_read_configure_options(self.ping_chunksize, self.nav_chunksize, self.att_chunksize)
                    opts[datatype]['final_attrs'] = combattrs
                    opts[datatype]['output_arrs'], opts[datatype]['time_arrs'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs, datatype, opts[datatype]['chunksize'])
                    del input_xarrs
                    finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, self.converted_pth))
                    del opts

            endtime = perf_counter()
            self.logger.info('****Distributed conversion complete: {}s****\n'.format(round(endtime - starttime, 1)))

            return finalpths
        return None

    def _return_runtime_and_installation_settings_dicts(self):
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

    def build_offsets(self, save_pths: list = None):
        """
        Form sorteddict for unique entries in installation parameters across all lines, retaining the xyzrph for each
        transducer/receiver.  key values depend on type of sonar, see sonar_translator

        Modifies the xyzrph attribute with timestamp dictionary of entries

        Parameters
        ----------
        save_pths
            a list of paths to zarr datastores for writing the xyzrph attribute to if provided
        """

        settdict, runtimesettdict = self._return_runtime_and_installation_settings_dicts()

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

            mintime = min(list(settdict.keys()))
            minactual = self.raw_ping[0].time.min().compute()
            if float(mintime) > float(minactual):
                self.logger.warning('Installation Parameters minimum time: {}'.format(mintime))
                self.logger.warning('Actual data minimum time: {}'.format(float(minactual)))
                self.logger.warning('First Installation Parameters does not cover the start of the dataset.' +
                                    '  Extending from nearest entry...')
                settdict[str(int(minactual))] = settdict.pop(mintime)

            # translate over the offsets/angles for the transducers following the sonar_translator scheme
            self.sonartype = snrmodels[0]
            self.xyzrph = build_xyzrph(settdict, self.sonartype)
            self.tpu_parameters = build_tpu_parameters()

            if save_pths is not None:
                for pth in save_pths:
                    my_xarr_add_attribute({'xyzrph': self.xyzrph, 'tpu_parameters': self.tpu_parameters,
                                           'sonartype': self.sonartype}, pth)
            self.logger.info('Constructed offsets successfully')

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
        if self.sonartype in ['em2040', 'em122', 'em710', 'em2045', 'em2040p']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['tx_r'].keys()), time_idx)))
            return {corr_timestmp: {'tx_roll': float(self.xyzrph['tx_r'][corr_timestmp]),
                                    'tx_pitch': float(self.xyzrph['tx_p'][corr_timestmp]),
                                    'tx_heading': float(self.xyzrph['tx_h'][corr_timestmp]),
                                    'tx_x': float(self.xyzrph['tx_x'][corr_timestmp]),
                                    'tx_y': float(self.xyzrph['tx_y'][corr_timestmp]),
                                    'tx_z': float(self.xyzrph['tx_z'][corr_timestmp])}}

        elif self.sonartype in ['em2040_dual_rx', 'em2040_dual_tx']:
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
        if self.sonartype in ['em2040', 'em122', 'em710', 'em2045', 'em2040p']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['rx_r'].keys()), time_idx)))
            return {corr_timestmp: {'rx_roll': float(self.xyzrph['rx_r'][corr_timestmp]),
                                    'rx_pitch': float(self.xyzrph['rx_p'][corr_timestmp]),
                                    'rx_heading': float(self.xyzrph['rx_h'][corr_timestmp]),
                                    'rx_x': float(self.xyzrph['rx_x'][corr_timestmp]),
                                    'rx_y': float(self.xyzrph['rx_y'][corr_timestmp]),
                                    'rx_z': float(self.xyzrph['rx_z'][corr_timestmp])}}

        elif self.sonartype in ['em2040_dual_rx', 'em2040_dual_tx']:
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
            self.logger.error('No settings attributes found, possibly no install params in .all files')
            raise ValueError('No settings attributes found, possibly no install params in .all files')

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

    def return_active_sectors_for_ping_counter(self, rangeangle_idx: int, ping_counters: Union[int, np.array]):
        """
        Take in the ping counter number(s) and return all the times the ping counter exists in the given rangeangle

        If by_counter is True, will return array of the same size as the ping_counters input (i.e. all the active
        times for this sector for each ping counter value)

        If by_counter is False, will return array of the same size as the ping_times input (i.e. all the active
        times for this sector for each ping time value)

        Parameters
        ----------
        rangeangle_idx
            index to select the range angle dataset
        ping_counters
            ping counter number(s)

        Returns
        -------
        np.array
            size equal to given ping_counters array, with elements filled with ping time if that ping counter/ping time is in the given range angle dataset
        """

        # sector we are examining
        ra = self.raw_ping[rangeangle_idx]
        # msk = np.logical_and(ping_times.min() <= ra.time, ra.time <= ping_times.max())
        # ra = ra.where(msk, drop=True)

        # boolean mask of which ping counter values are in the provided ping counters
        ra_cntr_msk = np.isin(ra.counter, ping_counters)
        # boolean mask of which provided ping counters are in the ping counter values
        p_cntr_msk = np.isin(ping_counters, ra.counter)

        ans = np.zeros_like(ping_counters, dtype=np.float64)

        ans[p_cntr_msk] = ra.time[ra_cntr_msk].values
        return ans

    def return_all_profiles(self):
        """
        Return dict of attribute_name/data for each sv profile in the ping dataset

        attribute name is always 'profile_timestamp' format, ex: 'profile_1503411780'

        Returns
        -------
        dict
            dictionary of attribute_name/data for each sv profile
        """

        prof_keys = [x for x in self.raw_ping[0].attrs.keys() if x[0:7] == 'profile']
        if prof_keys:
            return {p: self.raw_ping[0].attrs[p] for p in prof_keys}
        else:
            return {}

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

    def return_xyz_prefixes_for_sectors(self):
        """
        self.raw_ping contains Datasets broken up by sector.  This method will return the prefixes you need to get
        the offsets/angles from self.xyzrph depending on model/dual-head

        Returns
        -------
        List
            list of two element lists containing the prefixes needed for tx/rx offsets and angles
        """

        if 'tx_r' in self.xyzrph:
            leverarms = [['tx', 'rx']]
        elif 'tx_port_r' in self.xyzrph:
            leverarms = [['tx_port', 'rx_port'], ['tx_stbd', 'rx_stbd']]
        else:
            self.logger.error('Not supporting this sonartype yet.')
            raise NotImplementedError('Not supporting this sonartype yet.')

        lever_prefix = []
        for ra in self.raw_ping:
            serial_ident = ra.sector_identifier.split('_')[0]
            # dual head logic
            if len(leverarms) > 1:
                if serial_ident == str(ra.system_serial_number[0]):
                    lever_prefix.append(leverarms[0])
                elif serial_ident == str(ra.secondary_system_serial_number[0]):
                    lever_prefix.append(leverarms[1])
                else:
                    self.logger.error('Found serial number attribute not included in sector')
                    raise NotImplementedError('Found serial number attribute not included in sector')
            else:
                lever_prefix.append(leverarms[0])
        return lever_prefix

    def select_array_from_rangeangle(self, ra_var: str, ra_sector: str):
        """
        Given variable name and sectors, return DataArray specified by var reduced to dimensions specified by sectors.

        Using the ntx array as a key (identifies sectors/freq that are pinging at that time) returned array will have
        NaN for all unused sector/freq combinations

        Parameters
        ----------
        ra_var
            variable name to identify xarray DataArray to return
        ra_sector
            string name for sectors to subset DataArray by

        Returns
        -------
        xr.DataArray
            dataarray corresponding to the given variable and sector
        """

        for ra in self.raw_ping:
            if ra.sector_identifier == ra_sector:
                sec_dat = ra[ra_var].where(ra.ntx > 0).astype(np.float32)
                if sec_dat.ndim == 2:
                    # for (time, beam) dimensional arrays, also replace 999.0 in the beam dimension with nan
                    #    to make downstream calculations a bit easier (see pad_to_dense)
                    return sec_dat.where(sec_dat != 999.0)
                else:
                    return sec_dat
        return None

    def return_ping_by_sector(self, secid: str):
        """
        Given sector identifier (secid), return the correct ping Dataset from the list

        Parameters
        ----------
        secid
            name of the sector you want

        Returns
        -------
        xr.Dataset
            Dataset for the right sector
        """

        for ra in self.raw_ping:
            if ra.sector_identifier == secid:
                return ra
        return None

    def return_sector_time_indexed_array(self, subset_time: list = None):
        """
        Most of the processing involves matching static, timestamped offsets or angles to time series data.  Given that
        we might have a list of sectors and a list of timestamped offsets, need to iterate through all of this in each
        processing loop.  Sectors/timestamps length should be minimal, so we just loop in python.

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process

        Returns
        -------
        list
            list of indices for each sector that are within the provided subset
        """

        resulting_sectors = []
        prefixes = self.return_xyz_prefixes_for_sectors()
        for cnt, ra in enumerate(self.raw_ping):
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
            resulting_sectors.append(resulting_tstmps)
        return resulting_sectors

    def get_minmax_extents(self):
        """
        Build dataset geographic extents
        """

        maxlat = self.raw_nav.latitude.max().compute()
        maxlon = self.raw_nav.longitude.max().compute()
        minlat = self.raw_nav.latitude.min().compute()
        minlon = self.raw_nav.longitude.min().compute()
        print('Max Lat/Lon: {}/{}'.format(maxlat, maxlon))
        print('Min Lat/Lon: {}/{}'.format(minlat, minlon))
        self.extents = [maxlat, maxlon, minlat, minlon]

    def return_utm_zone_number(self):
        """
        Compute the minimum/maximum longitude values and returns the utm zone number

        Returns
        -------
        zne: int, zone number, e.g. 19 for UTM Zone 19
        """

        minlon = float(self.raw_nav.longitude.min().values)
        maxlon = float(self.raw_nav.longitude.max().values)
        minlat = float(self.raw_nav.latitude.min().values)
        zne = return_zone_from_min_max_long(minlon, maxlon, minlat)
        return zne

    def show_description(self):
        """
        Display xarray Dataset description
        """

        print(self.raw_ping[0].info)

    def generate_plots(self):
        """
        | Generate some example plots showing the abilities of the xarray plotting engine
        | - plot detection info for beams/sector/times
        | - plot roll/pitch/heave on 3 row plot
        """

        self.raw_ping.detectioninfo.plot(x='beam', y='time', col='sector', col_wrap=3)

        fig, axes = plt.subplots(nrows=3)
        self.raw_ping['roll'].plot(ax=axes[0])
        self.raw_ping['pitch'].plot(ax=axes[1])
        self.raw_ping['heading'].plot(ax=axes[2])


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
        # really small files can be below the given minchunksize in size.  Just use 4 chunks per in this case
        finalchunksize = int(filesize / 4)
        max_chunks = 4
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


def build_tpu_parameters():
    """
    Generate default tpu parameters based on NOAA setup.  Assumes POS MV, vessel surveys as done by NOAA, patch test
    values commonly seen, etc.

    Returns
    -------
    dict
        keys are parameter names, vals are dicts with scalars for static uncertainty values
    """
    # generate default tpu parameters
    heave = 0.05  # 1 sigma standard deviation for the heave data (meters)
    roll_sensor_error = 0.0005  # 1 sigma standard deviation in the roll sensor (degrees)
    pitch_sensor_error = 0.0005  # 1 sigma standard deviation in the pitch sensor (degrees)
    heading_sensor_error = 0.02  # 1 sigma standard deviation in the pitch sensor (degrees)
    x_offset = 0.2  # 1 sigma standard deviation in your measurement of x lever arm (meters)
    y_offset = 0.2  # 1 sigma standard deviation in your measurement of y lever arm (meters)
    z_offset = 0.2  # 1 sigma standard deviation in your measurement of z lever arm (meters)
    surface_sv = 0.5  # 1 sigma standard deviation in surface sv sensor (meters/second)
    roll_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
    pitch_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
    heading_patch = 0.5  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
    latency_patch = 0.0  # 1 sigma standard deviation in your latency calculation (seconds)
    timing_latency = 0.001  # 1 sigma standard deviation of the timing accuracy of the system (seconds)
    dynamic_draft = 0.1  # 1 sigma standard deviation of the dynamic draft measurement (meters)
    separation_model = 0.1  # 1 sigma standard deivation in the sep model (tidal, ellipsoidal, etc) (meters)
    waterline = 0.02  # 1 sigma standard deviation of the waterline (meters)
    vessel_speed = 0.1  # 1 sigma standard deviation of the vessel speed (meters/second)
    horizontal_positioning = 1.5  # 1 sigma standard deviation of the horizontal positioning (meters)
    vertical_positioning = 0.8  # 1 sigma standard deviation of the horizontal positioning (meters)

    tpu_parameters = {'heave': heave, 'roll_sensor_error': roll_sensor_error, 'pitch_sensor_error': pitch_sensor_error,
                      'heading_sensor_error': heading_sensor_error, 'x_offset': x_offset, 'y_offset': y_offset,
                      'z_offset': z_offset, 'surface_sv': surface_sv, 'roll_patch': roll_patch,
                      'pitch_patch': pitch_patch, 'heading_patch': heading_patch, 'latency_patch': latency_patch,
                      'timing_latency': timing_latency, 'dynamic_draft': dynamic_draft,
                      'separation_model': separation_model, 'waterline': waterline, 'vessel_speed': vessel_speed,
                      'horizontal_positioning': horizontal_positioning, 'vertical_positioning': vertical_positioning}
    return tpu_parameters


def build_xyzrph(settdict: dict, sonartype: str):
    """
    Translate the raw settings dictionary from the multibeam file (see sequential_read_records) into a dictionary of
    timestamped entries for each sensor offset/angle.  Sector based phase center differences are included as well.

    Parameters
    ----------
    settdict
        keys are unix timestamps, vals are json dumps containing key/record for each system
    sonartype
        sonar identifer

    Returns
    -------
    dict
        keys are translated entries, vals are dicts with timestamps:values
    """

    xyzrph = {}
    for tme in settdict:
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
                xyzrph[tme][tx_ident + '_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                xyzrph[tme][tx_ident + '_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                xyzrph[tme][tx_ident + '_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                xyzrph[tme][tx_ident + '_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                xyzrph[tme][tx_ident + '_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                xyzrph[tme][tx_ident + '_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]
                xyzrph[tme][rx_ident + '_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                xyzrph[tme][rx_ident + '_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                xyzrph[tme][rx_ident + '_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]
                try:  # kmall workflow, rx offset is tacked on to the trans1 record
                    xyzrph[tme][rx_ident + '_x'] = str(float(settdict[tme]['transducer_{}_along_location'.format(ky)]) +\
                                                       float(settdict[tme]['transducer_{}_rx_forward'.format(ky)]))
                    xyzrph[tme][rx_ident + '_y'] = str(float(settdict[tme]['transducer_{}_athwart_location'.format(ky)]) +\
                                                       float(settdict[tme]['transducer_{}_rx_starboard'.format(ky)]))
                    xyzrph[tme][rx_ident + '_z'] = str(float(settdict[tme]['transducer_{}_vertical_location'.format(ky)]) +\
                                                       float(settdict[tme]['transducer_{}_rx_down'.format(ky)]))
                except KeyError:
                    xyzrph[tme][rx_ident + '_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                    xyzrph[tme][rx_ident + '_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                    xyzrph[tme][rx_ident + '_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
            else:
                xyzrph[tme][val + '_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                xyzrph[tme][val + '_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                xyzrph[tme][val + '_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                xyzrph[tme][val + '_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                xyzrph[tme][val + '_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                xyzrph[tme][val + '_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]

        # additional offsets based on sector
        if sonartype in install_parameter_modifier:
            for val in [v for v in install_parameter_modifier[sonartype] if v is not None]:
                for sec in install_parameter_modifier[sonartype][val]:
                    xyzrph[tme][val + '_x_' + sec] = str(install_parameter_modifier[sonartype][val][sec]['x'])
                    xyzrph[tme][val + '_y_' + sec] = str(install_parameter_modifier[sonartype][val][sec]['y'])
                    xyzrph[tme][val + '_z_' + sec] = str(install_parameter_modifier[sonartype][val][sec]['z'])

        # translate over the positioning sensor stuff using the installation parameters active identifiers
        pos_ident = settdict[tme]['active_position_system_number']  # 'position_1'
        for suffix in [['_vertical_location', '_z'], ['_along_location', '_x'],
                       ['_athwart_location', '_y'], ['_time_delay', '_latency']]:
            qry = pos_ident + suffix[0]
            xyzrph[tme]['imu' + suffix[1]] = settdict[tme][qry]

        # do the same over motion sensor (which is still the POSMV), make assumption that its one of the motion
        #   entries
        pos_motion_ident = settdict[tme]['active_heading_sensor'].split('_')
        pos_motion_ident = pos_motion_ident[0] + '_sensor_' + pos_motion_ident[1]  # 'motion_1_com2'

        # for suffix in [['_vertical_location', '_motionz'], ['_along_location', '_motionx'],
        #                ['_athwart_location', '_motiony'], ['_time_delay', '_motionlatency'],
        #                ['_roll_angle', '_r'], ['_pitch_angle', '_p'], ['_heading_angle', '_h']]:
        for suffix in [['_roll_angle', '_r'], ['_pitch_angle', '_p'], ['_heading_angle', '_h']]:
            qry = pos_motion_ident + suffix[0]
            xyzrph[tme]['imu' + suffix[1]] = settdict[tme][qry]

        # include blank entry for primary gps antenna
        xyzrph[tme]['antenna_x'] = '0.000'
        xyzrph[tme]['antenna_y'] = '0.000'
        xyzrph[tme]['antenna_z'] = '0.000'

        # include waterline if it exists
        if 'waterline_vertical_location' in settdict[tme]:
            xyzrph[tme]['waterline'] = settdict[tme]['waterline_vertical_location']

    # generate dict of ordereddicts for fast searching
    newdict = {}
    for ky in xyzrph:
        for stmp in xyzrph[ky].keys():
            if stmp not in newdict:
                newdict[stmp] = SortedDict()
            newdict[stmp][ky] = xyzrph[ky][stmp]
    xyzrph = SortedDict(newdict)

    return xyzrph


def return_xyzrph_from_mbes(mbesfil: str):
    """
    Currently being used to load Vessel View with the first installation parameters from a multibeam file.  This will
    take the first installation record in the multibeam file and convert it over to the xyzrph format used by kluster.

    xyzrph is a dict of offsets/angles (ex: 'tx_x') and values organized by the time of the installation record
    (ex: SortedDict({'1583306608': '0.000'})).

    Parameters
    ----------
    mbesfil
        path to a multibeam file

    Returns
    -------
    dict
        translated installation parameter record in the format used by Kluster

    """
    if os.path.splitext(mbesfil)[1] == '.all':
        mbes_object = par3.AllRead(mbesfil)
    else:
        mbes_object = kmall.kmall(mbesfil)

    recs = mbes_object.sequential_read_records(first_installation_rec=True)
    try:
        settings_dict = {str(int(recs['installation_params']['time'][0])): recs['installation_params']['installation_settings'][0]}
        snrmodels = np.unique([settings_dict[x]['sonar_model_number'] for x in settings_dict])
        if len(snrmodels) > 1:
            raise NotImplementedError('Found multiple sonars types in data provided: {}'.format(snrmodels))
        if snrmodels[0] not in sonar_translator:
            raise NotImplementedError('Sonar model not understood "{}"'.format(snrmodels[0]))

        # translate over the offsets/angles for the transducers following the sonar_translator scheme
        sonartype = snrmodels[0]
        xyzrph = build_xyzrph(settings_dict, sonartype)

        return xyzrph
    except IndexError:
        print('Unable to read from {}: data not found for installation records'.format(mbesfil))
        print(recs['installation_params'])


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

    pcs = PCSio.PCSFile(posfile, nCache=0)
    try:
        pcs.CacheHeaders(read_first_msg=(20, '$MSG'))
        msg20 = pcs.GetArray("$MSG", 20)
        data = {'antenna_x': round(msg20[0][10], 3), 'antenna_y': round(msg20[0][11], 3), 'antenna_z': round(msg20[0][12], 3),
                'imu_h': round(msg20[0][21], 3), 'imu_p': round(msg20[0][20], 3), 'imu_r': round(msg20[0][19], 3),
                'imu_x': round(msg20[0][7], 3), 'imu_y': round(msg20[0][8], 3), 'imu_z': round(msg20[0][9], 3)}
        return data
    except KeyError:
        try:
            print('Unable to read from {}: message 20 not found'.format(posfile))
            print('Found {}'.format(list(pcs.sensorHeaders.keys())))
        except:
            print('Unable to read from file: {}'.format(posfile))
    return None
