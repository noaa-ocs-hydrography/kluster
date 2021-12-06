import os
import numpy as np
from glob import glob
from typing import Union
from pyproj import CRS
from pyproj.exceptions import CRSError

from HSTB.kluster import kluster_variables


def build_crs(zone_num: str = None, datum: str = None, epsg: str = None, projected: bool = True):
    horizontal_crs = None
    if epsg:
        try:
            horizontal_crs = CRS.from_epsg(int(epsg))
        except CRSError:  # if the CRS we generate here has no epsg, when we save it to disk we save the proj string
            horizontal_crs = CRS.from_string(epsg)
    elif not epsg and not projected:
        datum = datum.upper()
        if datum == 'NAD83':
            horizontal_crs = CRS.from_epsg(epsg_determinator('nad83(2011)'))
        elif datum == 'NAD83 PA11':
            horizontal_crs = CRS.from_epsg(epsg_determinator('nad83(pa11)'))
        elif datum == 'NAD83 MA11':
            horizontal_crs = CRS.from_epsg(epsg_determinator('nad83(ma11)'))
        elif datum == 'WGS84':
            horizontal_crs = CRS.from_epsg(epsg_determinator('wgs84'))
        else:
            err = 'ERROR: {} not supported.  Only supports WGS84 and NAD83'.format(datum)
            return horizontal_crs, err
    elif not epsg and projected:
        datum = datum.upper()
        zone = zone_num  # this will be the zone and hemi concatenated, '10N'
        try:
            zone, hemi = int(zone[:-1]), str(zone[-1:])
        except:
            err = 'ERROR: found invalid projected zone/hemisphere identifier: {}, expected something like "10N"'.format(zone)
            return horizontal_crs, err
        if datum == 'NAD83':
            try:
                myepsg = epsg_determinator('nad83(2011)', zone=zone, hemisphere=hemi)
            except:
                err = 'ERROR: unable to determine epsg for NAD83(2011), zone={}, hemisphere={}, out of bounds?'.format(zone, hemi)
                return horizontal_crs, err
            horizontal_crs = CRS.from_epsg(myepsg)
        elif datum == 'NAD83 PA11':
            try:
                myepsg = epsg_determinator('nad83(pa11)', zone=zone, hemisphere=hemi)
            except:
                err = 'ERROR: unable to determine epsg for NAD83 PA11, zone={}, hemisphere={}, out of bounds?'.format(zone, hemi)
                return horizontal_crs, err
            horizontal_crs = CRS.from_epsg(myepsg)
        if datum == 'NAD83 MA11':
            try:
                myepsg = epsg_determinator('nad83(ma11)', zone=zone, hemisphere=hemi)
            except:
                err = 'ERROR: unable to determine epsg for NAD83 MA11, zone={}, hemisphere={}, out of bounds?'.format(zone, hemi)
                return horizontal_crs, err
            horizontal_crs = CRS.from_epsg(myepsg)
        elif datum == 'WGS84':
            try:
                myepsg = epsg_determinator('wgs84', zone=zone, hemisphere=hemi)
            except:
                err = 'ERROR: unable to determine epsg for WGS84, zone={}, hemisphere={}, out of bounds?'.format(zone, hemi)
                return horizontal_crs, err
            horizontal_crs = CRS.from_epsg(myepsg)
        else:
            err = 'ERROR: {} not supported.  Only supports WGS84 and NAD83'.format(datum)
            return horizontal_crs, err
    return horizontal_crs, ''


def epsg_determinator(datum: str, zone: int = None, hemisphere: str = None):
    """
    Take in a datum identifer and optional zone/hemi for projected and return an epsg code

    Parameters
    ----------
    datum
        datum identifier string, one of nad83(2011), nad83(pa11), nad83(ma11), wgs84 supported for now
    zone
        integer utm zone number
    hemisphere
        hemisphere identifier, "n" for north, "s" for south

    Returns
    -------
    int
        epsg code
    """

    try:
        datum = datum.lower()
    except:
        raise ValueError('epsg_determinator: {} is not a valid datum string, expected "nad83(2011)" or "wgs84"')

    if zone is None and hemisphere is not None:
        raise ValueError('epsg_determinator: zone is required for projected epsg determination')
    if zone is not None and hemisphere is None:
        raise ValueError('epsg_determinator: hemisphere is required for projected epsg determination')
    if datum not in ['nad83(2011)', 'wgs84']:
        raise ValueError('epsg_determinator: {} not supported'.format(datum))

    if zone is None and hemisphere is None:
        if datum in ['nad83(2011)', 'nad83(pa11)', 'nad83(ma11)']:  # using the 3d geodetic NAD83(2011)
            return kluster_variables.epsg_nad83
        elif datum == 'wgs84':  # using the 3d geodetic WGS84/ITRF2008
            return kluster_variables.epsg_wgs84
    else:
        hemisphere = hemisphere.lower()
        if datum == 'nad83(2011)':
            if hemisphere == 'n':
                if zone <= 19:
                    return 6329 + zone
                elif zone == 59:
                    return 6328
                elif zone == 60:
                    return 6329
        elif datum == 'nad83(pa11)':
            if zone in [4, 5] and hemisphere == 'n':
                return 6630 + zone
            elif zone == 2 and hemisphere == 's':
                return 6636
        elif datum == 'nad83(ma11)':
            if zone == 54 and hemisphere == 'n':
                return 8692
            elif zone == 55 and hemisphere == 's':
                return 8693
        elif datum == 'wgs84':
            if hemisphere == 's':
                return 32700 + zone
            elif hemisphere == 'n':
                return 32600 + zone
    raise ValueError('epsg_determinator: no valid epsg for datum={} zone={} hemisphere={}'.format(datum, zone, hemisphere))


def return_files_from_path(pth: str, in_chunks: bool = True):
    """
    Input files can be entered into an xarray_conversion.BatchRead instance as either a list, a path to a directory
    of multibeam files or as a path to a single file.  Here we return all the files in each of these scenarios as a list
    for the gui to display or to be analyzed in some other way

    Provide an optional fileext argument if you want to specify files with a different extension

    Parameters
    ----------
    pth
        either a list of files, a string path to a directory or a string path to a file
    in_chunks
        if True, returns lists of lists of size kluster_variables.converted_files_at_once

    Returns
    -------
    list
        list of files found
    """

    fils = None
    if type(pth) == list:
        fils = pth
    elif os.path.isdir(pth):
        for fext in kluster_variables.supported_multibeam:
            fils = glob(os.path.join(pth, '*{}'.format(fext)))
            if fils:
                break
    elif os.path.isfile(pth):
        fils = [pth]
    else:
        raise ValueError('_chunks_of_files: Expected either a multibeam file, a list of multibeam files or a directory')
    if not fils:
        return []

    if not in_chunks:
        return fils
    else:
        maxchunks = kluster_variables.converted_files_at_once
        final_fils = [fils[i * maxchunks:(i + 1) * maxchunks] for i in range(int(np.ceil((len(fils) + maxchunks - 1) / maxchunks)))]
        if final_fils[-1] == []:
            final_fils = final_fils[:-1]
        return final_fils


def return_directory_from_data(data: Union[list, str]):
    """
    Given either a path to a zarr store, a path to a directory of multibeam files, a list of paths to multibeam files
    or a path to a single multibeam file, return the parent directory.

    Parameters
    ----------
    data
        a path to a zarr store, a path to a directory of multibeam files, a list of paths to multibeam files or a path
        to a single multibeam file.

    Returns
    -------
    output_directory: str, path to output directory
    """

    try:  # they provided a path to a zarr store or a path to a directory of .all files or a single .all file
        output_directory = os.path.dirname(data)
    except TypeError:  # they provided a list of files
        output_directory = os.path.dirname(data[0])
    return output_directory


def seconds_to_formatted_string(seconds: Union[float, int]):
    """
    Get a nicely formatted time elapsed string

    Parameters
    ----------
    seconds
        number of seconds in either float or int, will be rounded to int

    Returns
    -------
    str
        formatted string
    """

    if seconds < 0:
        return '0 seconds'
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f'{h} hours, {m} minutes, {int(s)} seconds'
    elif m:
        return f'{m} minutes, {int(s)} seconds'
    else:
        return f'{int(s)} seconds'


def haversine(lon1: Union[float, int, np.ndarray], lat1: Union[float, int, np.ndarray],
              lon2: Union[float, int, np.ndarray], lat2: Union[float, int, np.ndarray]):
    """
    Calculate the great circle distance in kilometers between two points on the earth (specified in decimal degrees).
    Can take numpy arrays as inputs, doing a vectorized calculation of multiple points.

    Parameters
    ----------
    lon1
        longitude in degrees of position one
    lat1
        latitude in degrees of position one
    lon2
        longitude in degrees of position two
    lat2
        latitude in degrees of position two
    """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = np.deg2rad(lon1), np.deg2rad(lat1), np.deg2rad(lon2), np.deg2rad(lat2)

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r
