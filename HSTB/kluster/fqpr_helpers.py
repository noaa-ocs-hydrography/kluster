import os
from typing import Union


def epsg_determinator(datum: str, zone: int = None, hemisphere: str = None):
    """
    Take in a datum identifer and optional zone/hemi for projected and return an epsg code

    Parameters
    ----------
    datum
        datum identifier string, one of nad83(2011), wgs84 supported for now
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
        if datum == 'nad83(2011)':  # using the 3d geodetic NAD83(2011)
            return 6319
        elif datum == 'wgs84':  # using the 3d geodetic WGS84/ITRF2008
            return 7911
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
        elif datum == 'wgs84':
            if hemisphere == 's':
                return 32700 + zone
            elif hemisphere == 'n':
                return 32600 + zone
    raise ValueError('epsg_determinator: no valid epsg for datum={} zone={} hemisphere={}'.format(datum, zone, hemisphere))


def return_files_from_path(pth: str, file_ext: str = '.all'):
    """
    Input files can be entered into an xarray_conversion.BatchRead instance as either a list, a path to a directory
    of multibeam files or as a path to a single file.  Here we return all the files in each of these scenarios as a list
    for the gui to display or to be analyzed in some other way

    Provide an optional fileext argument if you want to specify files with a different extension

    Parameters
    ----------
    pth
        either a list of files, a string path to a directory or a string path to a file
    file_ext
        file extension of the file(s) you are looking for

    Returns
    -------
    list
        list of files found
    """

    if type(pth) == list:
        if len(pth) == 1 and os.path.isdir(pth[0]):  # a list one element long that is a path to a directory
            return [os.path.join(pth[0], p) for p in os.listdir(pth[0]) if os.path.splitext(p)[1] == file_ext]
        else:
            return [p for p in pth if os.path.splitext(p)[1] == file_ext]
    elif os.path.isdir(pth):
        return [os.path.join(pth, p) for p in os.listdir(pth) if os.path.splitext(p)[1] == file_ext]
    elif os.path.isfile(pth):
        if os.path.splitext(pth)[1] == file_ext:
            return [pth]
        else:
            return []
    else:
        return []


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
