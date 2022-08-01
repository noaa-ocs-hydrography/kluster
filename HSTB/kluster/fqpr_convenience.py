import os, csv
from time import perf_counter
import xarray as xr
import numpy as np
from dask.distributed import Client
from typing import Union
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt
from datetime import datetime
import laspy
from pyproj import CRS, Transformer

from HSTB.kluster.modules.backscatter import generate_avg_corrector, avg_correct
from HSTB.kluster.fqpr_drivers import return_xyz_from_multibeam
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.fqpr_helpers import seconds_to_formatted_string, return_files_from_path, epsg_determinator
from HSTB.kluster.dask_helpers import dask_find_or_start_client
from HSTB.kluster.logging_conf import return_log_name
from HSTB.kluster.dms import return_zone_from_min_max_long
from HSTB.kluster import kluster_variables

from bathygrid.convenience import create_grid, load_grid, BathyGrid
from bathycube.numba_cube import compile_now


def perform_all_processing(filname: Union[str, list], navfiles: list = None, input_datum: Union[str, int] = None,
                           outfold: str = None, coord_system: str = 'WGS84',
                           vert_ref: str = 'waterline', orientation_initial_interpolation: bool = False,
                           add_cast_files: Union[str, list] = None,
                           skip_dask: bool = False, show_progress: bool = True, parallel_write: bool = True,
                           vdatum_directory: str = None, cast_selection_method: str = 'nearest_in_time', **kwargs):
    """
    Use fqpr_generation to process multibeam data on the local cluster and generate a sound velocity corrected,
    georeferenced xyz with uncertainty in csv files in the provided output folder.

    This is convert_multibeam, process_multibeam, and the import processes all combined into one function.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    filname
        either a list of .all file paths, a single .all file path or a path to a directory with .all files
    navfiles
        list of postprocessed navigation file paths.  If provided, expects either a log file or
        weekstart_year/weekstart_week/override_datum arguments, see import_navigation
    input_datum
        Optional, the basic input datum of the converted multibeam data, should either be nad83, wgs84 or a epsg integer code
        for a geographic coordinate reference system.  This will be used in georeferencing with ellipsoidally based
        vertical reference systems.  If None, will use the encoded string in the multibeam data.
    outfold
        full file path to a directory you want to contain all the zarr folders.  Will create this folder
        if it does not exist.
    coord_system
        a valid datum identifier that pyproj CRS will accept
    vert_ref
        the vertical reference point, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW', 'Aviso MLLW']
    orientation_initial_interpolation
        see process_multibeam
    add_cast_files
        see process_multibeam
    skip_dask
        if True, will not start/find the dask client.  Useful for small datasets where parallel processing actually
        makes the process slower
    show_progress
        If true, uses dask.distributed.progress.
    parallel_write
        if True, will write in parallel to disk, Disable for permissions issues troubleshooting.
    vdatum_directory
        if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
    cast_selection_method
        the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
        'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']

    Returns
    -------
    Fqpr
        Fqpr object containing processed data
    """

    fqpr_inst = convert_multibeam(filname, input_datum=input_datum, outfold=outfold, skip_dask=skip_dask,
                                  show_progress=show_progress, parallel_write=parallel_write)
    if fqpr_inst is not None:
        if navfiles is not None:
            fqpr_inst = import_processed_navigation(fqpr_inst, navfiles, **kwargs)
        fqpr_inst = process_multibeam(fqpr_inst, add_cast_files=add_cast_files, coord_system=coord_system, vert_ref=vert_ref,
                                      orientation_initial_interpolation=orientation_initial_interpolation,
                                      vdatum_directory=vdatum_directory, cast_selection_method=cast_selection_method)
    return fqpr_inst


def convert_multibeam(filname: Union[str, list], input_datum: Union[str, int] = None, outfold: str = None,
                      client: Client = None, skip_dask: bool = False, show_progress: bool = True, parallel_write: bool = True):
    """
    Use fqpr_generation to process multibeam data on the local cluster and generate a new Fqpr instance saved to the
    provided output folder.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    filname
        either a list of .all file paths, a single .all file path or a path to a directory with .all files
    input_datum
        Optional, the basic input datum of the converted multibeam data, should either be nad83, wgs84 or a epsg integer code
        for a geographic coordinate reference system.  This will be used in georeferencing with ellipsoidally based
        vertical reference systems.  If None, will use the encoded string in the multibeam data.
    outfold
        full file path to a directory you want to contain all the zarr folders.  Will create this folder if it does
        not exist.  If not provided will automatically create folder next to lines.
    client
        if you have already created a Client, pass it in here to use it
    skip_dask
        if True, will not start/find the dask client.  Useful for small datasets where parallel processing actually
        makes the process slower
    show_progress
        If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text
    parallel_write
        if True, will write in parallel to disk.  Disable for permissions issues troubleshooting.

    Returns
    -------
    Fqpr
        Fqpr containing converted source data
    """

    fqpr_inst = None
    mfiles = return_files_from_path(filname, in_chunks=True)
    for filchunk in mfiles:
        mbes_read = BatchRead(filchunk, dest=outfold, client=client, skip_dask=skip_dask, show_progress=show_progress,
                              parallel_write=parallel_write)
        fqpr_inst = Fqpr(mbes_read, show_progress=show_progress, parallel_write=parallel_write)
        fqpr_inst.read_from_source(build_offsets=False)
    if fqpr_inst is not None:
        fqpr_inst.multibeam.build_offsets(save_pths=fqpr_inst.multibeam.final_paths['ping'])  # write offsets to ping rootgroup
        fqpr_inst.multibeam.build_additional_line_metadata(save_pths=fqpr_inst.multibeam.final_paths['ping'])
        if input_datum:
            fqpr_inst.input_datum = input_datum
    return fqpr_inst


def import_processed_navigation(fqpr_inst: Fqpr, navfiles: list, errorfiles: list = None, logfiles: list = None,
                                weekstart_year: int = None, weekstart_week: int = None, override_datum: str = None,
                                override_grid: str = None, override_zone: str = None, override_ellipsoid: str = None,
                                max_gap_length: float = 1.0, overwrite: bool = False):
    """
    Convenience function for importing post processed navigation from sbet/smrmsg files, for use in georeferencing
    xyz data.  Converted attitude must exist before importing navigation, timestamps are used to figure out what
    part of the new nav to keep.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    fqpr_inst
        Fqpr instance containing converted data (converted data must exist for the import to work)
    navfiles:
        list of postprocessed navigation file paths
    errorfiles
        list of postprocessed error file paths.  If provided, must be same number as nav files
    logfiles
        list of export log file paths associated with navfiles.  If provided, must be same number as nav files
    weekstart_year
        if you aren't providing a logfile, must provide the year of the sbet here
    weekstart_week
        if you aren't providing a logfile, must provide the week of the sbet here
    override_datum
        provide a string datum identifier if you want to override what is read from the log or you don't have a log,
        ex: 'NAD83 (2011)'
    override_grid
        provide a string grid identifier if you want to override what is read from the log or you don't have a log,
        ex: 'Universal Transverse Mercator'
    override_zone
        provide a string zone identifier if you want to override what is read from the log or you don't have a log,
        ex: 'UTM North 20 (66W to 60W)'
    override_ellipsoid
        provide a string ellipsoid identifier if you want to override what is read from the log or you don't have a
        log, ex: 'GRS80'
    max_gap_length
        maximum allowable gap in the sbet in seconds, excluding gaps found in raw navigation
    overwrite
        if True, will include files that are already in the navigation dataset as valid

    Returns
    -------
    Fqpr
        Fqpr passed in with additional post processed navigation
    """

    fqpr_inst.import_post_processed_navigation(navfiles, errorfiles=errorfiles, logfiles=logfiles,
                                               weekstart_year=weekstart_year, weekstart_week=weekstart_week,
                                               override_datum=override_datum, override_grid=override_grid,
                                               override_zone=override_zone, override_ellipsoid=override_ellipsoid,
                                               max_gap_length=max_gap_length, overwrite=overwrite)
    return fqpr_inst


def overwrite_raw_navigation(fqpr_inst: Fqpr, navfiles: list, weekstart_year: int = None, weekstart_week: int = None,
                             overwrite: bool = False):
    """
    Convenience function for importing raw navigation from pos mv .000 files, for use in georeferencing
    xyz data.  Will overwrite the raw navigation, we don't want this in the post processed section, so you can compare
    the loaded pos mv .000 file data to the processed sbet.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    fqpr_inst
        Fqpr instance containing converted data (converted data must exist for the import to work)
    navfiles:
        list of postprocessed navigation file paths
    weekstart_year
        if you aren't providing a logfile, must provide the year of the sbet here
    weekstart_week
        if you aren't providing a logfile, must provide the week of the sbet here
    overwrite
        if True, will include files that are already in the navigation dataset as valid

    Returns
    -------
    Fqpr
        Fqpr passed in with additional post processed navigation
    """

    fqpr_inst.overwrite_raw_navigation(navfiles, weekstart_year=weekstart_year, weekstart_week=weekstart_week,
                                       overwrite=overwrite)
    return fqpr_inst


def import_sound_velocity(fqpr_inst: Fqpr, sv_files: Union[str, list], cast_selection_method: str = 'nearest_in_time'):
    """
    Convenience function for passing in an instance of fqpr_generation.Fqpr and importing the provided sound velocity
    profile files as attributes.  Allows you to then run sv_correct and automatically select from the saved cast file
    attributes.

    Currently only supports .svp files following the Caris svp file format.  If you have an unsupported file type, please
    submit an issue to have it added.  File format should include depth and soundvelocity arrays as well as cast location
    and time.

    Parameters
    ----------
    fqpr_inst
        Fqpr instance containing converted data (converted data must exist for the import to work)
    sv_files
        either a list of files to include or the path to a directory containing sv files (only supporting .svp currently)
    cast_selection_method
        method used to determine the cast appropriate for each data chunk.  Used here to determine whether or not this new cast(s)
        will require reprocessing, i.e. they are selected by one or more chunks of this dataset.

    Returns
    -------
    Fqpr
        Fqpr passed in with additional post processed navigation
    """

    fqpr_inst.import_sound_velocity_files(sv_files, cast_selection_method=cast_selection_method)
    return fqpr_inst


def process_multibeam(fqpr_inst: Fqpr, run_orientation: bool = True, orientation_initial_interpolation: bool = False,
                      run_beam_vec: bool = True, run_svcorr: bool = True, run_georef: bool = True, run_tpu: bool = True,
                      add_cast_files: Union[str, list] = None, input_datum: Union[str, int] = None,
                      use_epsg: bool = False, use_coord: bool = True, epsg: int = None, coord_system: str = 'WGS84',
                      vert_ref: str = 'waterline', vdatum_directory: str = None, cast_selection_method: str = 'nearest_in_time',
                      only_this_line: str = None, only_these_times: tuple = None):
    """
    Use fqpr_generation to process already converted data on the local cluster and generate sound velocity corrected,
    georeferenced soundings in the same data store as the converted data.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    fqpr_inst
        Fqpr instance, must contain converted data
    run_orientation
        perform the get_orientation_vectors step
    orientation_initial_interpolation
        If true and running orientation, this will interpolate the raw attitude to ping time and save
        it in the respective arrays.  Otherwise, each processing step will do the interpolation on it's own.  Turn this
        off for the in memory workflow, ex: tests turn this off as we don't want to save to disk
    run_beam_vec
        perform the get_beam_pointing_vectors step
    run_svcorr
        perform the sv_correct step
    run_georef
        perform the georef_xyz step
    run_tpu
        perform the tpu step
    add_cast_files
        either a list of files to include or the path to a directory containing files.  These are in addition to
        the casts in the ping dataset.
    input_datum
        Optional, the basic input datum of the converted multibeam data, should either be nad83, wgs84 or a epsg integer code
        for a geographic coordinate reference system.  If None, will use the encoded string in the multibeam data.  If sbet_datum
        exists, input_datum will not be used, as sbet navigation and altitude are used by default.
    use_epsg
        if True, will use the epsg code to build the CRS to use
    use_coord
        if True, will use the coord_system parameter and autodetect UTM zone
    epsg
        epsg code, used if use_epsg is True
    coord_system
        coord system identifier, anything that pyproj supports can be used here, will be used if use_coord is True
    vert_ref
        the vertical reference point, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW', 'Aviso MLLW']
    vdatum_directory
        if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
    cast_selection_method
        the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
        'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']
    only_this_line
        only process this line, subset the full dataset by the min time and maximum time of the line name provided.  ex: 0000_testline.all
    only_these_times
        only process this time region, expects this to be a tuple, (minimum time in UTC seconds, maximum time in UTC seconds)

    Returns
    -------
    Fqpr
        Fqpr passed in with processed xyz soundings
    """

    if not use_epsg:
        epsg = None  # epsg is given priority, so if you don't want to use it, set it to None here
    if not use_coord and not use_epsg and run_georef:
        print('process_multibeam: please select either use_coord or use_epsg to process')
        return
    if input_datum:
        fqpr_inst.input_datum = input_datum
    subset_time = None
    if only_this_line or only_these_times:
        if only_this_line:
            minimum_time, maximum_time = fqpr_inst.return_line_time(only_this_line)
            if minimum_time is None or maximum_time is None:
                raise ValueError('process_multibeam: only_this_line={}, this line is not in the current fqpr instance'.format(only_this_line))
        else:
            minimum_time, maximum_time = only_these_times
        subset_time = [minimum_time, maximum_time]

    fqpr_inst.construct_crs(epsg=epsg, datum=coord_system, projected=True, vert_ref=vert_ref)
    if run_orientation:
        fqpr_inst.get_orientation_vectors(initial_interp=orientation_initial_interpolation, subset_time=subset_time)
    if run_beam_vec:
        fqpr_inst.get_beam_pointing_vectors(subset_time=subset_time)
    if run_svcorr:
        fqpr_inst.sv_correct(add_cast_files=add_cast_files, cast_selection_method=cast_selection_method, subset_time=subset_time)
    if run_georef:
        fqpr_inst.georef_xyz(vdatum_directory=vdatum_directory, subset_time=subset_time)
    if run_tpu:
        fqpr_inst.calculate_total_uncertainty(subset_time=subset_time)

    # dask processes appear to suffer from memory leaks regardless of how carefully we track and wait on futures, reset the client here to clear memory after processing
    # if fqpr_inst.client is not None:
    #     fqpr_inst.client.restart()

    return fqpr_inst


def reload_data(converted_folder: str, require_raw_data: bool = True, skip_dask: bool = False, silent: bool = False,
                show_progress: bool = True):
    """
    Pick up from a previous session.  Load in all the data that exists for the session using the provided
    converted_folder.  Expects there to be fqpr generated zarr datastore folders in this folder.

    os.listdir(r'C:/data_dir/converted_093926')\n
    ['attitude.zarr', 'soundings.zarr', 'navigation.zarr', 'ping_389_1_270000.zarr', 'ping_389_1_290000.zarr',
    'ping_394_1_310000.zarr', 'ping_394_1_330000.zarr']\n
    fqpr = reload_data(r'C:/data_dir/converted_093926')

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    converted_folder
        path to the parent folder containing all the zarr data store folders
    require_raw_data
        if True, raise exception if you can't find the raw data
    skip_dask
        if True, will not start/find the dask client.  Only use this if you are just reading attribution
    silent
        if True, will not print messages
    show_progress
        If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text

    Returns
    -------
    Fqpr
        Fqpr object reloaded from disk
    """

    final_paths = return_processed_data_folders(converted_folder)
    if final_paths is None:
        return None

    if (require_raw_data and final_paths['ping'] and final_paths['attitude']) or (final_paths['ping']):
        mbes_read = BatchRead(None, skip_dask=skip_dask, show_progress=show_progress)
        mbes_read.final_paths = final_paths
        read_error = mbes_read.read_from_zarr_fils(final_paths['ping'], final_paths['attitude'][0], final_paths['logfile'])
        if read_error:
            return None

        fqpr_inst = Fqpr(mbes_read, show_progress=show_progress)
        if not silent:
            fqpr_inst.logger.info('****Reloading from file {}****'.format(converted_folder))
        fqpr_inst.multibeam.xyzrph = fqpr_inst.multibeam.raw_ping[0].xyzrph

        # set new output path to the current directory of the reloaded data
        fqpr_inst.write_attribute_to_ping_records({'output_path': fqpr_inst.multibeam.converted_pth})

        if 'vertical_reference' in fqpr_inst.multibeam.raw_ping[0].attrs:
            fqpr_inst.set_vertical_reference(fqpr_inst.multibeam.raw_ping[0].vertical_reference)
        fqpr_inst.generate_starter_orientation_vectors(None, None)

        if 'horizontal_crs' in fqpr_inst.multibeam.raw_ping[0].attrs:
            fqpr_inst.construct_crs(epsg=fqpr_inst.multibeam.raw_ping[0].attrs['horizontal_crs'])
        fqpr_inst.client = mbes_read.client
    else:
        # not a valid zarr datastore
        if not silent:
            print('reload_data: Unable to open FqprProject {}'.format(converted_folder))
        return None

    if fqpr_inst is not None and not silent:
        fqpr_inst.logger.info('Successfully reloaded\n'.format(converted_folder))
    return fqpr_inst


def return_svcorr_xyz(filname: str, outfold: str = None, visualizations: bool = False):
    """
    Using fqpr_generation, convert and sv correct multibeam file (or directory of files) and return the sound velocity
    corrected xyz soundings.

    Parameters
    ----------
    filname
        multibeam file path or directory path of multibeam files
    outfold
        full file path to a directory you want to contain all the zarr folders.  Will create this folder if it does not
        exist.
    visualizations
        True if you want the matplotlib animations as well

    Returns
    -------
    Fqpr
        Fqpr object containing svcorrected offsets
    xr.Dataset
        Dataset of the variables + time + system identifier
    """

    fqpr_inst = convert_multibeam(filname, outfold)
    fqpr_inst.get_orientation_vectors()
    fqpr_inst.get_beam_pointing_vectors()
    fqpr_inst.sv_correct()

    if visualizations:
        fqpr_inst.plot.visualize_beam_pointing_vectors(corrected=False)
        fqpr_inst.plot.visualize_orientation_vector()

    dset = fqpr_inst.subset_variables(['alongtrack', 'acrosstrack', 'depthoffset'])

    return fqpr_inst, dset


def _add_points_to_mosaic(fqpr_inst: Fqpr, bgrid: BathyGrid, fqpr_crs: int, fqpr_vertref: str, avg_table: list = None):
    """
    Add this FQPR instance to the backscatter bathygrid provided.
    """

    cont_name = os.path.split(fqpr_inst.output_folder)[1]
    min_time = np.min([rp.time.values[0] for rp in fqpr_inst.multibeam.raw_ping])
    max_time = np.max([rp.time.values[-1] for rp in fqpr_inst.multibeam.raw_ping])
    multibeamfiles = list(fqpr_inst.multibeam.raw_ping[0].multibeam_files.keys())
    if bgrid.epsg and (bgrid.epsg != fqpr_crs):
        print(f'ERROR: this imported data {cont_name} has an EPSG of {fqpr_crs}, where the grid has an EPSG of {bgrid.epsg}')
        return

    print()
    for mfile in multibeamfiles:
        linedata = fqpr_inst.subset_variables_by_line(['x', 'y', 'corr_pointing_angle', 'backscatter'], line_names=mfile, filter_by_detection=True)
        rp = linedata[mfile]
        # drop nan values in georeferenced data, generally where number of beams vary between pings
        data = rp.where(~np.isnan(rp['x']), drop=True)
        if avg_table is not None:
            data['backscatter'] = data['backscatter'] - avg_correct(np.rad2deg(data['corr_pointing_angle']), avg_table)
        if data['backscatter'].any():
            try:
                bgrid.add_points(data, '{}__{}'.format(cont_name, mfile), [mfile], fqpr_crs, fqpr_vertref, min_time=min_time, max_time=max_time)
            except:
                print(f'ERROR: this imported data {"{}__{}".format(cont_name, mfile)} was not able to be added to the surface')
                continue


def _add_points_to_surface(fqpr_inst: Union[dict, Fqpr], bgrid: BathyGrid, fqpr_crs: int, fqpr_vertref: str, add_lines: Union[list, str] = None):
    """
    Add this FQPR instance or dict of point data to the bathygrid provided.
    """

    if isinstance(fqpr_inst, dict):
        has_thu = bgrid.has_horizontal_uncertainty
        has_tvu = bgrid.has_vertical_uncertainty
        nan_msk = ~np.isnan(fqpr_inst['z'])
        dtyp = [('x', np.float64), ('y', np.float64), ('z', np.float32)]
        if 'tvu' in fqpr_inst or has_tvu:
            dtyp += [('tvu', np.float32)]
        if 'thu' in fqpr_inst or has_thu:
            dtyp += [('thu', np.float32)]
        parray = np.empty(len(fqpr_inst['z'][nan_msk]), dtype=dtyp)
        parray['x'] = fqpr_inst['x'][nan_msk]
        parray['y'] = fqpr_inst['y'][nan_msk]
        parray['z'] = fqpr_inst['z'][nan_msk]
        if 'tvu' in fqpr_inst:
            parray['tvu'] = fqpr_inst['tvu'][nan_msk]
        elif has_tvu:
            print('WARNING: This grid contains vertical uncertainty but these new points do not, algorithms like CUBE will no longer work properly')
            parray['tvu'] = np.full_like(parray['z'], np.nan)
        if 'thu' in fqpr_inst:
            parray['thu'] = fqpr_inst['thu'][nan_msk]
        elif has_thu:
            print('WARNING: This grid contains vertical uncertainty but these new points do not, algorithms like CUBE will no longer work properly\n')
            parray['thu'] = np.full_like(parray['z'], np.nan)
        if 'tag' in fqpr_inst:
            containername = fqpr_inst['tag']
        else:
            containername = datetime.now().strftime('%Y%m%d_%H%M%S')
        if 'files' in fqpr_inst:
            datafiles = fqpr_inst['files']
        else:
            datafiles = None
        if bgrid.vertical_reference and (bgrid.vertical_reference != fqpr_vertref):
            # add another check for when you have complicated mllw wkt strings that might be slightly different
            if not (bgrid.vertical_reference.find('MLLW') != -1 and fqpr_vertref.find('MLLW') != -1):
                print(f'ERROR: this imported data {containername} has a vertical reference of {fqpr_vertref}, where the grid has a vertical reference of {bgrid.vertical_reference}')
                return
        elif bgrid.epsg and (bgrid.epsg != fqpr_crs):
            print(f'ERROR: this imported data {containername} has an EPSG of {fqpr_crs}, where the grid has an EPSG of {bgrid.epsg}')
            return
        try:
            bgrid.add_points(parray, containername, datafiles, fqpr_crs, fqpr_vertref)
        except:
            print(f'ERROR: this imported data {containername} was not able to be added to the surface')
            return
    else:
        cont_name = os.path.split(fqpr_inst.output_folder)[1]
        min_time = np.min([rp.time.values[0] for rp in fqpr_inst.multibeam.raw_ping])
        max_time = np.max([rp.time.values[-1] for rp in fqpr_inst.multibeam.raw_ping])
        multibeamfiles = list(fqpr_inst.multibeam.raw_ping[0].multibeam_files.keys())
        if bgrid.vertical_reference and (bgrid.vertical_reference != fqpr_vertref):
            if not (bgrid.vertical_reference.find('MLLW') != -1 and fqpr_vertref.find('MLLW') != -1):
                print(f'ERROR: this imported data {cont_name} has a vertical reference of {fqpr_vertref}, where the grid has a vertical reference of {bgrid.vertical_reference}')
                return
        elif bgrid.epsg and (bgrid.epsg != fqpr_crs):
            print(f'ERROR: this imported data {cont_name} has an EPSG of {fqpr_crs}, where the grid has an EPSG of {bgrid.epsg}')
            return
        if add_lines:
            multibeamfiles = [mfile for mfile in multibeamfiles if mfile in add_lines]
        print()
        for mfile in multibeamfiles:
            linedata = fqpr_inst.subset_variables_by_line(['x', 'y', 'z', 'tvu', 'thu'], line_names=mfile, filter_by_detection=True)
            rp = linedata[mfile]
            # drop nan values in georeferenced data, generally where number of beams vary between pings
            data = rp.where(~np.isnan(rp['z']), drop=True)
            if data['z'].any():
                try:
                    bgrid.add_points(data, '{}__{}'.format(cont_name, mfile), [mfile], fqpr_crs, fqpr_vertref, min_time=min_time,
                                     max_time=max_time)
                except:
                    print(f'ERROR: this imported data {"{}__{}".format(cont_name, mfile)} was not able to be added to the surface')
                    continue


def _remove_points_from_surface(fqpr_inst: Union[Fqpr, str], bgrid: BathyGrid, remove_lines: Union[list, str] = None):
    """
    Remove all points from the grid that match this FQPR instance.  Will remove all tags from the grid that match the
    container name of the FQPR instance.

    if remove_lines is provided, will only remove container names that contain one of the line names in remove_lines

    ex: for cont_name = em2040_dual_tx_rx_389_07_10_2019, removes 'em2040_dual_tx_rx_389_07_10_2019__linename1',
    'em2040_dual_tx_rx_389_07_10_2019__linename2', 'em2040_dual_tx_rx_389_07_10_2019__linename3', etc.
    """
    if isinstance(fqpr_inst, str):
        cont_name = fqpr_inst
    else:
        cont_name = os.path.split(fqpr_inst.output_folder)[1]
    if isinstance(remove_lines, str):
        remove_lines = [remove_lines]
    elif not remove_lines:
        remove_lines = []

    remove_these = []
    for existing_cont in bgrid.container:
        if existing_cont.find(cont_name) != -1:  # container name match
            if remove_lines:
                for remline in remove_lines:
                    if existing_cont.find(remline) != -1:
                        remove_these.append(existing_cont)
            else:
                remove_these.append(existing_cont)
    print()
    for remove_cont in remove_these:
        bgrid.remove_points(remove_cont)


def _get_unique_crs_vertref(fqpr_instances: list):
    """
    Pull the CRS and vertical reference from each FQPR instance, check to make sure there aren't differences.  We cant
    add points from different FQPR instances if the CRS or vertical reference is different.  The grid itself will check
    to make sure that the crs/vertref matches the gridded data when you add to the grid.
    """
    unique_crs = []
    unique_vertref = []
    for fq in fqpr_instances:
        if not fq.is_processed():
            print(f'_get_unique_crs_vertref: {fq.output_folder} is not fully processed, current processing status={fq.status}')
            return None, None
        crs_data = fq.horizontal_crs.to_epsg()
        vertref = fq.multibeam.raw_ping[0].vertical_reference
        if crs_data is None:
            crs_data = fq.horizontal_crs.to_proj4()
        if crs_data not in unique_crs:
            unique_crs.append(crs_data)
        if vertref not in unique_vertref:
            unique_vertref.append(vertref)

    if not fqpr_instances:
        print('_get_unique_crs_vertref: no fqpr instances provided')
        return None, None
    if len(unique_crs) > 1:
        print('_get_unique_crs_vertref: Found multiple EPSG codes in the input data, data must be of the same code: {}'.format(unique_crs))
        return None, None
    if len(unique_vertref) > 1:
        print('_get_unique_crs_vertref: Found multiple vertical references in the input data, data must be of the same reference: {}'.format(unique_vertref))
        return None, None
    if not unique_crs:
        print('_get_unique_crs_vertref: No valid EPSG for {}'.format(fqpr_instances[0].horizontal_crs.to_proj4()))
        return None, None

    # if the vertical reference is an ERS one, return the first WKT string.  We can't just get the unique WKT strings,
    #  as there might be differences in region (which we should probably concatenate or something)
    if unique_vertref[0] in kluster_variables.ellipse_based_vertical_references:
        unique_vertref = [fqpr_instances[0].multibeam.raw_ping[0].vertical_crs]

    return unique_crs, unique_vertref


def _validate_fqpr_for_gridding(fqpr_instances: list):
    """
    Check to make sure all fqpr instances that we are trying to grid have the correct georeferenced data
    """

    try:
        all_have_soundings = np.all(['x' in rp for f in fqpr_instances for rp in f.multibeam.raw_ping])
    except AttributeError:
        print('_validate_fqpr_for_gridding: Invalid Fqpr instances passed in, could not find instance.multibeam.raw_ping[0].x')
        return False

    if not all_have_soundings:
        print('_validate_fqpr_for_gridding: No georeferenced soundings found')
        return False
    return True


def generate_new_surface(fqpr_inst: Union[Fqpr, list] = None, grid_type: str = 'single_resolution', tile_size: float = 1024.0,
                         subtile_size: float = 128.0, gridding_algorithm: str = 'mean', resolution: float = None,
                         auto_resolution_mode: str = 'depth',
                         use_dask: bool = False, output_path: str = None, export_path: str = None,
                         export_format: str = 'geotiff', export_z_positive_up: bool = True,
                         export_resolution: float = None, client: Client = None, grid_parameters: dict = None):
    """
    Using the bathygrid create_grid convenience function, generate a new variable/single resolution surface for the
    provided Kluster fqpr instance(s).

    If fqpr_inst is provided and is not a list, generates a surface based on that specific fqpr converted instance.

    If fqpr_inst provided is a list of fqpr instances, will concatenate these instances and build a single surface.

    Returns an instance of the surface class and optionally saves the surface to disk.

    Parameters
    ----------
    fqpr_inst
        instance or list of instances of fqpr_generation.Fqpr class that contains generated soundings data, see
        perform_all_processing or reload_data.  Can also be a dict or list of dicts when generating a surface from
        point data outside of the FQPR object.  These dicts should have keys including ['x', 'y', 'z', 'crs', 'vert_ref']
        and optionally ['tvu', 'thu', 'tag', 'files'].  tvu and thu will be used in the CUBE algorithm, tag will be
        the container name tagged for these points in the bathygrid instance, and files will be logged as the source
        files for that container in the bathygrid metadata.  If None is provided, will create an empty surface.
    grid_type
        one of 'single_resolution', 'variable_resolution_tile'
    tile_size
        main tile size, the size in meters of the tiles within the grid, a larger tile size will improve performance,
        but size should be at most 1/2 the length/width of the survey area
    subtile_size
        sub tile size, only used for variable resolution, the size of the subtiles within the tiles, subtiles are the
        smallest unit within the grid that is single resolution
    gridding_algorithm
        algorithm to grid by, one of 'mean', 'shoalest', 'cube'
    resolution
        resolution of the gridded data in the Tiles
    auto_resolution_mode
        one of density, depth; chooses the algorithm used to determine the resolution for the grid/tile
    use_dask
        if True, will start a dask LocalCluster instance and perform the gridding in parallel
    output_path
        if provided, will save the Bathygrid to this path
    export_path
        if provided, will export the Bathygrid to file using export_format and export_resolution
    export_format
        format option, one of 'csv', 'geotiff', 'bag'
    export_z_positive_up
        if True, will output bands with positive up convention
    export_resolution
        if provided, will only export the given resolution
    client
        dask.distributed.Client instance, if you don't include this, it will automatically start a LocalCluster with the
        default options, if you set use_dask to True
    grid_parameters
        optional dict of settings to pass to the grid algorithm

    Returns
    -------
    BathyGrid
        BathyGrid instance for the newly created surface
    """

    print('***** Generating new Bathygrid surface *****')
    strttime = perf_counter()

    if fqpr_inst is not None:  # creating and adding data to a surface, validate the input data
        if not isinstance(fqpr_inst, list):
            fqpr_inst = [fqpr_inst]
        if isinstance(fqpr_inst[0], Fqpr):
            is_fqpr = True
            unique_crs, unique_vertref = _get_unique_crs_vertref(fqpr_inst)
        elif isinstance(fqpr_inst[0], dict):
            try:
                assert all([all([ky in fqprinst for ky in ['x', 'y', 'z', 'crs', 'vert_ref']]) for fqprinst in fqpr_inst])
            except:
                raise ValueError("generate_new_surface: When using point data, you must provide ['x', 'y', 'z', 'crs', 'vert_ref'] keys in each dict object")
            try:
                assert all([fqpr_inst[0]['crs'] == fq['crs'] for fq in fqpr_inst])
            except:
                raise ValueError("generate_new_surface: When using point data, all 'crs' keys must match")
            try:
                assert all([fqpr_inst[0]['vert_ref'] == fq['vert_ref'] for fq in fqpr_inst])
            except:
                raise ValueError("generate_new_surface: When using point data, all 'crs' keys must match")
            is_fqpr = False
            unique_crs = [fqpr_inst[0]['crs']]
            unique_vertref = [fqpr_inst[0]['vert_ref']]
        else:
            raise NotImplementedError('generate_new_surface: Expected input data to either be a FQPR instance, a list of FQPR instances or a dict of variables')

        if unique_vertref is None or unique_crs is None:
            return None

        if not _validate_fqpr_for_gridding(fqpr_inst):
            return None

        gridding_algorithm = gridding_algorithm.lower()
        if gridding_algorithm == 'cube' and fqpr_inst is not None:
            print('compiling cube algorithm...')
            compile_now()

        print('Preparing data...')
        # add data to grid line by line

    bg = create_grid(folder_path=output_path, grid_type=grid_type, tile_size=tile_size, subtile_size=subtile_size)
    if fqpr_inst is not None:
        if client is not None:
            bg.client = client
        for f in fqpr_inst:
            _add_points_to_surface(f, bg, unique_crs[0], unique_vertref[0])

        # now after all points are added, run grid with the options presented.  If empty grid, just save the parameters
        print()
        bg.grid(algorithm=gridding_algorithm, resolution=resolution, auto_resolution_mode=auto_resolution_mode,
                use_dask=use_dask, grid_parameters=grid_parameters)
        if export_path:
            bg.export(output_path=export_path, export_format=export_format, z_positive_up=export_z_positive_up,
                      resolution=export_resolution)
    else:  # save the gridding variables to the empty surface, so that you can add and regrid easily later without respecifying
        bg.grid_algorithm = gridding_algorithm
        if resolution is None:
            bg.grid_resolution = 'AUTO_{}'.format(auto_resolution_mode).upper()
        else:
            bg.grid_resolution = float(resolution)
        bg.grid_parameters = grid_parameters
        bg.resolutions = []
        bg._save_grid()

    endtime = perf_counter()
    print('***** Surface Generation Complete: {} *****'.format(seconds_to_formatted_string(int(endtime - strttime))))
    return bg


def _validate_fqpr_for_mosaic(fqpr_instances: list):
    """
    Check to make sure all fqpr instances that we are trying to create mosaics for have the correct georeferenced data and
    processed backscatter
    """

    try:
        all_have_soundings = np.all(['x' in rp for f in fqpr_instances for rp in f.multibeam.raw_ping])
    except AttributeError:
        print('_validate_fqpr_for_mosaic: Invalid Fqpr instances passed in, could not find instance.multibeam.raw_ping[0].x')
        return False

    if not all_have_soundings:
        print('_validate_fqpr_for_mosaic: No georeferenced soundings found')
        return False

    try:
        all_have_backscatter = np.all(['backscatter' in rp for f in fqpr_instances for rp in f.multibeam.raw_ping])
    except AttributeError:
        print('_validate_fqpr_for_mosaic: Invalid Fqpr instances passed in, could not find instance.multibeam.raw_ping[0].backscatter')
        return False

    if not all_have_backscatter:
        print('_validate_fqpr_for_mosaic: No processed backscatter found')
        return False
    return True


def return_avg_tables(fqpr_inst: list = None, avg_bin_size: float = 1.0, avg_angle: float = 45.0,
                      avg_line: str = None, overwrite_existing_avg: bool = True):
    """
    Helper function for building the angle varying gain tables used during backscatter processing.  This function is
    also wrapped into generate_new_mosaic, so you will most likely use it there.

    Will return a list of the avg table dict for each fqpr_inst provided, and if new tables are created, will also
    save the table to the 'avg_table' attribute in each fqpr object.

    Parameters
    ----------
    fqpr_inst
        list of instances of fqpr_generation.Fqpr class that contains generated backscatter data, see
        perform_all_processing or reload_data.
    avg_bin_size
        the size of the bins in the avg table in degrees.
    avg_angle
        reference angle used in the angle varying gain process
    avg_line
        multibeam file name used for the subset of data used in angle varying gain process.  if None, will use the first
        line in the first dataset
    overwrite_existing_avg
        if True, will overwrite the existing avg table with a new one.  if False, will use the existing avg table.

    Returns
    -------
    dict
        dictionary of {angles (degrees): avg correctors (dB)}
    """

    avg_tables = []
    if not overwrite_existing_avg:
        for fq in fqpr_inst:
            try:
                avg_tables.append(fq.multibeam.raw_ping[0].attrs['avg_table'])
            except:
                raise ValueError(f'_avgcorrect_fqprs: using existing avg tables, but unable to find avg table in FQPR {fq.output_folder}')
    else:
        if avg_line:
            found_fq = None
            for fq in fqpr_inst:
                if fq.line_attributes(avg_line) is not None:
                    found_fq = fq
                    fq.subset_by_lines(avg_line)
            if found_fq is None:
                raise ValueError(f'_avgcorrect_fqprs: was provided avg line = {avg_line}, but was unable to find this line in any of the provided fqpr instances')
        else:
            found_fq = fqpr_inst[0]
            first_line = list(found_fq.multibeam.raw_ping[0].multibeam_files.keys())[0]
            found_fq.subset_by_lines(first_line)
        bscatter = xr.concat([rp.backscatter for rp in fq.multibeam.raw_ping], dim='time')
        bangle = xr.concat([np.rad2deg(rp.corr_pointing_angle) for rp in fq.multibeam.raw_ping], dim='time')
        avgtbl = generate_avg_corrector(bscatter, bangle, avg_bin_size, avg_angle)
        for fq in fqpr_inst:
            fq.write_attribute_to_ping_records({'avg_table': avgtbl})
            avg_tables.append(avgtbl)
    return avg_tables


def generate_new_mosaic(fqpr_inst: Union[Fqpr, list] = None, tile_size: float = 1024.0, gridding_algorithm: str = 'mean',
                        resolution: float = None, process_backscatter: bool = True, create_mosaic: bool = True,
                        angle_varying_gain: bool = True, avg_angle: float = 45.0, avg_line: str = None, avg_bin_size: float = 1.0,
                        overwrite_existing_avg: bool = True, process_backscatter_fixed_gain_corrected: bool = True,
                        process_backscatter_tvg_corrected: bool = True, process_backscatter_transmission_loss_corrected: bool = True,
                        process_backscatter_area_corrected: bool = True, use_dask: bool = False, output_path: str = None,
                        export_path: str = None, export_format: str = 'geotiff', export_resolution: float = None,
                        client: Client = None):
    """
    Using the bathygrid create_grid convenience function, process backscatter and generate a new single resolution
    backscatter mosaic for the provided Kluster fqpr instance(s).  This is a three part process, including processing
    backscatter and saving that data to disk as a new variable, generating avg table and correcting for angle varying gain,
    building a new backscatter mosaic.  You can enable/disable these processes as you choose, for example:

     - Create mosaic from existing backscatter data = process_backscatter=False, create_mosaic=True, angle_varying_gain=True, overwrite_existing_avg=False
     - Create mosaic and process backscatter at the same time (what you do for new data) = default options
     - Just process backscatter, no mosaic = process_backscatter=True, create_mosaic=False, angle_varying_gain=False

    If fqpr_inst is provided and is not a list, generates a mosaic based on that specific fqpr converted instance.

    If fqpr_inst provided is a list of fqpr instances, will concatenate these instances and build a single mosaic.

    Returns an instance of the surface class and optionally saves the surface to disk.

    Parameters
    ----------
    fqpr_inst
        instance or list of instances of fqpr_generation.Fqpr class that contains generated backscatter data, see
        perform_all_processing or reload_data.  If None is provided, will create an empty surface.
    tile_size
        main tile size, the size in meters of the tiles within the grid, a larger tile size will improve performance,
        but size should be at most 1/2 the length/width of the survey area
    gridding_algorithm
        algorithm to grid by, one of 'mean'
    resolution
        resolution of the gridded data in the Tiles
    process_backscatter
        set to True if you want to generate the 'backscatter' variable and save this variable to disk, overwriting any
        existing processed backscatter.  'backscatter' must exist for you to create a new mosaic.  Set to False only
        if you want to use existing 'backscatter' variable
    create_mosaic
        set to True if you want to generate a new Bathygrid backscatter mosaic, saving to disk at output_path
    angle_varying_gain
        set to True if you want to normalize the processed 'backscatter' variable to the value at avg_angle prior to
        generating the mosaic.  Will use avg_line or the first line in the first dataset to generate the avg table.  avg
        table is then saved to the attribution in each fqpr provided.
    avg_angle
        if angle_varying_gain, reference angle used in the angle varying gain process
    avg_line
        if angle_varying_gain, multibeam file name used for the subset of data used in angle varying gain process.  if None, will use the first
        line in the first dataset
    avg_bin_size
        if angle_varying_gain, the size of the bins in the avg table in degrees.
    overwrite_existing_avg
        if True, will overwrite the existing avg table with a new one, if angle_varying_gain is True.  if False, will
        use the existing avg table.
    process_backscatter_fixed_gain_corrected
        if True and process_backscatter is True, will remove fixed gain from the raw reflectivity during backscatter processing,
        default is True and should probably be left so except for research purposes.
    process_backscatter_tvg_corrected
        if True and process_backscatter is True, will remove tvg from the raw reflectivity during backscatter processing,
        default is True and should probably be left so except for research purposes.
    process_backscatter_transmission_loss_corrected
        if True and process_backscatter is True, will add a transmission loss corrector to the raw reflectivity during backscatter processing,
        default is True and should probably be left so except for research purposes.
    process_backscatter_area_corrected
        if True and process_backscatter is True, will add an insonified area corrector to the raw reflectivity during backscatter processing,
        default is True and should probably be left so except for research purposes.
    use_dask
        if True, will start a dask LocalCluster instance and perform the gridding in parallel
    output_path
        if provided, will save the Bathygrid to this path
    export_path
        if provided, will export the Bathygrid to file using export_format and export_resolution
    export_format
        format option, one of 'csv', 'geotiff', 'bag'
    export_resolution
        if provided, will only export the given resolution
    client
        dask.distributed.Client instance, if you don't include this, it will automatically start a LocalCluster with the
        default options, if you set use_dask to True

    Returns
    -------
    BathyGrid
        BathyGrid instance for the newly created surface
    """

    print('***** Generating new Bathygrid mosaic *****')
    strttime = perf_counter()

    avgtables = None
    if fqpr_inst is not None:  # creating and adding data to a surface, validate the input data
        if not isinstance(fqpr_inst, list):
            fqpr_inst = [fqpr_inst]
        if isinstance(fqpr_inst[0], Fqpr):
            unique_crs, unique_vertref = _get_unique_crs_vertref(fqpr_inst)
        else:
            raise NotImplementedError('generate_new_mosaic: Expected input data to either be a FQPR instance a list of FQPR instances')

        if unique_vertref is None or unique_crs is None:
            return None

        if angle_varying_gain:
            avgtables = return_avg_tables(fqpr_inst, avg_bin_size, avg_angle, avg_line, overwrite_existing_avg)

        if process_backscatter:
            for fq in fqpr_inst:
                fq.process_backscatter(fixed_gain_corrected=process_backscatter_fixed_gain_corrected, tvg_corrected=process_backscatter_tvg_corrected,
                                       transmission_loss_corrected=process_backscatter_transmission_loss_corrected, area_corrected=process_backscatter_area_corrected)

        if create_mosaic:
            if not _validate_fqpr_for_mosaic(fqpr_inst):
                return None
            if resolution is None and fqpr_inst is not None:
                print('generate_new_mosaic: resolution must be provided and be a power of two value to create a non-empty mosaic')
                return None

        print('Preparing data...')
        # add data to grid line by line

    if create_mosaic:
        bg = create_grid(folder_path=output_path, grid_type='single_resolution', tile_size=tile_size, is_backscatter=True)
        if fqpr_inst is not None:
            if client is not None:
                bg.client = client
            for cnt, f in enumerate(fqpr_inst):
                if avgtables:
                    _add_points_to_mosaic(f, bg, unique_crs[0], unique_vertref[0], avg_table=avgtables[cnt])
                else:
                    _add_points_to_mosaic(f, bg, unique_crs[0], unique_vertref[0])

            # now after all points are added, run grid with the options presented.  If empty grid, just save the parameters
            print()
            bg.grid(algorithm=gridding_algorithm, resolution=resolution, use_dask=use_dask)
            if export_path:
                bg.export(output_path=export_path, export_format=export_format, resolution=export_resolution)
        else:  # save the gridding variables to the empty surface, so that you can add and regrid easily later without respecifying
            bg.grid_algorithm = gridding_algorithm
            bg.grid_resolution = float(resolution)
            bg.resolutions = []
            bg._save_grid()

        endtime = perf_counter()
        print('***** Mosaic Generation Complete: {} *****'.format(seconds_to_formatted_string(int(endtime - strttime))))
        return bg
    else:
        return None


def update_surface(surface_instance: Union[str, BathyGrid], add_fqpr: Union[Fqpr, list] = None, add_lines: list = None,
                   remove_fqpr: Union[Fqpr, list, str] = None, remove_lines: list = None, regrid: bool = True,
                   regrid_option: str = 'update', use_dask: bool = False):
    """
    Bathygrid instances can be updated with new points from new converted multibeam data, or have points removed from
    old multibeam data.  If you want to update the surface for changes in the multibeam data, provide the same FQPR instance
    as both add_fqpr and remove_fqpr, and it will be removed and then added back.  If you want to regrid right after updating
    the data, set regrid to True, and it will regrid any new points in the grid.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    surface_instance
        Either a path to a Bathygrid folder (will reload the surface) or a loaded Bathygrid instance
    add_fqpr
        Either a list of Fqpr instances or a single Fqpr instance to add to the surface
    add_lines
        Optional, if provided will only add lines that are in this list(s).  Either a list of lines (when a single fqpr instance
        is provided) or a list of lists of lines (when a list of fqpr instances is provided)
    remove_fqpr
        Either a list of Fqpr instances, a list of fqpr container names or a single Fqpr instance to remove from the surface
    remove_lines
        Optional, if provided will only add lines that are in this list(s).  Either a list of lines (when a single fqpr instance
        is provided) or a list of lists of lines (when a list of fqpr instances is provided)
    regrid
        If True, will immediately run grid() after adding the points to update the gridded data
    regrid_option
        controls what parts of the grid will get re-gridded if regrid is True, one of 'full', 'update'.  Full mode will
        regrid the entire grid.  Update mode will only update those tiles that have a point_count_changed=True
    use_dask
        if True, will start a dask LocalCluster instance and perform the gridding in parallel

    Returns
    -------
    BathyGrid
        BathyGrid instance for the newly updated surface
    list
        old resolution list
    list
        new resolution list
    """

    print('***** Updating Bathygrid surface *****\n')
    strttime = perf_counter()

    if isinstance(surface_instance, str):
        surface_instance = reload_surface(surface_instance)
        if surface_instance is None:
            return None, None, None

    if surface_instance.grid_algorithm == 'cube':
        print('compiling cube algorithm...')
        compile_now()

    oldrez = surface_instance.resolutions
    newrez = None
    if remove_fqpr:
        if not isinstance(remove_fqpr, list):
            remove_fqpr = [remove_fqpr]
            if remove_lines and not isinstance(remove_lines[0], str):  # expect a list of lines when a single fqpr is provided
                print(f'update_surface - remove: when a single fqpr data instance is removed by line, expect a list of line names: fqpr: {len(remove_fqpr)}, add_lines: {len(remove_lines)}')
                return None, oldrez, newrez
            remove_lines = [remove_lines]
        else:  # list of fqprs provided
            if remove_lines and len(remove_fqpr) != len(remove_lines):
                print(f'update_surface - remove: when a list of fqpr data instances are removed by line, expect a list of line names of the same length: fqpr: {len(remove_fqpr)}, add_lines: {len(remove_lines)}')
                return None, oldrez, newrez

        for cnt, rfqpr in enumerate(remove_fqpr):
            if remove_lines:
                _remove_points_from_surface(rfqpr, surface_instance, remove_lines=remove_lines[cnt])
            else:
                _remove_points_from_surface(rfqpr, surface_instance)

    if add_fqpr:
        if not isinstance(add_fqpr, list):
            add_fqpr = [add_fqpr]
            if add_lines and not isinstance(add_lines[0], str):  # expect a list of lines when a single fqpr is provided
                print(f'update_surface - add: when a single fqpr data instance is added by line, expect a list of line names: fqpr: {len(add_fqpr)}, add_lines: {len(add_lines)}')
                return None, oldrez, newrez
            add_lines = [add_lines]
        else:  # list of fqprs provided
            if add_lines and len(add_fqpr) != len(add_lines):
                print(f'update_surface - add: when a list of fqpr data instances are added by line, expect a list of line names of the same length: fqpr: {len(add_fqpr)}, add_lines: {len(add_lines)}')
                return None, oldrez, newrez

        for fq in add_fqpr:
            if not fq.is_processed():
                print(f'_get_unique_crs_vertref: {fq.output_folder} is not fully processed, current processing status={fq.status}')
                return None, oldrez, newrez

        if not _validate_fqpr_for_gridding(add_fqpr):
            return None, oldrez, newrez

        unique_crs, unique_vertref = _get_unique_crs_vertref(add_fqpr)
        if unique_vertref is None or unique_crs is None:
            return None, oldrez, newrez

        for cnt, afqpr in enumerate(add_fqpr):
            if add_lines:
                _add_points_to_surface(afqpr, surface_instance, unique_crs[0], unique_vertref[0], add_lines=add_lines[cnt])
            else:
                _add_points_to_surface(afqpr, surface_instance, unique_crs[0], unique_vertref[0])

    if regrid:
        if isinstance(surface_instance.grid_resolution, str):
            if surface_instance.name[:2].lower() == 'sr':
                # single resolution with a layer that exists already, just pull it for the update
                if surface_instance.resolutions:
                    rez = surface_instance.resolutions[0]
                    automode = 'depth'  # doesn't matter, not used
                # single resolution empty grid, with a specified resolution option
                elif isinstance(surface_instance.grid_resolution, float):
                    rez = surface_instance.grid_resolution
                    automode = 'depth'  # doesn't matter, not used
                # single resolution empty grid, with one of the auto options to pick the resolution
                else:
                    rez = None
                    if surface_instance.grid_resolution.lower() == 'auto_depth':
                        automode = 'depth'
                    elif surface_instance.grid_resolution.lower() == 'auto_density':
                        automode = 'density'
                    else:
                        print('Unrecognized grid resolution: {}'.format(surface_instance.grid_resolution))
                        return None, oldrez, newrez
            elif surface_instance.grid_resolution.lower() == 'auto_depth':
                rez = None
                automode = 'depth'
            elif surface_instance.grid_resolution.lower() == 'auto_density':
                rez = None
                automode = 'density'
            else:
                print('Unrecognized grid resolution: {}'.format(surface_instance.grid_resolution))
                return None, oldrez, newrez
        else:
            rez = float(surface_instance.grid_resolution)
            automode = 'depth'  # the default value, this will not be used when resolution is specified
        print()
        surface_instance.grid(surface_instance.grid_algorithm, rez, auto_resolution_mode=automode,
                              regrid_option=regrid_option, use_dask=use_dask, grid_parameters=surface_instance.grid_parameters)

    newrez = surface_instance.resolutions
    endtime = perf_counter()
    print('***** Surface Update Complete: {} *****'.format(seconds_to_formatted_string(int(endtime - strttime))))

    return surface_instance, oldrez, newrez


def reload_surface(surface_path: str):
    """
    Simple convenience method for reloading a surface from a path

    | surface_path = 'C:/data_directory/grid'
    | surf = reload_surface(surface_path)

    Parameters
    ----------
    surface_path
        path to the grid folder containing the surface data

    Returns
    -------
    BathyGrid
        BathyGrid instance loaded from the file path provided
    """

    try:
        bg = load_grid(surface_path)
    except Exception as e:  # allow to continue and simply print the exception to the screen
        print(e)
        bg = None
    return bg


def _csv_has_header(datafile: str):
    with open(datafile, 'r') as dfile:
        firstline = dfile.readline()
        try:
            int(firstline[0])
            return False, 0
        except ValueError:
            skiplines = 1
            for fline in dfile:
                try:
                    int(fline[0])
                    break
                except ValueError:
                    skiplines += 1
            return True, skiplines


def _csv_get_delimiter(datafile: str, skiprows: int):
    sniffer = csv.Sniffer()
    with open(datafile, 'r') as dfile:
        firstline = dfile.readline()
        for i in range(skiprows):
            firstline = dfile.readline()
    dialect = sniffer.sniff(firstline)
    return str(dialect.delimiter)


def _get_pointstosurface_transformer(datablock, input_epsg):
    zne = return_zone_from_min_max_long(datablock['x'][0], datablock['x'][0], datablock['y'][0])
    zone, hemi = int(zne[:-1]), str(zne[-1:])
    inname = CRS.from_epsg(input_epsg).name.lower()
    if inname.find('wgs') != -1 or inname.find('itrf') != -1:
        myepsg = epsg_determinator('wgs84', zone=zone, hemisphere=hemi)
    elif input_epsg in [6324]:
        myepsg = epsg_determinator('nad83(ma11)', zone=zone, hemisphere=hemi)
    elif input_epsg in [6322]:
        myepsg = epsg_determinator('nad83(pa11)', zone=zone, hemisphere=hemi)
    else:
        myepsg = epsg_determinator('nad83(2011)', zone=zone, hemisphere=hemi)

    new_transformer = Transformer.from_crs(CRS.from_epsg(input_epsg), CRS.from_epsg(myepsg), always_xy=True)
    return new_transformer


def points_to_surface(data_files: list, horizontal_epsg: int, vertical_reference: str, grid_type: str = 'single_resolution',
                      tile_size: float = 1024.0, subtile_size: float = 128, gridding_algorithm: str = 'mean', resolution: float = None,
                      auto_resolution_mode: str = 'depth', use_dask: bool = False, output_path: str = None, allow_append: bool = True,
                      export_path: str = None, export_format: str = 'geotiff', export_z_positive_up: bool = True, export_resolution: float = None,
                      client: Client = None, grid_parameters: dict = None, csv_columns: list = ('x', 'y', 'z')):
    """
    Take in points in either csv or las/laz formats, and build a new bathygrid grid from the data points.

    Parameters
    ----------
    data_files
        list of filepaths to csv or las/laz files
    horizontal_epsg
        epsg integer code for the horizontal crs of this dataset
    vertical_reference
        string identifier for the vertical reference, ex: 'MLLW'
    grid_type
        one of 'single_resolution', 'variable_resolution_tile'
    tile_size
        main tile size, the size in meters of the tiles within the grid, a larger tile size will improve performance,
        but size should be at most 1/2 the length/width of the survey area
    subtile_size
        sub tile size, only used for variable resolution, the size of the subtiles within the tiles, subtiles are the
        smallest unit within the grid that is single resolution
    gridding_algorithm
        algorithm to grid by, one of 'mean', 'shoalest', 'cube'
    resolution
        resolution of the gridded data in the Tiles
    auto_resolution_mode
        one of density, depth; chooses the algorithm used to determine the resolution for the grid/tile
    use_dask
        if True, will start a dask LocalCluster instance and perform the gridding in parallel
    output_path
        if provided, will save the Bathygrid to this path, with data saved as stacked numpy (npy) files
    allow_append
        if True and the output_path provided exists, this function will attempt to add the new points to the existing
        grid, using the existing grid resolution, grid type, etc.
    export_path
        if provided, will export the Bathygrid to csv
    export_format
        format option, one of 'csv', 'geotiff', 'bag'
    export_z_positive_up
        if True, will output bands with positive up convention
    export_resolution
        if provided, will only export the given resolution
    client
        dask.distributed.Client instance, if you don't include this, it will automatically start a LocalCluster with the
        default options, if you set use_dask to True
    grid_parameters
        optional dict of settings to pass to the grid algorithm
    csv_columns
        Used with csv files, columns in order for variables ('x', 'y', 'z', 'thu', 'tvu').  'thu' and 'tvu' are optional columns,
        but this tuple must at least include 'x', 'y' and 'z'.  If these columns are in a different order in the file,
        use the order of the tuple to reflect this.  EX: ('y', '', 'x', 'z') for northings in first column, eastings
        in the third column, depth in the fourth column, skipping the second column.

    Returns
    -------
    BathyGrid
        BathyGrid instance for the newly created surface
    """

    print('***** Generating new Bathygrid surface *****')
    strttime = perf_counter()

    try:
        assert all([os.path.splitext(f)[1] in ['.csv', '.txt', '.las', '.laz'] for f in data_files])
    except:
        raise NotImplementedError("points_to_surface: only accepting files with the following extensions ['.csv', '.txt', '.las', '.laz']")
    iscsv = os.path.splitext(data_files[0])[1] in ['.csv', '.txt']

    gridding_algorithm = gridding_algorithm.lower()
    if gridding_algorithm == 'cube':
        print('compiling cube algorithm...')
        compile_now()

    is_geographic = CRS.from_epsg(horizontal_epsg).is_geographic
    new_transformer = None

    print('Preparing data...')
    if allow_append and output_path and os.path.exists(output_path):
        print('Appending to existing grid, using the existing grid attribution for resolution, epsg, vertical reference, etc.')
        bg = reload_surface(output_path)
        grid_attribution = bg.return_attribution()
        horizontal_epsg = grid_attribution['epsg']
        vertical_reference = grid_attribution['vertical_reference']
        gridding_algorithm = grid_attribution['grid_algorithm']
        if isinstance(bg.grid_resolution, float) or isinstance(bg.grid_resolution, int):  # sr
            resolution = bg.grid_resolution
            auto_resolution_mode = 'depth'
        else:  # variable resolution mode
            resolution = None
            auto_resolution_mode = bg.grid_resolution[5:].lower()
        grid_parameters = grid_attribution['grid_parameters']
    else:
        bg = create_grid(folder_path=output_path, grid_type=grid_type, tile_size=tile_size, subtile_size=subtile_size)
    if client is not None:
        bg.client = client
    for f in data_files:
        fname = os.path.split(f)[1]
        ftag = fname + '__' + fname
        ffiles = [f]
        if iscsv:
            has_header, skiprows = _csv_has_header(f)
            delimiter = _csv_get_delimiter(f, skiprows)
            data = np.genfromtxt(f, delimiter=delimiter, skip_header=skiprows)
            datablock = {'crs': horizontal_epsg, 'vert_ref': vertical_reference, 'tag': ftag, 'files': ffiles}
            columnheaders = ['x', 'y', 'z']
            if 'thu' in csv_columns:
                columnheaders += ['thu']
            if 'tvu' in csv_columns:
                columnheaders += ['tvu']
            for column_header in columnheaders:
                try:
                    datablock[column_header] = data[:, csv_columns.index(column_header)]
                except IndexError:
                    raise NotImplementedError('points_to_surface: Unable to read "{}" column in position {}, column index does not work'.format(column_header, csv_columns.index(column_header)))
        else:
            las = laspy.read(f)
            datablock = {'x': las.x, 'y': las.y, 'z': las.z, 'crs': horizontal_epsg, 'vert_ref': vertical_reference,
                         'tag': ftag, 'files': ffiles}
        if is_geographic:
            if new_transformer is None:
                new_transformer = _get_pointstosurface_transformer(datablock, horizontal_epsg)
            newpos = new_transformer.transform(datablock['x'], datablock['y'], errcheck=False)  # longitude / latitude order (x/y)
            datablock['x'] = newpos[0]
            datablock['y'] = newpos[1]
            datablock['crs'] = new_transformer.target_crs.to_epsg()
        _add_points_to_surface(datablock, bg, datablock['crs'], vertical_reference)
    # now after all points are added, run grid with the options presented
    print()
    bg.grid(algorithm=gridding_algorithm, resolution=resolution, auto_resolution_mode=auto_resolution_mode,
            use_dask=use_dask, grid_parameters=grid_parameters)
    if export_path:
        bg.export(output_path=export_path, export_format=export_format, z_positive_up=export_z_positive_up,
                  resolution=export_resolution)

    endtime = perf_counter()
    print('***** Surface Generation Complete: {} *****'.format(seconds_to_formatted_string(int(endtime - strttime))))
    return bg


def return_processed_data_folders(converted_folder: str):
    """
    After processing, you'll have a directory of folders containing the kluster records.  Use this function to return an
    organized dict of which folders correspond to which records.

    converted_folder = C:/data_dir/converted_093926

    return_processed_data_folders(converted_folder)

    | {'attitude': ['C:/data_dir/converted_093926/attitude.zarr'],
    |  'ping': ['C:/data_dir/converted_093926/ping_40107_0_260000.zarr',
    |           'C:/data_dir/converted_093926/ping_40107_1_320000.zarr',
    |           'C:/data_dir/converted_093926/ping_40107_2_290000.zarr'],
    |  'ppnav': [],
    |  'logfile': 'C:/data_dir/converted_093926/logfile_120826.txt'}

    Parameters
    ----------
    converted_folder
        path to the folder containing the kluster processed data folders

    Returns
    -------
    final_paths
        directory paths according to record type (ex: navigation, attitude, etc.)
    """

    final_paths = {'attitude': [], 'ping': [], 'ppnav': [], 'logfile': ''}
    if os.path.isdir(converted_folder):
        for fldr in os.listdir(converted_folder):
            fldrpath = os.path.join(converted_folder, fldr)
            for ky in list(final_paths.keys()):
                if fldr.find(ky) != -1 and fldr.find('sync') == -1:  # exclude any sync folders from the zarr process file lock
                    if os.path.isdir(fldrpath):
                        final_paths[ky].append(fldrpath)
                    elif ky in ['logfile']:
                        final_paths[ky] = fldrpath
        # no log file found for this data for some reason, generate a new path for a new logfile
        if not final_paths['logfile']:
            final_paths['logfile'] = os.path.join(converted_folder, return_log_name())

    for ky in ['attitude', 'ppnav']:
        if len(final_paths[ky]) > 1:
            print(len(final_paths[ky]))
            print('return_processed_data_folders: Only one {} folder is allowed in a data store'.format(ky))
            print('found {}'.format(final_paths[ky]))
            return None
    return final_paths


def reprocess_sounding_selection(fqpr_inst: Fqpr, new_xyzrph: dict = None, subset_time: list = None, return_soundings: bool = False,
                                 georeference: bool = False, turn_off_dask: bool = True, turn_dask_back_on: bool = False,
                                 override_datum: str = None, override_vertical_reference: str = None, isolate_head: int = None,
                                 vdatum_directory: str = None, cast_selection_method: str = 'nearest_in_time'):
    """
    Designed to feed a patch test tool.  This function will reprocess all the soundings within the given subset
    time and return the xyz values without writing to disk.  If a new xyzrph (dictionary that holds the offsets and
    mounting angles information) is provided, the reprocessing will use those offsets/angles. Presumably after many
    iterations, the patch test tool would provide good offsets/angles that make this sounding selection look good and
    the full dataset can be reprocessed.

    Soundings returned as a list of numpy arrays, with xyz plus the timestamp that you can use to figure out which
    installation parameters entry applies.

    I've found that you can get up to a 6x speed increase by disabling dask.distributed for small datasets, which is
    huge for something like this that might require many iterations with small time ranges.  I've yet to see a way
    to skip the client/scheduler, so we brute force destroy it for now.  This takes a couple seconds, but shouldn't be
    a big deal.

    Parameters
    ----------
    fqpr_inst
        instance of fqpr_generation.Fqpr class containing processed xyz soundings
    new_xyzrph
        keys are translated entries, vals are dicts with timestamps:values
    subset_time
        List of utc timestamps in seconds, used as ranges for times that you want to process.\n
        ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000\n
        ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are\n
              from either 1531317999 to 1531318885 or 1531318886 to 1531321000
    return_soundings
        if True, will compute and return the soundings as well
    georeference
        if True, will georeference the soundings, else will return the vessel coordinate system aligned sv corrected
        offsets (forward, starboard, down)
    turn_off_dask
        if True, close the client and destroy it.  Just closing doesn't work, as it retains the scheduler,
        which will try and find workers that don't exist when you run a process
    turn_dask_back_on
        if True, will restart the client by reloading data if the client does not exist
    override_datum
        datum identifier if soundings does not exist, will prefer this over the soundings information
    override_vertical_reference
        vertical reference identifier, will prefer this over the soundings information
    isolate_head
        only used with return_soundings, if provided will only return soundings corresponding to this head index,
        0 = port, 1 = starboard
    vdatum_directory
        path to the vdatum directory, required for georeferencing with NOAA MLLW or MHW vertical references
    cast_selection_method
        the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
        'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']

    Returns
    -------
    Fqpr
        instance or list of instances of fqpr_generation.Fqpr class that contains generated soundings data in the
        intermediate_data attribute, see Fqpr.intermediate_dat
    list
        list of numpy arrays, [x (easting in meters), y (northing in meters), z (depth pos down in meters),
        tstmp (xyzrph timestamp for each sounding)
    """

    if 'horizontal_crs' not in fqpr_inst.multibeam.raw_ping[0].attrs and georeference:
        raise ValueError('horizontal_crs object not found.  Please run Fqpr.construct_crs first')
    if 'vertical_reference' not in fqpr_inst.multibeam.raw_ping[0].attrs and georeference:
        raise NotImplementedError('set_vertical_reference must be run before georeferencing')

    if turn_off_dask and fqpr_inst.client is not None:
        fqpr_inst.client.close()
        fqpr_inst.client = None
        fqpr_inst.multibeam.client = None

    if new_xyzrph is not None:
        fqpr_inst.multibeam.xyzrph = new_xyzrph

    if override_vertical_reference:
        fqpr_inst.set_vertical_reference(override_vertical_reference)

    if subset_time is not None:
        if type(subset_time[0]) is not list:  # fix for when user provides just a list of floats [start,end]
            subset_time = [subset_time]

    fqpr_inst.get_orientation_vectors(subset_time=subset_time, dump_data=False)
    fqpr_inst.get_beam_pointing_vectors(subset_time=subset_time, dump_data=False)
    fqpr_inst.sv_correct(cast_selection_method=cast_selection_method, subset_time=subset_time, dump_data=False)
    if georeference:
        if override_datum is not None:
            datum = override_datum
            epsg = None
        else:
            datum = None
            epsg = fqpr_inst.multibeam.raw_ping[0].horizontal_crs

        fqpr_inst.construct_crs(epsg=epsg, datum=datum)
        fqpr_inst.georef_xyz(subset_time=subset_time, vdatum_directory=vdatum_directory, dump_data=False)
        data_store = 'georef'
    else:
        data_store = 'sv_corr'

    if return_soundings:
        soundings = [[], [], [], []]
        for sector in fqpr_inst.intermediate_dat:
            if isolate_head and sector != fqpr_inst.multibeam.raw_ping[isolate_head].system_identifier:
                continue
            if data_store in fqpr_inst.intermediate_dat[sector]:
                for tstmp in fqpr_inst.intermediate_dat[sector][data_store]:
                    dat = fqpr_inst.intermediate_dat[sector][data_store][tstmp]
                    for d in dat:
                        x_vals = np.ravel(d[0][0])
                        y_vals = np.ravel(d[0][1])
                        z_vals = np.ravel(d[0][2])
                        idx = ~np.isnan(x_vals)
                        soundings[0].append(x_vals[idx])
                        soundings[1].append(y_vals[idx])
                        soundings[2].append(z_vals[idx])
                        soundings[3].append([tstmp] * len(z_vals[idx]))
            else:
                print('No soundings found for {}'.format(sector))

        soundings = [np.concatenate(s, axis=0) for s in soundings]
    else:
        soundings = None
    if turn_dask_back_on and fqpr_inst.client is None:
        fqpr_inst.client = dask_find_or_start_client(address=fqpr_inst.multibeam.address)
        fqpr_inst.multibeam.client = fqpr_inst.client
    return fqpr_inst, soundings


def _dual_head_sort(idx: int, my_y: np.array, kongs_y: np.array, prev_index: int):
    """
    Big ugly check to see if the par found xyz88 records are in alternating port head/stbd head (or stbd head/port head)
    order.  Important because we want to compare the result side by side with the Kluster sv corrected data.

    Idea here is to check the mean across track value and see if it is on the left or right (positive or negative).
    If the Kluster is on the left and the par is on the right, look at the surrounding recs to fix the order.

    Parameters
    ----------
    idx
        int, index for the par/kluster records
    my_y
        numpy array, acrosstrack 2d (ping, beam) from kluater/fqpr_generation sv correction
    kongs_y
        numpy array, acrosstrack 2d (ping, beam) from par xyz88 read
    prev_index
        int, feedback from previous run of _dual_head_sort to guide the surrounding search

    Returns
    -------
    int
        corrected index for port/stbd head order in par module xyz88
    int
        feedback for next run
    """

    ki = idx
    my_port = my_y[idx].mean()
    kongs_port = kongs_y[idx].mean()

    my_port = my_port < 0
    kongs_port = kongs_port < 0
    if not my_port == kongs_port:
        print('Found ping that doesnt line up with par, checking nearby pings (should only occur with dual head)')
        found = False
        potential_idxs = [-prev_index, 1, -1, 2, -2]
        for pot_idx in potential_idxs:
            try:
                kongs_port = kongs_y[idx + pot_idx].mean() < 0
                if kongs_port == my_port:
                    found = True
                    ki = idx + pot_idx
                    prev_index = pot_idx
                    print('- Adjusting {} to {} for par index'.format(idx, ki))
                    break
            except IndexError:
                print('_dual_head_sort: Reached end of par xyz88 records')
        if not found:
            raise ValueError('Found ping at {} that does not appear to match nearby kluster processed pings'.format(idx))
    return ki, prev_index


def _single_head_sort(idx: int, my_x: np.array, kongs_x: np.array):
    """
    Big ugly check to see if the par found xyz88 records are in alternating port head/stbd head (or stbd head/port head)
    order.  Important because we want to compare the result side by side with the Kluster sv corrected data.

    Idea here is to check the mean across track value and see if it is on the left or right (positive or negative).
    If the Kluster is on the left and the par is on the right, look at the surrounding recs to fix the order.

    Parameters
    ----------
    idx
        int, index for the par/kluster records
    my_x
        numpy array, acrosstrack 2d (ping, beam) from kluater/fqpr_generation sv correction
    kongs_x
        numpy array, acrosstrack 2d (ping, beam) from par xyz88 read

    Returns
    -------
    int
        corrected index for port/stbd head order in par module xyz88
    int
        feedback for next run
    """

    my_alongtrack = float(my_x[idx].mean())
    kongs_alongtrack = kongs_x[idx].mean()

    previous_index = idx - 1
    if previous_index >= 0:
        kongs_alongtrack_pre = kongs_x[idx-1].mean()
    else:
        kongs_alongtrack_pre = 999

    post_index = idx + 1
    if post_index < kongs_x.shape[0]:
        kongs_alongtrack_post = kongs_x[idx+1].mean()
    else:
        kongs_alongtrack_post = 999

    closest_index = np.argmin(np.abs(my_alongtrack - np.array([kongs_alongtrack, kongs_alongtrack_pre, kongs_alongtrack_post])))
    correct_idx = [idx, idx - 1, idx + 1][closest_index]

    return correct_idx


def validation_against_xyz88(filname: str, analysis_mode: str = 'even', numplots: int = 10,
                             visualizations: bool = False, export: str = None):
    """
    Function to take a multibeam file and compare the svcorrected xyz with the converted and sound velocity
    corrected data generated by Kluster/fqpr_generation.  This is mostly here just to validate the Kluster data,
    definitely not performant.  Will generate a plot with subplots that are useful for analysis.

    Will take select pings from xyz88 and Kluster and compare them with a plot of differences

    Parameters
    ----------
    filname
        full path to multibeam file
    analysis_mode
        'even' = select pings are evenly distributed, 'random' = select pings are randomly distributed, 'first' = pings will be the first found in the file
    numplots
        number of pings to compare
    visualizations
        True if you want the matplotlib animations
    export
        if export path is provided, save the image to this path

    Returns
    -------
    Fqpr
        returned here for further analysis if you want it
    """

    x, y, z, times, counters = return_xyz_from_multibeam(filname)
    print('Reading and processing from raw raw_ping/.all file with Kluster...')
    fq, dset = return_svcorr_xyz(filname, visualizations=visualizations)

    print('Plotting...')
    if times[0] == 0.0:
        # seen this with EM710 data, the xyz88 dump has zeros arrays at the start, find the first nonzero time
        #    (assuming it starts in the first 100 times)
        print('Found empty arrays in xyz88, seems common with EM710 data')
        first_nonzero = np.where(times[:100] != 0.0)[0][0]
        x = x[first_nonzero:]
        y = y[first_nonzero:]
        z = z[first_nonzero:]
        times = times[first_nonzero:]
    if x.shape != dset.alongtrack.shape:
        print('Found incompatible par/Kluster data sets.  Kluster x shape {}, par x shape {}'.format(dset.alongtrack.shape,
                                                                                                     x.shape))
    if fq.multibeam.is_dual_head():
        print('WANRING: I have not figured out the comparison of xyz88/kluster generated data with dual head systems.' +
              'The ping order is different and the ping counters are all the same across heads.')

    # pick some indexes interspersed in the array to visualize
    if analysis_mode == 'even':
        idx = np.arange(0, len(z), int(len(z) / numplots), dtype=np.int32)
    elif analysis_mode == 'random':
        idx = np.random.randint(0, len(z), size=int(numplots))
    elif analysis_mode == 'first':
        idx = np.arange(0, numplots, dtype=np.int32)
    else:
        raise ValueError('{} is not a valid analysis mode'.format(analysis_mode))

    # plot some results
    if export:  # need to predefine figure size when saving to disk
        fig = plt.figure(figsize=(22, 14))
    else:  # user can resize
        fig = plt.figure()
    gs = GridSpec(3, 3, figure=fig)
    myz_plt = fig.add_subplot(gs[0, :])
    kongsz_plt = fig.add_subplot(gs[1, :])
    alongdif_plt = fig.add_subplot(gs[2, 0])
    acrossdif_plt = fig.add_subplot(gs[2, 1])
    zvaldif_plt = fig.add_subplot(gs[2, 2])

    fig.suptitle('XYZ88 versus Kluster Processed Data')
    myz_plt.set_title('Kluster Vertical')
    kongsz_plt.set_title('XYZ88 Vertical')
    alongdif_plt.set_title('Kluster/XYZ88 Alongtrack Difference')
    acrossdif_plt.set_title('Kluster/XYZ88 Acrosstrack Difference')
    zvaldif_plt.set_title('Kluster/XYZ88 Vertical Difference')

    lbls = []
    prev_index = 0
    for i in idx:
        if fq.multibeam.is_dual_head():
            ki, prev_index = _dual_head_sort(i, dset.acrosstrack, y, prev_index)
        elif (np.abs(times[i - 1] - times[i]) > 0.01) or (np.abs(times[i + 1] - times[i]) > 0.01):  # dual ping
            ki = _single_head_sort(i, dset.alongtrack, x)
        else:
            ki = i
        lbls.append(times[ki])
        myz_plt.plot(dset.depthoffset[i])
        kongsz_plt.plot(z[ki])
        alongdif_plt.plot(dset.alongtrack[i] - x[ki])
        acrossdif_plt.plot(dset.acrosstrack[i] - y[ki])
        zvaldif_plt.plot(dset.depthoffset[i] - z[ki])

    myz_plt.legend(labels=lbls, bbox_to_anchor=(1.05, 1), loc="upper left")
    if export:
        plt.tight_layout()
        plt.savefig(export)
        print('Figure saved to {}'.format(export))
    fq.close()
    return fq


def get_attributes_from_zarr_stores(list_dir_paths: list):
    """
    Takes in a list of paths to directories containing fqpr generated zarr stores.  Returns a list where each element
    is a dict of attributes found in each zarr store.  Prefers the attributes from the xyz_dat store, but if it can't
    find it, will read from one of the raw_ping converted zarr stores.  (all attributes across raw_ping data stores
    are identical)

    Parameters
    ----------
    list_dir_paths
        list of strings for paths to each converted folder containing the zarr folders

    Returns
    -------
    list
        list of dicts for each successfully reloaded fqpr object
    """

    attrs = []
    for pth in list_dir_paths:
        fqpr_instance = reload_data(pth, skip_dask=True)
        if fqpr_instance is not None:
            newattrs = get_attributes_from_fqpr(fqpr_instance)
            attrs.append(newattrs)
        else:
            attrs.append([None])
    return attrs


def get_attributes_from_fqpr(fqpr_instance, include_mode: bool = True):
    """
    Takes in a FQPR instance.  Returns a dict of the attribution in that instance.  Prefers the attributes from the
    xyz_dat store, but if it can't find it, will read from one of the raw_ping converted zarr stores.
    (all attributes across raw_ping data stores are identical)

    If include_mode is True, will also read and translate unique modes from each raw_ping and include a unique set of
    those modes in the returned attributes.

    Parameters
    ----------
    fqpr_instance
        fqpr instance that you want to load from
    include_mode
        if True, include mode in the returned attributes

    Returns
    -------
    dict
        dict of attributes in that FQPR instance
    """

    mode_translator = {'vsCW': 'CW_veryshort', 'shCW': 'CW_short', 'meCW': 'CW_medium', 'loCW': 'CW_long',
                       'vlCW': 'CW_verylong', 'elCW': 'CW_extralong', 'shFM': 'FM_short', 'loFM': 'FM_long',
                       '__FM': 'FM', 'FM': 'FM', 'CW': 'CW', 'VS': 'VeryShallow', 'SH': 'Shallow', 'ME': 'Medium',
                       'DE': 'Deep', 'VD': 'VeryDeep', 'ED': 'ExtraDeep'}

    if 'xyz_dat' in fqpr_instance.__dict__:
        if fqpr_instance.soundings is not None:
            newattrs = fqpr_instance.soundings.attrs.copy()
        else:
            newattrs = fqpr_instance.multibeam.raw_ping[0].attrs.copy()
    else:
        newattrs = fqpr_instance.multibeam.raw_ping[0].attrs.copy()

    try:
        # update for the attributes in other datasets
        for other_attrs in [fqpr_instance.multibeam.raw_att.attrs]:
            for k, v in other_attrs.items():
                if k not in newattrs:
                    try:
                        newattrs[k] = v
                    except:
                        print('unable to add {}'.format(k))
                elif isinstance(newattrs[k], list):
                    try:
                        for sub_att in v:
                            if sub_att not in newattrs[k]:
                                newattrs[k].append(sub_att)
                    except:
                        print('unable to append {}'.format(k))
                elif isinstance(newattrs[k], dict):
                    try:
                        newattrs[k].update(v)
                    except:
                        print('Unable to update {}'.format(k))
    except AttributeError:
        print('Unable to read from Navigation')

    if include_mode:
        translated_mode = [mode_translator[a] for a in fqpr_instance.return_unique_mode()]
        newattrs['mode'] = str(translated_mode)
    return newattrs

#
# def write_all_attributes_to_excel(list_dir_paths: list, output_excel: str):
#     """
#     Using get_attributes_from_zarr_stores, write an excel document, where each row contains the attributes from
#     each provided fqpr_generation made fqpr zarr store.
#
#     Parameters
#     ----------
#     list_dir_paths
#         list of strings for paths to each converted folder containing the zarr folders
#     output_excel
#         path to the newly created excel file
#     """
#
#     attrs = get_attributes_from_zarr_stores(list_dir_paths)
#     headers = list(attrs[0].keys())
#
#     wb = openpyxl.Workbook()
#     for name in wb.get_sheet_names():
#         if name == 'Sheet':
#             temp = wb.get_sheet_by_name('Sheet')
#             wb.remove_sheet(temp)
#     ws = wb.create_sheet('Kluster Attributes')
#
#     for cnt, h in enumerate(headers):
#         ws[chr(ord('@') + cnt + 1) + '1'] = h
#     for row_indx, att in enumerate(attrs):
#         for cnt, key in enumerate(att):
#             ws[chr(ord('@') + cnt + 1) + str(row_indx + 2)] = str(att[key])
#     wb.save(output_excel)
