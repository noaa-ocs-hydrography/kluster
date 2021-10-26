import os
from time import perf_counter
import xarray as xr
import numpy as np
from dask.distributed import Client
from typing import Union
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt

from HSTB.drivers.par3 import AllRead
from HSTB.drivers.kmall import kmall
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.fqpr_helpers import seconds_to_formatted_string, return_files_from_path
from HSTB.kluster.logging_conf import return_log_name
from bathygrid.convenience import create_grid, load_grid, BathyGrid
from HSTB.kluster import kluster_variables


def perform_all_processing(filname: Union[str, list], navfiles: list = None, outfold: str = None, coord_system: str = 'WGS84',
                           vert_ref: str = 'waterline', orientation_initial_interpolation: bool = False,
                           add_cast_files: Union[str, list] = None,
                           skip_dask: bool = False, show_progress: bool = True, parallel_write: bool = True,
                           vdatum_directory: str = None, **kwargs):
    """
    Use fqpr_generation to process multibeam data on the local cluster and generate a sound velocity corrected,
    georeferenced xyz with uncertainty in csv files in the provided output folder.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    filname
        either a list of .all file paths, a single .all file path or a path to a directory with .all files
    navfiles
        list of postprocessed navigation file paths.  If provided, expects either a log file or
        weekstart_year/weekstart_week/override_datum arguments, see import_navigation
    outfold
        full file path to a directory you want to contain all the zarr folders.  Will create this folder
        if it does not exist.
    coord_system
        a valid datum identifier that pyproj CRS will accept
    vert_ref
        the vertical reference point, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW']
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

    Returns
    -------
    Fqpr
        Fqpr object containing processed data
    """

    fqpr_inst = convert_multibeam(filname, outfold, skip_dask=skip_dask, show_progress=show_progress, parallel_write=parallel_write)
    if fqpr_inst is not None:
        if navfiles is not None:
            fqpr_inst = import_processed_navigation(fqpr_inst, navfiles, **kwargs)
        fqpr_inst = process_multibeam(fqpr_inst, add_cast_files=add_cast_files, coord_system=coord_system, vert_ref=vert_ref,
                                      orientation_initial_interpolation=orientation_initial_interpolation,
                                      vdatum_directory=vdatum_directory)
    return fqpr_inst


def convert_multibeam(filname: Union[str, list], outfold: str = None, client: Client = None, skip_dask: bool = False,
                      show_progress: bool = True, parallel_write: bool = True):
    """
    Use fqpr_generation to process multibeam data on the local cluster and generate a new Fqpr instance saved to the
    provided output folder.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Parameters
    ----------
    filname
        either a list of .all file paths, a single .all file path or a path to a directory with .all files
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
        fqpr_inst.read_from_source()

        # dask processes appear to suffer from memory leaks regardless of how carefully we track and wait on futures, reset the client here to clear memory after processing
        # if fqpr_inst.client is not None:
        #     fqpr_inst.client.restart()
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


def import_sound_velocity(fqpr_inst: Fqpr, sv_files: Union[str, list]):
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

    Returns
    -------
    Fqpr
        Fqpr passed in with additional post processed navigation
    """

    fqpr_inst.import_sound_velocity_files(sv_files)
    return fqpr_inst


def process_multibeam(fqpr_inst: Fqpr, run_orientation: bool = True, orientation_initial_interpolation: bool = False,
                      run_beam_vec: bool = True, run_svcorr: bool = True, run_georef: bool = True, run_tpu: bool = True,
                      add_cast_files: Union[str, list] = None, use_epsg: bool = False,
                      use_coord: bool = True, epsg: int = None, coord_system: str = 'WGS84',
                      vert_ref: str = 'waterline', vdatum_directory: str = None, only_this_line: str = None,
                      only_these_times: tuple = None):
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
    use_epsg
        if True, will use the epsg code to build the CRS to use
    use_coord
        if True, will use the coord_system parameter and autodetect UTM zone
    epsg
        epsg code, used if use_epsg is True
    coord_system
        coord system identifier, anything that pyproj supports can be used here, will be used if use_coord is True
    vert_ref
        the vertical reference point, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW']
    vdatum_directory
        if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
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
        fqpr_inst.sv_correct(add_cast_files=add_cast_files, subset_time=subset_time)
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
        for rp in fqpr_inst.multibeam.raw_ping:
            rp.attrs['output_path'] = fqpr_inst.multibeam.converted_pth
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


def _add_points_to_surface(fqpr_inst: Fqpr, bgrid: BathyGrid, fqpr_crs: int, fqpr_vertref: str, chunksize: int = 10000):
    """
    Add this FQPR instance to the bathygrid provided.
    """
    cont_name = os.path.split(fqpr_inst.multibeam.raw_ping[0].output_path)[1]
    multibeamfiles = list(fqpr_inst.multibeam.raw_ping[0].multibeam_files.keys())
    cont_name_idx = 0
    for rp in fqpr_inst.multibeam.raw_ping:
        mintime, maxtime = rp.time.values[0], rp.time.values[-1]
        number_of_pings = rp.time.size
        rp = rp.drop_vars([nms for nms in rp.variables if nms not in ['x', 'y', 'z', 'tvu', 'thu', 'detectioninfo']])
        totalchunks = int(np.ceil(number_of_pings / chunksize))
        print('Adding points from {} in {} chunks...\n'.format(os.path.split(fqpr_inst.output_folder)[1], totalchunks))
        for idx in range(totalchunks):
            strt, end = idx * chunksize, min((idx + 1) * chunksize, number_of_pings)
            data = rp.isel(time=slice(strt, end)).stack({'sounding': ('time', 'beam')})
            # drop nan values in georeferenced data, generally where number of beams vary between pings
            data = data.where(~np.isnan(data['z']), drop=True)
            # filter out rejected soundings, i.e. where detectioninfo = 2
            data = data.where(data['detectioninfo'] != kluster_variables.rejected_flag, drop=True)
            data = data.drop_vars(['detectioninfo'])
            bgrid.add_points(data, '{}_{}'.format(cont_name, cont_name_idx), multibeamfiles, fqpr_crs, fqpr_vertref,
                             min_time=mintime, max_time=maxtime)
            cont_name_idx += 1


def _remove_points_from_surface(fqpr_inst: Union[Fqpr, str], bgrid: BathyGrid):
    """
    Remove all points from the grid that match this FQPR instance.  Will remove all tags from the grid that match the
    container name of the FQPR instance.

    ex: for cont_name = em2040_dual_tx_rx_389_07_10_2019, removes 'em2040_dual_tx_rx_389_07_10_2019_0',
    'em2040_dual_tx_rx_389_07_10_2019_1', 'em2040_dual_tx_rx_389_07_10_2019_2', etc.
    """
    if isinstance(fqpr_inst, str):
        cont_name = fqpr_inst
    else:
        cont_name = os.path.split(fqpr_inst.output_folder)[1]
    remove_these = []
    for existing_cont in bgrid.container:
        if existing_cont.find(cont_name) != -1:
            remove_these.append(existing_cont)

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

    # if the vertical reference is a vdatum one, return the first WKT string.  We can't just get the unique WKT strings,
    #  as there might be differences in region (which we should probably concatenate or something)
    if unique_vertref[0] in ['NOAA MLLW', 'NOAA MHW']:
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


def generate_new_surface(fqpr_inst: Union[Fqpr, list], grid_type: str = 'single_resolution', tile_size: float = 1024.0,
                         subtile_size: float = 128, gridding_algorithm: str = 'mean', resolution: float = None,
                         auto_resolution_mode: str = 'depth',
                         use_dask: bool = False, output_path: str = None, export_path: str = None,
                         export_format: str = 'geotiff', export_z_positive_up: bool = True,
                         export_resolution: float = None, client: Client = None):
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
        perform_all_processing or reload_data
    grid_type
        one of 'single_resolution', 'variable_resolution_tile'
    tile_size
        main tile size, the size in meters of the tiles within the grid, a larger tile size will improve performance,
        but size should be at most 1/2 the length/width of the survey area
    subtile_size
        sub tile size, only used for variable resolution, the size of the subtiles within the tiles, subtiles are the
        smallest unit within the grid that is single resolution
    gridding_algorithm
        algorithm to grid by, one of 'mean', 'shoalest
    resolution
        resolution of the gridded data in the Tiles
    auto_resolution_mode
        one of density, depth; chooses the algorithm used to determine the resolution for the grid/tile
    use_dask
        if True, will start a dask LocalCluster instance and perform the gridding in parallel
    output_path
        if provided, will save the Bathygrid to this path, with data saved as stacked numpy (npy) files
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

    Returns
    -------
    BathyGrid
        BathyGrid instance for the newly created surface
    """

    print('***** Generating new Bathygrid surface *****')
    strttime = perf_counter()

    if not isinstance(fqpr_inst, list):
        fqpr_inst = [fqpr_inst]

    if not _validate_fqpr_for_gridding(fqpr_inst):
        return None

    unique_crs, unique_vertref = _get_unique_crs_vertref(fqpr_inst)
    if unique_vertref is None or unique_crs is None:
        return None

    print('Preparing data...')
    # set some arbitrary number of pings to hold in memory at once, probably need a smarter way to do this eventually
    #  just make sure it is a multiple of 1000, the chunksize of the raw_ping dataset
    bg = create_grid(folder_path=output_path, grid_type=grid_type, tile_size=tile_size, subtile_size=subtile_size)
    if client is not None:
        bg.client = client
    for f in fqpr_inst:
        _add_points_to_surface(f, bg, unique_crs[0], unique_vertref[0])

    # now after all points are added, run grid with the options presented
    bg.grid(algorithm=gridding_algorithm, resolution=resolution, auto_resolution_mode=auto_resolution_mode, use_dask=use_dask)
    if export_path:
        bg.export(output_path=export_path, export_format=export_format, z_positive_up=export_z_positive_up,
                  resolution=export_resolution)

    endtime = perf_counter()
    print('***** Surface Generation Complete: {} *****'.format(seconds_to_formatted_string(int(endtime - strttime))))
    return bg


def update_surface(surface_instance: Union[str, BathyGrid], add_fqpr: Union[Fqpr, list] = None, remove_fqpr: Union[Fqpr, list, str] = None,
                   regrid: bool = True, regrid_option: str = 'update', use_dask: bool = False):
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
    remove_fqpr
        Either a list of Fqpr instances, a list of fqpr container names or a single Fqpr instance to remove from the surface
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
    """

    if isinstance(surface_instance, str):
        surface_instance = reload_surface(surface_instance)
        if surface_instance is None:
            return None

    if remove_fqpr:
        if not isinstance(remove_fqpr, list):
            remove_fqpr = [remove_fqpr]
        for rfqpr in remove_fqpr:
            _remove_points_from_surface(rfqpr, surface_instance)

    if add_fqpr:
        if not isinstance(add_fqpr, list):
            add_fqpr = [add_fqpr]

        if not _validate_fqpr_for_gridding(add_fqpr):
            return None

        unique_crs, unique_vertref = _get_unique_crs_vertref(add_fqpr)
        if unique_vertref is None or unique_crs is None:
            return None

        for afqpr in add_fqpr:
            _add_points_to_surface(afqpr, surface_instance, unique_crs[0], unique_vertref[0])

    if regrid:
        if surface_instance.grid_resolution.lower() == 'auto_depth':
            rez = None
            automode = 'depth'
        elif surface_instance.grid_resolution.lower() == 'auto_density':
            rez = None
            automode = 'density'
        else:
            rez = surface_instance.grid_resolution
            automode = 'depth'  # the default value, this will not be used when resolution is specified
        surface_instance.grid(surface_instance.grid_algorithm, rez, auto_resolution_mode=automode,
                              regrid_option=regrid_option, use_dask=use_dask)
    return surface_instance


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


def reprocess_sounding_selection(fqpr_inst: Fqpr, new_xyzrph: dict = None, subset_time: list = None,
                                 georeference: bool = False, turn_off_dask: bool = True, turn_dask_back_on: bool = False,
                                 override_datum: str = None, override_vertical_reference: str = None):
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

    Returns
    -------
    Fqpr
        instance or list of instances of fqpr_generation.Fqpr class that contains generated soundings data
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
        fqpr_inst = reload_data(os.path.dirname(fqpr_inst.multibeam.final_paths['ping'][0]), skip_dask=True)

    if new_xyzrph is not None:
        fqpr_inst.multibeam.xyzrph = new_xyzrph

    if override_vertical_reference:
        fqpr_inst.set_vertical_reference(override_vertical_reference)

    if subset_time is not None:
        if type(subset_time[0]) is not list:  # fix for when user provides just a list of floats [start,end]
            subset_time = [subset_time]

    fqpr_inst.get_orientation_vectors(subset_time=subset_time, dump_data=False)
    fqpr_inst.get_beam_pointing_vectors(subset_time=subset_time, dump_data=False)
    fqpr_inst.sv_correct(subset_time=subset_time, dump_data=False)
    if georeference:
        if override_datum is not None:
            datum = override_datum
            epsg = None
        else:
            datum = None
            epsg = fqpr_inst.multibeam.raw_ping[0].horizontal_crs

        fqpr_inst.construct_crs(epsg=epsg, datum=datum)
        fqpr_inst.georef_xyz(subset_time=subset_time, dump_data=False)
        data_store = 'xyz'
    else:
        data_store = 'sv_corr'

    soundings = [[], [], [], []]
    for sector in fqpr_inst.intermediate_dat:
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
    if turn_dask_back_on and fqpr_inst.client is None:
        fqpr_inst = reload_data(os.path.dirname(fqpr_inst.multibeam.final_paths['ping'][0]))
    return fqpr_inst, soundings


def xyz_from_allfile(filname: str):
    """
    function using par to pull out the xyz88 datagram and return the xyz for each ping.  Times returned are a sum of
    ping time and delay time (to match Kluster, I do this so that times are unique across sector identifiers).

    Parameters
    ----------
    filname
        str, path to .all file

    Returns
    -------
    np.array
        2d numpy array (time, beam) of the alongtrack offsets from the xyz88 record
    np.array
        2d numpy array (time, beam) of the acrosstrack offsets from the xyz88 record
    np.array
        2d numpy array (time, beam) of the depth offsets from the xyz88 record
    np.array
        numpy array of the times from the xyz88 record
    np.array
        numpy array of the ping counter index from the xyz88 record
    """

    pfil = AllRead(filname)
    pfil.mapfile()
    num88 = len(pfil.map.packdir['88'])
    numbeams = pfil.getrecord(88, 0).data['Depth'].shape[0]

    dpths = np.zeros((num88, numbeams))
    xs = np.zeros((num88, numbeams))
    ys = np.zeros((num88, numbeams))
    tms = np.zeros(num88)
    cntrs = np.zeros(num88)

    for i in range(num88):
        try:
            rec88 = pfil.getrecord(88, i)
            rec78 = pfil.getrecord(78, i)

            dpths[i, :] = rec88.data['Depth']
            ys[i, :] = rec88.data['AcrossTrack']
            xs[i, :] = rec88.data['AlongTrack']
            tms[i] = rec88.time + rec78.tx_data.Delay[0]  # match par sequential_read, ping time = timestamp + delay
            cntrs[i] = rec88.Counter

        except IndexError:
            break

    # ideally this would do it, but we have to sort by prim/stbd arrays when cntr/times are equal between heads for dual head
    cntrsorted = np.argsort(cntrs)

    tms = tms[cntrsorted]
    xs = xs[cntrsorted]
    ys = ys[cntrsorted]
    dpths = dpths[cntrsorted]
    cntrs = cntrs[cntrsorted]
    pfil.close()

    return xs, ys, dpths, tms, cntrs


def xyz_from_kmallfile(filname: str):
    """
    function using kmall to pull out the xyz88 datagram and return the xyz for each ping.  Times returned are a sum of
    ping time and delay time (to match Kluster, I do this so that times are unique across sector identifiers).

    The kmall svcorrected soundings are rel ref point and not tx.  We need to remove the reference point lever arm
    to get the valid comparison with kluster.  Kluster sv correct is rel tx.

    Parameters
    ----------
    filname
        str, path to .all file

    Returns
    -------
    np.array
        2d numpy array (time, beam) of the alongtrack offsets from the MRZ record
    np.array
        2d numpy array (time, beam) of the acrosstrack offsets from the MRZ record
    np.array
        2d numpy array (time, beam) of the depth offsets from the MRZ record
    np.array
        numpy array of the times from the MRZ record
    np.array
        numpy array of the ping counter index from the MRZ record
    """

    km = kmall(filname)
    km.index_file()
    numpings = km.Index['MessageType'].value_counts()["b'#MRZ'"]
    numbeams = len(km.read_first_datagram('MRZ')['sounding']['z_reRefPoint_m'])

    dpths = np.zeros((numpings, numbeams))
    xs = np.zeros((numpings, numbeams))
    ys = np.zeros((numpings, numbeams))
    tms = np.zeros(numpings)
    cntrs = np.zeros(numpings)

    install = km.read_first_datagram('IIP')
    read_count = 0
    for offset, size, mtype in zip(km.Index['ByteOffset'],
                                   km.Index['MessageSize'],
                                   km.Index['MessageType']):
        km.FID.seek(offset, 0)
        if mtype == "b'#MRZ'":
            dg = km.read_EMdgmMRZ()
            xs[read_count, :] = np.array(dg['sounding']['x_reRefPoint_m'])
            ys[read_count, :] = np.array(dg['sounding']['y_reRefPoint_m'])
            # we want depths rel tx to align with our sv correction output
            dpths[read_count, :] = np.array(dg['sounding']['z_reRefPoint_m']) - \
                float(install['install_txt']['transducer_1_vertical_location'])
            tms[read_count] = dg['header']['dgtime']
            cntrs[read_count] = dg['cmnPart']['pingCnt']
            read_count += 1

    if read_count != numpings:
        raise ValueError('kmall index count for MRZ records does not match actual records read')
    cntrsorted = np.argsort(cntrs)  # ideally this would do it, but we have to sort by prim/stbd arrays when cntr/times
    # are equal between heads for dual head
    tms = tms[cntrsorted]
    xs = xs[cntrsorted]
    ys = ys[cntrsorted]
    dpths = dpths[cntrsorted]
    cntrs = cntrs[cntrsorted]
    km.closeFile()

    return xs, ys, dpths, tms, cntrs


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

    mbes_extension = os.path.splitext(filname)[1]
    if mbes_extension == '.all':
        print('Reading from xyz88/.all file with par Allread...')
        kongs_x, kongs_y, kongs_z, kongs_tm, kongs_cntrs = xyz_from_allfile(filname)
    elif mbes_extension == '.kmall':
        print('Reading from MRZ/.kmall file with kmall reader...')
        kongs_x, kongs_y, kongs_z, kongs_tm, kongs_cntrs = xyz_from_kmallfile(filname)
    else:
        raise NotImplementedError('Only .all and .kmall file types are supported')
    print('Reading and processing from raw raw_ping/.all file with Kluster...')
    fq, dset = return_svcorr_xyz(filname, visualizations=visualizations)

    print('Plotting...')
    if kongs_tm[0] == 0.0:
        # seen this with EM710 data, the xyz88 dump has zeros arrays at the start, find the first nonzero time
        #    (assuming it starts in the first 100 times)
        print('Found empty arrays in xyz88, seems common with EM710 data')
        first_nonzero = np.where(kongs_tm[:100] != 0.0)[0][0]
        kongs_x = kongs_x[first_nonzero:]
        kongs_y = kongs_y[first_nonzero:]
        kongs_z = kongs_z[first_nonzero:]
        kongs_tm = kongs_tm[first_nonzero:]
    if kongs_x.shape != dset.alongtrack.shape:
        print('Found incompatible par/Kluster data sets.  Kluster x shape {}, par x shape {}'.format(dset.alongtrack.shape,
                                                                                                     kongs_x.shape))
    if fq.multibeam.is_dual_head():
        print('WANRING: I have not figured out the comparison of xyz88/kluster generated data with dual head systems.' +
              'The ping order is different and the ping counters are all the same across heads.')

    # pick some indexes interspersed in the array to visualize
    if analysis_mode == 'even':
        idx = np.arange(0, len(kongs_z), int(len(kongs_z) / numplots), dtype=np.int32)
    elif analysis_mode == 'random':
        idx = np.random.randint(0, len(kongs_z), size=int(numplots))
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
            ki, prev_index = _dual_head_sort(i, dset.acrosstrack, kongs_y, prev_index)
        elif (np.abs(kongs_tm[i - 1] - kongs_tm[i]) > 0.01) or (np.abs(kongs_tm[i + 1] - kongs_tm[i]) > 0.01):  # dual ping
            ki = _single_head_sort(i, dset.alongtrack, kongs_x)
        else:
            ki = i
        lbls.append(kongs_tm[ki])
        myz_plt.plot(dset.depthoffset[i])
        kongsz_plt.plot(kongs_z[ki])
        alongdif_plt.plot(dset.alongtrack[i] - kongs_x[ki])
        acrossdif_plt.plot(dset.acrosstrack[i] - kongs_y[ki])
        zvaldif_plt.plot(dset.depthoffset[i] - kongs_z[ki])

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
