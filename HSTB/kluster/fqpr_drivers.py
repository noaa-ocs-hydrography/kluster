"""
fqpr_drivers = holding place for all the file level access methods that are contained in the HSTB.drivers repository.

Makes adding a new multibeam format a little easier, as if you have a new driver that can be included in all the relevant
functions here, (and you add the format to kluster_variables supported_XXXXXX list) it will work in Kluster.
"""

import os
import numpy as np

from HSTB.kluster import kluster_variables
from HSTB.drivers import kmall, par3, sbet, svp, PCSio, prr3

par_sonar_translator = par3.sonar_translator
kmall_sonar_translator = kmall.sonar_translator
sonar_reference_point = {'.all': ['tx_x', 'tx_y', 'tx_z'],
                         '.kmall': ['tx_x', 'tx_y', 'tx_z'],
                         '.s7k': ['rx_x', 'tx_y', 'rx_z']}


def _check_multibeam_file(multibeam_file: str):
    fileext = os.path.splitext(multibeam_file)[1]
    if fileext not in kluster_variables.supported_multibeam:
        raise NotImplementedError('fqpr_drivers: File ({}) is not a Kluster supported multibeam file ({})'.format(multibeam_file, kluster_variables.supported_multibeam))


def _check_sbet_file(sbet_file: str):
    fileext = os.path.splitext(sbet_file)[1]
    if fileext not in kluster_variables.supported_ppnav:
        raise NotImplementedError('fqpr_drivers: File ({}) is not a Kluster supported post processed navigation (SBET) file ({})'.format(sbet_file, kluster_variables.supported_ppnav))


def _check_pos_file(pos_file: str):
    fileext = os.path.splitext(pos_file)[1]
    try:
        int(fileext[1]), int(fileext[2]), int(fileext[3])
    except:
        raise NotImplementedError('fqpr_drivers: File ({}) is not a Kluster supported position (POS) file (.000 -> .999)'.format(pos_file))


def _check_export_log_file(log_file: str):
    fileext = os.path.splitext(log_file)[1]
    if fileext not in kluster_variables.supported_ppnav_log:
        raise NotImplementedError('fqpr_drivers: File ({}) is not a Kluster supported export log file ({})'.format(log_file, kluster_variables.supported_ppnav_log))


def _check_svp_file(svp_file: str):
    fileext = os.path.splitext(svp_file)[1]
    if fileext not in kluster_variables.supported_sv:
        raise NotImplementedError('fqpr_drivers: File ({}) is not a Kluster supported sound velocity file ({})'.format(svp_file, kluster_variables.supported_sv))


def fast_read_multibeam_metadata(multibeam_file: str, gather_times: bool = True, gather_serialnumber: bool = True):
    """
    Return metadata from a multibeam file using the fast read methods.  Fast read methods allow getting small amounts of
    data without reading the entire file.  These include: the start and end time of the file in utc seconds, the serial
    number(s) of the multibeam sonar in the file.  Use gather_times and gather_serialnumber to select which/both of these options.

    Multibeam file must be one of the multibeam files that we support in Kluster, see kluster_variables.supported_multibeam

    Parameters
    ----------
    multibeam_file
        multibeam file
    gather_times
        if True, returns the start and end time of the file
    gather_serialnumber
        if True, returns the serial number(s) of the multibeam sonar in the file

    Returns
    -------
    str
        the type of multibeam file discovered, i.e. 'kongsberg_all'
    list
        [UTC start time in seconds, UTC end time in seconds] or None if gather_times is False
    list
        [serialnumber: int, secondaryserialnumber: int, sonarmodelnumber: str] or None if gather_serialnumber is False
    """

    _check_multibeam_file(multibeam_file)
    fileext = os.path.splitext(multibeam_file)[1]
    if fileext == '.all':
        mtype = 'kongsberg_all'
        aread = par3.AllRead(multibeam_file)
        if gather_times:
            start_end = aread.fast_read_start_end_time()
        else:
            start_end = None
        if gather_serialnumber:
            serialnums = aread.fast_read_serial_number()
        else:
            serialnums = None
        aread.close()
    elif fileext == '.kmall':
        mtype = 'kongsberg_kmall'
        km = kmall.kmall(multibeam_file)
        if gather_times:
            start_end = km.fast_read_start_end_time()
        else:
            start_end = None
        if gather_serialnumber:
            serialnums = km.fast_read_serial_number()
        else:
            serialnums = None
        km.closeFile()
    elif fileext == '.s7k':
        mtype = 'reson_s7k'
        skread = prr3.X7kRead(multibeam_file)
        if gather_times:
            start_end = skread.fast_read_start_end_time()
        else:
            start_end = None
        if gather_serialnumber:
            serialnums = skread.fast_read_serial_number()
        else:
            serialnums = None
        skread.close()
    else:
        raise NotImplementedError('fqpr_drivers: {} is supported by kluster, but not currently supported by fast_read_multibeam_metadata'.format(multibeam_file))
    return mtype, start_end, serialnums


def return_xyz_from_multibeam(multibeam_file: str):
    """
    Return the already sound velocity corrected data that is in the multibeam file.  We use this to compare with Kluster
    data in a couple functions.

    Parameters
    ----------
    multibeam_file
        multibeam file of interest

    Returns
    -------
    np.ndarray
        one dimensional array of acrosstrack for the soundings
    np.ndarray
        one dimensional array of alongtrack for the soundings
    np.ndarray
        one dimensional array of depth offsets for the soundings
    np.ndarray
        one dimensional array of utc timestamps for the soundings
    np.ndarray
        one dimensional array of ping counters for the soundings
    """

    _check_multibeam_file(multibeam_file)
    mbes_extension = os.path.splitext(multibeam_file)[1]
    if mbes_extension == '.all':
        print('Reading from xyz88/.all file with par Allread...')
        x, y, z, times, counters = _xyz_from_allfile(multibeam_file)
    elif mbes_extension == '.kmall':
        print('Reading from MRZ/.kmall file with kmall reader...')
        x, y, z, times, counters = _xyz_from_kmallfile(multibeam_file)
    else:
        raise NotImplementedError('fqpr_drivers: {} is supported by kluster, but not currently supported by return_xyz_from_multibeam'.format(multibeam_file))
    return x, y, z, times, counters


def _validate_sequential_read_attitude(recs: dict):
    required_attitude = ['time', 'roll', 'pitch', 'heave', 'heading']
    required_attitude_dtype = ['float64', 'float32', 'float32', 'float32', 'float32']
    try:
        assert all([pms in recs['attitude'] for pms in required_attitude])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find required attitude records.  Required: {required_attitude}, Found: {list(recs["attitude"].keys())}')
    try:
        assert all([recs['attitude'][pms].size == recs['attitude']['time'].size for pms in required_attitude])
    except AssertionError:
        raise ValueError(f'sequential_read: All attitude records must be of the same size. Records: {required_attitude}, Sizes: {[recs["attitude"][pms].size for pms in required_attitude]}')
    try:
        assert all([recs['attitude'][pms].dtype == required_attitude_dtype[cnt] for cnt, pms in enumerate(required_attitude)])
    except AssertionError:
        raise ValueError(f'sequential_read: All attitude records must be of the required data type. Records: {required_attitude}, '
                         f'Dtype: {[recs["attitude"][pms].dtype for pms in required_attitude]}, Required Dtype: {required_attitude_dtype}')


def _validate_sequential_read_installation(recs: dict):
    required_installation_params = ['time', 'serial_one', 'serial_two', 'installation_settings']
    # the transducer entries here correspond to each tx/rx.  The number depends on where your sonar ends up in
    #   xarray_conversion.sonar_translator.  But every sonar has transducer 1, so just check for that.
    required_keys = ['sonar_model_number', 'transducer_1_vertical_location',
                     'transducer_1_along_location', 'transducer_1_athwart_location',
                     'transducer_1_heading_angle', 'transducer_1_roll_angle', 'transducer_1_pitch_angle',
                     'position_1_time_delay', 'position_1_vertical_location', 'position_1_along_location',
                     'position_1_athwart_location', 'motion_sensor_1_time_delay',
                     'motion_sensor_1_vertical_location', 'motion_sensor_1_along_location',
                     'motion_sensor_1_athwart_location', 'motion_sensor_1_roll_angle',
                     'motion_sensor_1_pitch_angle', 'motion_sensor_1_heading_angle',
                     'waterline_vertical_location', 'active_position_system_number',
                     'active_heading_sensor', 'position_1_datum']
    try:
        assert all([pms in recs['installation_params'] for pms in required_installation_params])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find required installation parameter records.  Required: {required_installation_params}, Found: {list(recs["installation_params"].keys())}')
    try:
        assert recs['installation_params']['time'].size == recs['installation_params']['installation_settings'].size
    except AssertionError:
        raise ValueError(f'sequential_read: All installation parameter records must be of the same size. Records: {["time", "installation_settings"]}, Sizes: {[recs["installation_params"][pms].size for pms in ["time", "installation_settings"]]}')
    if recs['installation_params']['installation_settings'].size:
        for irec in recs['installation_params']['installation_settings']:
            for ky in required_keys:
                try:
                    assert ky in irec
                except AssertionError:
                    raise ValueError(f'sequential_read: {ky} not found in installation parameters entry: {irec}')


def _validate_sequential_read_ping(recs: dict):
    # two options for runtime parameters.  They can either be a value per ping stored in the ping records, or they can
    #   be in a separate record that shows intermittently throughout the file and needs to be interpolated to the ping
    #   time during xarray conversion (all format).
    if recs['format'] in ['all']:
        required_ping = ['time', 'counter', 'soundspeed', 'serial_num', 'tiltangle', 'delay', 'frequency', 'beampointingangle',
                         'txsector_beam', 'detectioninfo', 'qualityfactor', 'traveltime', 'processing_status']
        required_ping_dtype = ['float64', 'uint32', 'float32', 'uint16', 'float32', 'float32', 'int32', 'float32',
                               'uint8', 'int32', 'float32', 'float32', 'uint8']
    else:
        required_ping = ['time', 'counter', 'soundspeed', 'serial_num', 'tiltangle', 'delay', 'frequency', 'beampointingangle',
                         'txsector_beam', 'detectioninfo', 'qualityfactor', 'traveltime', 'processing_status', 'mode',
                         'modetwo', 'yawpitchstab']
        required_ping_dtype = ['float64', 'uint32', 'float32', 'uint16', 'float32', 'float32', 'int32', 'float32',
                               'uint8', 'int32', 'float32', 'float32', 'uint8', 'u2-u5', 'u2-u5', 'u2-u5']
    try:
        assert all([pms in recs['ping'] for pms in required_ping])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find required ping records.  Required: {required_ping}, Found: {list(recs["ping"].keys())}')
    try:
        assert all([recs['ping'][pms].shape == recs['ping']['time'].shape for pms in required_ping if pms in kluster_variables.subset_variable_1d])
    except AssertionError:
        raise ValueError(f'sequential_read: All ping records must be of the same size. Records: {[rec for rec in required_ping if rec in kluster_variables.subset_variable_1d]}, Sizes: {[recs["ping"][pms].size for pms in [rec for rec in required_ping if rec in kluster_variables.subset_variable_1d]]}')
    try:
        assert all([recs['ping'][pms].shape == recs['ping']['frequency'].shape for pms in required_ping if pms in kluster_variables.subset_variable_2d])
    except AssertionError:
        raise ValueError(f'sequential_read: All ping records must be of the same size. Records: {[rec for rec in required_ping if rec in kluster_variables.subset_variable_2d]}, Sizes: {[recs["ping"][pms].size for pms in [rec for rec in required_ping if rec in kluster_variables.subset_variable_2d]]}')
    if 'u2-u5' in required_ping_dtype:  # this is a place holder to put in here to allow us to have a range of lengths for these variables
        for tmprec in ['mode', 'modetwo', 'yawpitchstab']:
            try:
                assert recs['ping'][tmprec].dtype in ['<U2', '<U3', '<U4', '<U5']
                required_ping_dtype[required_ping.index(tmprec)] = str(recs['ping'][tmprec].dtype)
            except AssertionError:
                raise ValueError(f'sequential_read: ping record {tmprec} must be of the required data type. '
                                 f"Dtype: {recs['ping'][tmprec].dtype}, Required Dtype: {['<U2', '<U3', '<U4', '<U5']}")
    try:
        assert all([recs['ping'][pms].dtype == required_ping_dtype[cnt] for cnt, pms in enumerate(required_ping)])
    except AssertionError:
        raise ValueError(f'sequential_read: All ping records must be of the required data type. Records: {required_ping}, '
                         f'Dtype: {[recs["ping"][pms].dtype for pms in required_ping]}, Required Dtype: {required_ping_dtype}')
    if 'reflectivity' in recs['ping']:
        try:
            assert recs['ping']['reflectivity'].dtype == 'float32'
        except AssertionError:
            raise ValueError(f'sequential_read: expected reflectivity record with dtype of "float32", found {recs["ping"]["reflectivity"].dtype}')


def _validate_sequential_read_runtime(recs: dict):
    # two options for runtime parameters.  They can either be a value per ping stored in the ping records, or they can
    #   be in a separate record that shows intermittently throughout the file and needs to be interpolated to the ping
    #   time during xarray conversion (all format).
    if recs['format'] in ['all']:
        required_runtime_params = ['time', 'mode', 'modetwo', 'yawpitchstab', 'runtime_settings']
        required_runtime_params_dtype = ['float64', 'u2-u5', 'u2-u5', 'u2-u5', 'object']
    else:
        required_runtime_params = ['time', 'runtime_settings']
        required_runtime_params_dtype = ['float64', 'object']
    try:
        assert all([pms in recs['runtime_params'] for pms in required_runtime_params])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find required runtime parameter records.  Required: {required_runtime_params}, Found: {list(recs["runtime_params"].keys())}')
    try:
        assert all([recs['runtime_params'][pms].size == recs['runtime_params']['time'].size for pms in required_runtime_params])
    except AssertionError:
        raise ValueError(f'sequential_read: All runtime parameter records must be of the same size. Records: {required_runtime_params}, Sizes: {[recs["runtime_params"][pms].size for pms in required_runtime_params]}')
    if 'u2-u5' in required_runtime_params_dtype:  # this is a place holder to put in here to allow us to have a range of lengths for these variables
        for tmprec in ['mode', 'modetwo', 'yawpitchstab']:
            try:
                assert recs['runtime_params'][tmprec].dtype in ['<U2', '<U3', '<U4', '<U5']
                required_runtime_params_dtype[required_runtime_params.index(tmprec)] = str(recs['runtime_params'][tmprec].dtype)
            except AssertionError:
                raise ValueError(f'sequential_read: runtime parameters record {tmprec} must be of the required data type. '
                                 f"Dtype: {recs['runtime_params'][tmprec].dtype}, Required Dtype: {['<U2', '<U3', '<U4', '<U5']}")
    try:
        assert all([recs['runtime_params'][pms].dtype == required_runtime_params_dtype[cnt] for cnt, pms in enumerate(required_runtime_params)])
    except AssertionError:
        raise ValueError(f'sequential_read: All runtime parameter records must be of the required data type. Records: {required_runtime_params}, '
                         f'Dtype: {[recs["runtime_params"][pms].dtype for pms in required_runtime_params]}, Required Dtype: {required_runtime_params_dtype}')


def _validate_sequential_read_navigation(recs: dict):
    required_navigation = ['time', 'latitude', 'longitude']
    required_navigation_dtype = ['float64', 'float64', 'float64']
    try:
        assert all([pms in recs['navigation'] for pms in required_navigation])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find required navigation records.  Required: {required_navigation}, Found: {list(recs["navigation"].keys())}')
    try:
        assert all([recs['navigation'][pms].size == recs['navigation']['time'].size for pms in required_navigation])
    except AssertionError:
        raise ValueError(f'sequential_read: All navigation records must be of the same size. Records: {required_navigation}, Sizes: {[recs["navigation"][pms].size for pms in required_navigation]}')
    try:
        assert all([recs['navigation'][pms].dtype == required_navigation_dtype[cnt] for cnt, pms in enumerate(required_navigation)])
    except AssertionError:
        raise ValueError(f'sequential_read: All navigation parameter records must be of the required data type. Records: {required_navigation}, '
                         f'Dtype: {[recs["navigation"][pms].dtype for pms in required_navigation]}, Required Dtype: {required_navigation_dtype}')
    if 'altitude' in recs['navigation']:
        try:
            assert recs['navigation']['altitude'].dtype == 'float32'
        except AssertionError:
            raise ValueError(f'sequential_read: expected altitude record with dtype of "float32", found {recs["navigation"]["altitude"].dtype}')


def _validate_sequential_read_profile(recs: dict):
    required_profile = ['time', 'depth', 'soundspeed']
    if 'profile' in recs:
        assert all([pms in recs['profile'] for pms in required_profile])


def _validate_sequential_read(recs: dict):
    """
    the return from sequential_read_multibeam should be a nested dict of records from the multibeam file.  This method
    will ensure that all the required records are there, and the shape of those records makes sense

    Parameters
    ----------
    recs
        dictionary return from sequential_read_multibeam
    """

    required_categories = ['attitude', 'installation_params', 'ping', 'navigation', 'runtime_params']

    try:
        assert all([ct in recs for ct in required_categories])
    except AssertionError:
        raise ValueError(f'sequential_read: Unable to find all required categories in multibeam data.  Required: {required_categories}, Found: {list(recs.keys())}')

    _validate_sequential_read_attitude(recs)
    _validate_sequential_read_installation(recs)
    _validate_sequential_read_ping(recs)
    _validate_sequential_read_runtime(recs)
    _validate_sequential_read_navigation(recs)
    _validate_sequential_read_profile(recs)

    assert '.' + recs['format'] in kluster_variables.supported_multibeam


def sequential_read_multibeam(multibeam_file: str, start_pointer: int = 0, end_pointer: int = 0, first_installation_rec: bool = False):
    """
    Run the sequential read function built in to all multibeam drivers in Kluster.  Sequential read takes a multibeam file
    (with an optional start/end pointer in bytes) and reads all the datagrams of interest sequentially, skipping any that
    are not in the required datagram lookups.

    Parameters
    ----------
    multibeam_file
        multibeam file of interest
    start_pointer
        the start pointer that we start the read at
    end_pointer
        the end pointer where we finish the read
    first_installation_rec
        if True, will just read the installation parameters entry and finish

    Returns
    -------
    dict
        nested dictionary object containing all the numpy arrays for the data of interest
    """

    _check_multibeam_file(multibeam_file)
    multibeam_extension = os.path.splitext(multibeam_file)[1]
    if multibeam_extension == '.all':
        ar = par3.AllRead(multibeam_file, start_ptr=start_pointer, end_ptr=end_pointer)
        recs = ar.sequential_read_records(first_installation_rec=first_installation_rec)
        ar.close()
    elif multibeam_extension == '.kmall':
        km = kmall.kmall(multibeam_file)
        # kmall doesnt have ping-wise serial number in header, we have to provide it from install params
        serial_translator = km.fast_read_serial_number_translator()
        recs = km.sequential_read_records(start_ptr=start_pointer, end_ptr=end_pointer, first_installation_rec=first_installation_rec,
                                          serial_translator=serial_translator)
        km.closeFile()
    elif multibeam_extension == '.s7k':
        sk = prr3.X7kRead(multibeam_file, start_ptr=start_pointer, end_ptr=end_pointer)
        recs = sk.sequential_read_records(first_installation_rec=first_installation_rec)
        sk.close()
    else:
        raise NotImplementedError('fqpr_drivers: {} is supported by kluster, but not currently supported by sequential_read_multibeam'.format(multibeam_file))
    _validate_sequential_read(recs)
    return recs


def read_first_fifty_records(file_object):
    print('***********************************************************')
    print('Read First Fifty Records:')
    if isinstance(file_object, par3.AllRead):
        par3.print_some_records(file_object, recordnum=50)
    elif isinstance(file_object, kmall.kmall):
        kmall.print_some_records(file_object, recordnum=50)
    elif isinstance(file_object, PCSio.PCSBaseFile):
        PCSio.print_some_records(file_object, recordnum=50)
    elif isinstance(file_object, np.ndarray):
        sbet.print_some_records(file_object, recordnum=50)
    else:
        print(f'read_first_fifty_records: Unsupported file object: {file_object}')


def kluster_read_test(file_object):
    print('***********************************************************')
    print('Kluster Read Test:')
    if isinstance(file_object, par3.AllRead):
        par3.kluster_read_test(file_object, byte_count=-1)
    else:
        print(f'read_first_fifty_records: Unsupported file object: {file_object}')


def bscorr_generation(file_object, second_file_object):
    print('***********************************************************')
    print('BSCORR Generation:')
    if isinstance(file_object, par3.AllRead):
        par3.build_BSCorr(file_object.infilename, second_file_object.infilename, show_fig=True, save_fig=True)
    else:
        print(f'bscorr_generation: Unsupported file object: {file_object}')


def return_xarray_from_sbet(sbetfiles: list, smrmsgfiles: list = None, logfiles: list = None, weekstart_year: int = None,
                            weekstart_week: int = None, override_datum: str = None, override_grid: str = None,
                            override_zone: str = None, override_ellipsoid: str = None):
    """
    Read all the provided nav files, error files and concatenate the result in to a single xarray dataset.

    Parameters
    ----------
    sbetfiles
        list of full file paths to the sbet files
    smrmsgfiles
        list of full file paths to the smrmsg files
    logfiles
        list of full file paths to the sbet export log files
    weekstart_year
        if you aren't providing a logfile, must provide the year of the sbet here
    weekstart_week
        if you aren't providing a logfile, must provide the week of the sbet here
    override_datum
        provide a string datum identifier if you want to override what is read from the log or you don't have a log, ex: 'NAD83 (2011)'
    override_grid
        provide a string grid identifier if you want to override what is read from the log or you don't have a log, ex: 'Universal Transverse Mercator'
    override_zone
        provide a string zone identifier if you want to override what is read from the log or you don't have a log, ex: 'UTM North 20 (66W to 60W)'
    override_ellipsoid
        provide a string ellipsoid identifier if you want to override what is read from the log or you don't have a log, ex: 'GRS80'

    Returns
    -------
    xarray Dataset
        data and attribution from the sbets relevant to our survey processing
    """

    if smrmsgfiles == [None] or smrmsgfiles == []:
        smrmsgfiles = None
    if logfiles == [None] or logfiles == []:
        logfiles = None
    [_check_sbet_file(fil) for fil in sbetfiles]
    if smrmsgfiles is not None:
        [_check_sbet_file(fil) for fil in smrmsgfiles]
    if logfiles is not None:
        [_check_export_log_file(fil) for fil in logfiles]
    return sbet.sbets_to_xarray(sbetfiles, smrmsgfiles, logfiles, weekstart_year, weekstart_week, override_datum,
                                override_grid, override_zone, override_ellipsoid)


def return_xarray_from_posfiles(posfiles: list, weekstart_year: int, weekstart_week: int):
    """
    Read all the provided pos files, error files and concatenate the result in to a single xarray dataset.

    Parameters
    ----------
    posfiles
        list of full file paths to the pos files
    weekstart_year
        must provide the year of the posfiles here
    weekstart_week
        must provide the gpsweek of the posfiles here

    Returns
    -------
    xarray Dataset
        data and attribution from the posfiles relevant to our survey processing
    """

    [_check_pos_file(fil) for fil in posfiles]
    return PCSio.posfiles_to_xarray(posfiles, weekstart_year, weekstart_week)


def return_offsets_from_posfile(posfile: str):
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

    _check_pos_file(posfile)
    pcs = PCSio.PCSFile(posfile, nCache=0)
    try:
        pcs.CacheHeaders(read_first_msg=(20, '$MSG'))
        msg20 = pcs.GetArray("$MSG", 20)
        data = {'tx_to_antenna_x': round(msg20[0][10], 3), 'tx_to_antenna_y': round(msg20[0][11], 3),
                'tx_to_antenna_z': round(msg20[0][12], 3),
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


def fast_read_sbet_metadata(sbet_file: str):
    """
    Determine the start and end time of the provided sbet file by reading the first and last record.

    Parameters
    ----------
    sbet_file
        full file path to a sbet file

    Returns
    -------
    list
        list of floats, [start time, end time] for the sbet
    """

    _check_sbet_file(sbet_file)
    tms = sbet.sbet_fast_read_start_end_time(sbet_file)
    return tms


def fast_read_errorfile_metadata(smrmsg_file: str):
    """
    Determine the start and end time of the provided smrmsg file by reading the first and last record.

    Parameters
    ----------
    smrmsg_file
        full file path to a smrmsg file

    Returns
    -------
    list
        list of floats, [start time, end time] for the smrmsg file
    """

    _check_sbet_file(smrmsg_file)
    tms = sbet.smrmsg_fast_read_start_end_time(smrmsg_file)
    return tms


def read_pospac_export_log(exportlog_file: str):
    """
    Read the POSPac export log to get the relevant attributes for the exported SBET.  SBET basically has no metadata,
    so this log file it generates is the only way to figure it out.  Log file is plain text, looks something like this:

    --------------------------------------------------------------------------------
    EXPORT Data Export Utility [Jun 18 2018]
    Copyright (c) 1997-2018 Applanix Corporation.  All rights reserved.
    Date : 09/09/18    Time : 17:01:12
    --------------------------------------------------------------------------------
    Mission date        : 9/9/2018
    Input file          : S:\\2018\\...sbet_H13131_251_2702.out
    Output file         : S:\\2018\\...export_H13131_251_2702.out
    Output Rate Type    : Specified Time Interval
    Time Interval       : 0.020
    Start time          : 0.000
    End time            : 999999.000
    UTC offset          : 18.000
    Lat/Lon units       : Radians
    Height              : Ellipsoidal
    Grid                : Universal Transverse Mercator
    Zone                : UTM North 01 (180W to 174W)
    Datum               : NAD83 (2011)
    Ellipsoid           : GRS 1980
    Transformation type : 14 Parameter
    Target epoch        : 2018.687671
    --------------------------------------------------------------------------------
    Processing completed.

    Parameters
    ----------
    exportlog_file: str, file path to the log file

    Returns
    -------
    attrs: dict, relevant data from the log file as a dictionary
    """

    _check_export_log_file(exportlog_file)
    loginfo = sbet.get_export_info_from_log(exportlog_file)
    return loginfo


def is_sbet(sbet_file: str):
    """
    Check if the file is an sbet.  Ideally we just rely on the checking if the file contains an even number of 17 doubles,
    but add in the time check just in case.

    Parameters
    ----------
    sbet_file
        file path to a POSPac sbet file

    Returns
    -------
    bool
        True if file is an sbet, False if not
    """

    _check_sbet_file(sbet_file)
    return sbet.is_sbet(sbet_file)


def is_smrmsg(smrmsg_file: str):
    """
    Check if the file is an smrmsg file.  Ideally we just rely on the checking if the file contains an even number of 10 doubles,
    but add in the time check just in case.

    Parameters
    ----------
    smrmsg_file
        file path to a POSPac smrmsg file

    Returns
    -------
    bool
        True if file is an smrmsg, False if not
    """

    _check_sbet_file(smrmsg_file)
    return sbet.is_smrmsg(smrmsg_file)


def read_soundvelocity_file(svp_file: str):
    """
    Export out the information in the svp file as a dict.  Keys include 'number_of_profiles', 'svp_julian_day',
    'svp_time_utc', 'latitude', 'longitude', 'source_epsg', 'utm_zone', 'utm_hemisphere', 'number_of_layers', 'profiles'.

    Returns
    -------
    dict
        dictionary of the class information
    """

    _check_svp_file(svp_file)
    svp_object = svp.CarisSvp(svp_file)
    svp_dict = svp_object.return_dict()
    return svp_dict


def _xyz_from_allfile(filname: str):
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

    pfil = par3.AllRead(filname)
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


def _xyz_from_kmallfile(filname: str):
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

    km = kmall.kmall(filname)
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
