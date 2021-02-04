import os, shutil
import time
import logging

from HSTB.kluster import fqpr_generation, fqpr_project, xarray_conversion, fqpr_intelligence
from .test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr  # relative import as tests directory can vary in location depending on how kluster is installed
from HSTB.kluster.xarray_helpers import interp_across_chunks
from HSTB.kluster.fqpr_convenience import *


datapath = ''


def test_get_orientation_vectors():
    """
    get_orientation_vectors test for the em2040 dataset
    """

    get_orientation_vectors(dset='real')


def test_get_orientation_vectors_dualhead():
    """
    get_orientation_vectors test for the em2040 dualrx/dualtx dataset
    """

    get_orientation_vectors(dset='realdualhead')


def test_build_beam_pointing_vector():
    """
    build_beam_pointing_vector test for the em2040 dataset
    """

    build_beam_pointing_vector(dset='real')


def test_build_beam_pointing_vector_dualhead():
    """
    build_beam_pointing_vector test for the em2040 dualrx/dualtx dataset
    """

    build_beam_pointing_vector(dset='realdualhead')


def test_sv_correct():
    """
    sv_correct test for the em2040 dataset
    """

    sv_correct(dset='real')


def test_sv_correct_dualhead():
    """
    sv_correct test for the em2040 dualrx/dualtx dataset
    """

    sv_correct(dset='realdualhead')


def test_georef_xyz():
    """
    georef_xyz test for the em2040 dataset
    """

    georef_xyz(dset='real')


def test_georef_xyz_dualhead():
    """
    georef_xyz test for the em2040 dualrx/dualtx dataset
    """

    georef_xyz(dset='realdualhead')


def test_find_testfile():
    """
    Find the test file we use for the next tests
    """
    testfile_path, expected_output = get_testfile_paths()
    if not os.path.exists(testfile_path):
        print('test_find_testfile: could not find {}'.format(testfile_path))
    assert os.path.exists(testfile_path)


def test_intelligence():
    """
    Test fqpr intelligence by kicking off a folder monitoring session, finding the test multibeam file, and checking
    the resulting actions to see if the conversion action matches expectations.
    """

    global datapath

    testfile_path, expected_output = get_testfile_paths()
    proj = fqpr_project.create_new_project(os.path.dirname(testfile_path))
    proj_path = os.path.join(os.path.dirname(testfile_path), 'kluster_project.json')
    fintel = fqpr_intelligence.FqprIntel(proj)
    fintel.start_folder_monitor(os.path.dirname(testfile_path), is_recursive=True)
    time.sleep(3)  # pause until the folder monitoring finds the multibeam file

    assert os.path.exists(proj_path)
    os.remove(proj_path)
    assert str(fintel.action_container) == "FqprActionContainer: 1 actions of types: ['multibeam']"
    assert len(fintel.action_container.actions) == 1

    action = fintel.action_container.actions[0]
    assert action.text[0:25] == 'Convert 1 multibeam lines'
    assert action.action_type == 'multibeam'
    assert action.priority == 1
    assert action.is_running == False
    assert len(action.input_files) == 1
    assert action.kwargs is None
    assert action.args[2:] == [None, False, True]

    fintel.execute_action()
    action = fintel.action_container.actions[0]
    assert action.text[0:21] == 'Run all processing on'
    assert action.action_type == 'processing'
    assert action.priority == 5
    assert action.is_running is False
    assert len(action.input_files) == 0
    assert action.kwargs == {'run_orientation': True, 'orientation_initial_interpolation': False, 'run_beam_vec': True,
                             'run_svcorr': True, 'add_cast_files': [], 'run_georef': True, 'use_epsg': False, 'use_coord': True,
                             'epsg': None, 'coord_system': 'NAD83', 'vert_ref': 'waterline'}
    assert isinstance(action.args[0], fqpr_generation.Fqpr)

    fintel.clear()
    datapath = action.args[0].multibeam.converted_pth
    action.args[0].close()
    action.args[0] = None

    cleanup_after_tests()


def test_process_testfile():
    """
    Run conversion and basic processing on the test file
    """
    global datapath

    testfile_path, expected_output = get_testfile_paths()
    out = convert_multibeam(testfile_path)
    out = process_multibeam(out)

    number_of_sectors = len(out.multibeam.raw_ping)
    rp = out.multibeam.raw_ping[0].isel(time=0).isel(beam=0)
    firstbeam_angle = rp.beampointingangle.values
    firstbeam_traveltime = rp.traveltime.values
    first_counter = rp.counter.values
    first_dinfo = rp.detectioninfo.values
    first_mode = rp.mode.values
    first_modetwo = rp.modetwo.values
    first_ntx = rp.ntx.values
    firstbeam_procstatus = rp.processing_status.values
    firstbeam_qualityfactor = rp.qualityfactor.values
    first_soundspeed = rp.soundspeed.values
    first_tiltangle = rp.tiltangle.values
    first_delay = rp.delay.values
    first_frequency = rp.frequency.values
    first_yawpitch = rp.yawpitchstab.values
    firstcorr_angle = rp.corr_pointing_angle.values
    firstcorr_altitude = rp.corr_altitude.values
    firstcorr_heave = rp.corr_heave.values
    firstdepth_offset = rp.depthoffset.values
    first_status = rp.processing_status.values
    firstrel_azimuth = rp.rel_azimuth.values
    firstrx = rp.rx.values
    firstthu = rp.thu.values
    firsttvu = rp.tvu.values
    firsttx = rp.tx.values
    firstx = rp.x.values
    firsty = rp.y.values
    firstz = rp.z.values

    assert number_of_sectors == 1
    assert firstbeam_angle == np.float32(74.64)
    assert firstbeam_traveltime == np.float32(0.3360895)
    assert first_counter == 61967
    assert first_dinfo == 2
    assert first_mode == 'FM'
    assert first_modetwo == '__FM'
    assert first_ntx == 3
    assert firstbeam_procstatus == 5
    assert firstbeam_qualityfactor == 42
    assert first_soundspeed == np.float32(1488.6)
    assert first_tiltangle == np.float32(-0.44)
    assert first_delay == np.float32(0.002206038)
    assert first_frequency == 275000
    assert first_yawpitch == 'PY'
    assert firstcorr_angle == np.float32(1.2028906)
    assert firstcorr_altitude == np.float32(0.0)
    assert firstcorr_heave == np.float32(-0.06)
    assert firstdepth_offset == np.float32(92.162)
    assert first_status == 5
    assert firstrel_azimuth == np.float32(4.703383)
    assert np.array_equal(firstrx, np.array([0.7870753, 0.60869384, -0.100021675], dtype=np.float32))
    assert firstthu == np.float32(8.857684)
    assert firsttvu == np.float32(2.5005076)
    assert np.array_equal(firsttx, np.array([0.6074468, -0.79435784, 0.0020107413], dtype=np.float32))
    assert firstx == 539028.45
    assert firsty == 5292783.977
    assert firstz == np.float32(92.742)

    datapath = out.multibeam.converted_pth
    out.close()
    out = None


def test_export_files():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    pths_one = out.export_pings_to_file(file_format='csv', filter_by_detection=True, export_by_identifiers=True)
    pths_two = out.export_pings_to_file(file_format='csv', filter_by_detection=True, export_by_identifiers=False)
    pths_three = out.export_pings_to_file(file_format='las', filter_by_detection=True, export_by_identifiers=True)
    pths_four = out.export_pings_to_file(file_format='las', filter_by_detection=True, export_by_identifiers=False)

    assert len(pths_one) == 6
    assert len(pths_two) == 1
    assert len(pths_three) == 6
    assert len(pths_four) == 1

    out.close()
    out = None
    cleanup_after_tests()


def cleanup_after_tests():
    """
    Clean up after test_intelligence and test_process_testfile
    """

    global datapath

    assert os.path.exists(datapath)
    clear_testfile_data(datapath)
    assert not os.path.exists(datapath)


def get_testfile_paths():
    """
    return the necessary paths for the testfile tests

    Returns
    -------
    str
        absolute file path to the test file
    str
        absolute folder path to the expected output folder
    """

    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    expected_output = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'converted')
    return testfile, expected_output


def clear_testfile_data(expected_output: str):
    """
    remove the converted data

    Parameters
    ----------
    expected_output
        path to the converted data folder
    """

    if os.path.exists(expected_output):
        shutil.rmtree(expected_output)
    proj_file = os.path.join(os.path.dirname(expected_output), 'kluster_project.json')
    if os.path.exists(proj_file):
        os.remove(proj_file)


def get_orientation_vectors(dset='realdualhead'):
    """
    Automated test of fqpr_generation get_orientation_vectors

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
    """

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_tx = [np.array([0.6136555921172974, -0.7895255928982701, 0.008726535498373935])]
        expected_rx = [np.array([0.7834063072490661, 0.6195440454987808, -0.04939365798750035])]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_tx = [np.array([-0.8173967230596009, -0.5756459946918305, -0.022232663846213512]),
                       np.array([-0.818098137098556, -0.5749317404941526, -0.013000579640495315])]
        expected_rx = [np.array([0.5707251056249292, -0.8178104883650188, 0.07388380848347877]),
                       np.array([0.5752302545527056, -0.8157217016726686, -0.060896177270015645])]
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    # dump_data/delete_futs set the workflow to either keeping everything in memory after completion (False) or writing
    #     data to disk (both are True).  Could probably condense these arguments to one argument in the future.
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)

    # arrays of computed vectors
    sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
    tstmp = list(fq.intermediate_dat[sysid[0]]['orientation'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['orientation'][tstmp][0][0] for s in sysid]

    # we examine the tx vector for each sector (not beam based) and the rx vector for each sector's first beam (rx
    #     vectors change per beam, as attitude changes based on beam traveltime)
    txvecdata = [ld[0].values[0][0] for ld in loaded_data]
    rxvecdata = [ld[1].values[0][0] for ld in loaded_data]

    print('ORIENTATION {}'.format(dset))
    print([x for y in txvecdata for x in y.flatten()])
    print([x for y in rxvecdata for x in y.flatten()])

    # check for the expected tx orientation vectors
    assert np.array_equal(expected_tx, txvecdata)

    # check for the expected rx orientation vectors
    assert np.array_equal(expected_rx, rxvecdata)

    print('Passed: get_orientation_vectors')


def build_beam_pointing_vector(dset='realdualhead'):
    """
    Automated test of fqpr_generation build_beam_pointing_vector

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
    """

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_ba = [np.array([4.697702878191307, 4.697679369354361, 4.697655798111743])]
        expected_bda = [np.array([1.209080677036444, 1.2074367547912856, 1.2057926824074374])]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_ba = [np.array([4.7144694193229295, 4.714486234983295, 4.714503034301336]),
                       np.array([4.72527541256665, 4.725306685935214, 4.725337688174256])]
        expected_bda = [np.array([1.2049043892451596, 1.20385629874863, 1.2028083855561609]),
                        np.array([0.5239366688735714, 0.5181768253459791, 0.5124169874635531])]
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)

    # arrays of computed vectors
    sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
    tstmp = list(fq.intermediate_dat[sysid[0]]['bpv'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['bpv'][tstmp][0][0] for s in sysid]

    ba_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
    bda_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]

    print('BEAMPOINTING {}'.format(dset))
    print([x for y in ba_data for x in y.flatten()])
    print([x for y in bda_data for x in y.flatten()])

    # beam azimuth check
    assert np.array_equal(ba_data, expected_ba)

    # beam depression angle check
    assert np.array_equal(bda_data, expected_bda)

    print('Passed: build_beam_pointing_vector')


def sv_correct(dset='realdualhead'):
    """
    Automated test of fqpr_generation sv_correct

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
    """

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_x = [np.array([-3.419, -3.406, -3.392])]
        expected_y = [np.array([-232.877, -231.562, -230.249])]
        expected_z = [np.array([91.139, 91.049, 90.955])]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_x = [np.array([0.692, 0.693, 0.693]),
                      np.array([0.567, 0.565, 0.564])]
        expected_y = [np.array([-59.992, -59.945, -59.848]),
                      np.array([-9.351, -9.215, -9.078])]
        expected_z = [np.array([18.305, 18.342, 18.359]),
                      np.array([18.861, 18.873, 18.883])]
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)

    # arrays of computed vectors
    sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
    tstmp = list(fq.intermediate_dat[sysid[0]]['sv_corr'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0] for s in sysid]

    x_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
    y_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]
    z_data = [ld[2].isel(time=0).values[0:3] for ld in loaded_data]

    print('SVCORR {}'.format(dset))
    print([x for y in x_data for x in y.flatten()])
    print([x for y in y_data for x in y.flatten()])
    print([x for y in z_data for x in y.flatten()])

    # forward offset check
    assert np.array_equal(x_data, expected_x)

    # acrosstrack offset check
    assert np.array_equal(y_data, expected_y)

    # depth offset check
    assert np.array_equal(z_data, expected_z)

    print('Passed: sv_correct')


def georef_xyz(dset='realdualhead'):
    """
    Automated test of fqpr_generation sv_correct

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
    """

    vert_ref = 'waterline'
    datum = 'NAD83'

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_x = [np.array([539017.745, 539018.535, 539019.322], dtype=np.float64)]
        expected_y = [np.array([5292788.295, 5292789.346, 5292790.396], dtype=np.float64)]
        expected_z = [np.array([91.789, 91.699, 91.605], dtype=np.float32)]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_x = [np.array([492984.906, 492984.867, 492984.787], dtype=np.float64),
                      np.array([492943.083, 492942.971, 492942.859], dtype=np.float64)]
        expected_y = [np.array([3365068.225, 3365068.25, 3365068.305], dtype=np.float64),
                      np.array([3365096.742, 3365096.82, 3365096.898], dtype=np.float64)]
        expected_z = [np.array([22.087, 22.124, 22.141], dtype=np.float32),
                      np.array([22.692, 22.704, 22.714], dtype=np.float32)]
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False, delete_futs=False)

    # arrays of computed vectors
    sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
    tstmp = list(fq.intermediate_dat[sysid[0]]['xyz'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['xyz'][tstmp][0][0] for s in sysid]

    x_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
    y_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]
    z_data = [ld[2].isel(time=0).values[0:3] for ld in loaded_data]

    print('GEOREF {}'.format(dset))
    print([x for y in x_data for x in y.flatten()])
    print([x for y in y_data for x in y.flatten()])
    print([x for y in z_data for x in y.flatten()])

    # easting
    assert np.array_equal(x_data, expected_x)

    # northing
    assert np.array_equal(y_data, expected_y)

    # depth
    assert np.array_equal(z_data, expected_z)
    
    print('Passed: georef_xyz')


def test_interp_across_chunks():
    synth = load_dataset(RealFqpr(), skip_dask=False)
    # 11 attitude values, chunking by 4 gives us three chunks
    # att.chunks
    # Out[10]: Frozen(SortedKeysDict({'time': (4, 4, 3)}))
    att = synth.raw_att.chunk(4)
    times_interp_to = xr.DataArray(np.array([1495563084.455, 1495563084.490, 1495563084.975]), dims={'time'},
                                   coords={'time': np.array([1495563084.455, 1495563084.490, 1495563084.975])})
    dask_interp_att = interp_across_chunks(att, times_interp_to, dimname='time', daskclient=synth.client)
    interp_att = interp_across_chunks(att, times_interp_to, dimname='time')

    expected_att = xr.Dataset({'heading': (['time'], np.array([307.8539977551496, 307.90348427192055, 308.6139892100822])),
                               'heave': (['time'], np.array([0.009999999776482582, 0.009608692733222632, -0.009999999776482582])),
                               'roll': (['time'], np.array([0.4400004684929343, 0.07410809820512047, -4.433999538421631])),
                               'pitch': (['time'], np.array([-0.5, -0.5178477924436871, -0.3760000467300415]))},
                              coords={'time': np.array([1495563084.455, 1495563084.49, 1495563084.975])})

    # make sure the dask/non-dask methods line up
    assert np.all(dask_interp_att.time == interp_att.time).compute()
    assert np.all(dask_interp_att.heading == interp_att.heading).compute()
    assert np.all(dask_interp_att.heave == interp_att.heave).compute()
    assert np.all(dask_interp_att.pitch == interp_att.pitch).compute()
    assert np.all(dask_interp_att['roll'] == interp_att['roll']).compute()

    # make sure the values line up with what we would expect
    assert np.all(dask_interp_att.time == expected_att.time).compute()
    assert np.all(dask_interp_att.heading == expected_att.heading).compute()
    assert np.all(dask_interp_att.heave == expected_att.heave).compute()
    assert np.all(dask_interp_att.pitch == expected_att.pitch).compute()
    assert np.all(dask_interp_att['roll'] == expected_att['roll']).compute()

    print('Passed: interp_across_chunks')


def test_basesurface():
    testz = np.array([1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2., 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0,
                      3.1, 3.2, 3.3, 3.4, 3.5, 3.6])
    testx = np.array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5])
    testy = np.array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
    testbs = BaseSurface(testx, testy, testz, resolution=1)
    testbs.construct_base_grid()
    testbs.build_histogram()
    testbs.build_surfaces(method='linear', count_msk=1)

    expected_surf = np.array([[1.2, 1.3, 1.4, 1.5],
                              [1.7, 1.8, 1.9, 2.0],
                              [2.2, 2.3, 2.4, 2.5],
                              [2.7, 2.8, 2.9, 3.0]])

    assert np.array_equal(testbs.surf, expected_surf)

    print('Passed: basesurface')


def build_georef_correct_comparison(dset='realdual', vert_ref='waterline', datum='NAD83'):
    """
   Generate mine/kongsberg xyz88 data set from the test dataset.

   Will run using the 'realdualhead' dataset included in this file or a small synthetic test dataset with meaningless
   numbers that I've just come up with.

   Parameters
   ----------
   dset: str, specify which dataset you want to use
   vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
   datum: str, datum identifier, anything recognized by pyproj CRS

   """

    if dset == 'real':
        synth_dat = RealFqpr()
        synth = load_dataset(synth_dat)
    elif dset == 'realdual':
        synth_dat = RealDualheadFqpr()
        synth = load_dataset(synth_dat)
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False, delete_futs=False)

    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['xyz'].keys())[0]

    loaded_xyz_data = [fq.intermediate_dat[s]['xyz'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    loaded_sv_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    loaded_ang_data = [np.rad2deg(fq.intermediate_dat[s]['bpv'][tstmp][0][0].result()[1]) for s in
                       fq.return_sector_ids()]

    fq.intermediate_dat = {}

    if dset == 'realdual':
        loaded_data = [[loaded_sv_data[i][0].values[0], loaded_sv_data[i][1].values[0], loaded_xyz_data[i][2].values[0],
                        loaded_ang_data[i].values[0]] for i in range(int(len(loaded_xyz_data)))]

        # apply waterline, z lever arm and z phase center offsets to get at the actual waterline rel value
        depth_wline_addtl = [-float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp])]

        # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
        xyz_88_corrangle = [90 - np.array(synth_dat.xyz88_corrangle[0]), 90 - np.array(synth_dat.xyz88_corrangle[1]),
                            np.array(synth_dat.xyz88_corrangle[2]) - 90, np.array(synth_dat.xyz88_corrangle[3]) - 90]
        xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
                       np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl[i],
                       xyz_88_corrangle[i]] for i in range(int(len(synth_dat.xyz88_depth)))]

    elif dset == 'real':
        loaded_data = []
        for tme in [0, 1]:
            for secs in [[0, 2, 4], [1, 3, 5]]:
                dpth = np.concatenate([loaded_xyz_data[secs[0]][2].values[tme][~np.isnan(loaded_xyz_data[secs[0]][2].values[tme])],
                                       loaded_xyz_data[secs[1]][2].values[tme][~np.isnan(loaded_xyz_data[secs[1]][2].values[tme])],
                                       loaded_xyz_data[secs[2]][2].values[tme][~np.isnan(loaded_xyz_data[secs[2]][2].values[tme])]])
                along = np.concatenate([loaded_sv_data[secs[0]][0].values[tme][~np.isnan(loaded_sv_data[secs[0]][0].values[tme])],
                                        loaded_sv_data[secs[1]][0].values[tme][~np.isnan(loaded_sv_data[secs[1]][0].values[tme])],
                                        loaded_sv_data[secs[2]][0].values[tme][~np.isnan(loaded_sv_data[secs[2]][0].values[tme])]])
                across = np.concatenate([loaded_sv_data[secs[0]][1].values[tme][~np.isnan(loaded_sv_data[secs[0]][1].values[tme])],
                                         loaded_sv_data[secs[1]][1].values[tme][~np.isnan(loaded_sv_data[secs[1]][1].values[tme])],
                                         loaded_sv_data[secs[2]][1].values[tme][~np.isnan(loaded_sv_data[secs[2]][1].values[tme])]])
                angle = np.concatenate([loaded_ang_data[secs[0]].values[tme][~np.isnan(loaded_ang_data[secs[0]].values[tme])],
                                        loaded_ang_data[secs[1]].values[tme][~np.isnan(loaded_ang_data[secs[1]].values[tme])],
                                        loaded_ang_data[secs[2]].values[tme][~np.isnan(loaded_ang_data[secs[2]].values[tme])]])
                loaded_data.append([along, across, dpth, angle])

        # in the future, include sec index to get the additional phase center offsets included here
        depth_wline_addtl = -float(fq.multibeam.xyzrph['waterline'][tstmp]) + float(fq.multibeam.xyzrph['tx_z'][tstmp])

        # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
        xyz_88_corrangle = []
        for ang in synth_dat.xyz88_corrangle:
            ang = 90 - np.array(ang)
            ang[np.argmin(ang):] = ang[np.argmin(ang):] * -1
            xyz_88_corrangle.append(ang)

        xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
                       np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl, xyz_88_corrangle[i]] for i in
                      range(int(len(synth_dat.xyz88_depth)))]

    else:
        raise NotImplementedError('only real and realdual are currently implemented')

    return loaded_data, xyz88_data


def build_kongs_comparison_plots(dset='realdual', vert_ref='waterline', datum='NAD83'):
    """
    Use the build_georef_correct_comparison function to get kongsberg and my created values from the test_dataset
    and build some comparison plots.

    Parameters
    ----------
    dset: string identifier, identifies which of the test_datasets to use
    vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
    datum: str, datum identifier, anything recognized by pyproj CRS

    Returns
    -------
    plots: list, each element of the list is a tuple of the figure and all the subplots associated with that ping

    """
    mine, kongsberg = build_georef_correct_comparison(dset=dset, vert_ref=vert_ref, datum=datum)

    plots = []

    if dset == 'realdual':
        for cnt, idxs in enumerate([[0, 2], [1, 3]]):
            print('Generating Ping {} plot'.format(cnt + 1))

            fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)

            fig.suptitle('Ping {}'.format(cnt + 1))
            z_plt.set_title('depth compare')
            x_plt.set_title('along compare')
            y_plt.set_title('across compare')
            ang_plt.set_title('angle compare')

            z_plt.plot(np.concatenate([mine[idxs[0]][2], mine[idxs[1]][2]]), c='b')
            z_plt.plot(np.concatenate([kongsberg[idxs[0]][2], kongsberg[idxs[1]][2]]), c='r')
            x_plt.plot(np.concatenate([mine[idxs[0]][0], mine[idxs[1]][0]]), c='b')
            x_plt.plot(np.concatenate([kongsberg[idxs[0]][0], kongsberg[idxs[1]][0]]), c='r')
            y_plt.plot(np.concatenate([mine[idxs[0]][1], mine[idxs[1]][1]]), c='b')
            y_plt.plot(np.concatenate([kongsberg[idxs[0]][1], kongsberg[idxs[1]][1]]), c='r')
            ang_plt.plot(np.concatenate([mine[idxs[0]][3], mine[idxs[1]][3]]), c='b')
            ang_plt.plot(np.concatenate([kongsberg[idxs[0]][3], kongsberg[idxs[1]][3]]), c='r')
            plots.append([fig, z_plt, x_plt, y_plt, ang_plt])
    else:
        for i in range(len(mine)):
            print('Generating Ping {} plot'.format(i + 1))

            fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)

            fig.suptitle('Ping {}'.format(i + 1))
            z_plt.set_title('depth compare')
            x_plt.set_title('along compare')
            y_plt.set_title('across compare')
            ang_plt.set_title('angle compare')

            z_plt.plot(mine[i][2], c='b')
            z_plt.plot(kongsberg[i][2], c='r')
            x_plt.plot(mine[i][0], c='b')
            x_plt.plot(kongsberg[i][0], c='r')
            y_plt.plot(mine[i][1], c='b')
            y_plt.plot(kongsberg[i][1], c='r')
            ang_plt.plot(mine[i][3], c='b')
            ang_plt.plot(kongsberg[i][3], c='r')
            plots.append([fig, z_plt, x_plt, y_plt, ang_plt])

    return plots


def load_dataset(dset=None, skip_dask=True):
    """
    Returns the 'real' dataset constructed using one of the synth data classes.  If None, uses SyntheticFqpr with some
    dummy values.  Otherwise, expects one of RealFqpr, RealDualheadFqpr, SyntheticFqpr, etc.  Builds the
    xarray_conversion BatchRead class using the dataset data.

    Parameters
    ----------
    dset: optional, if None will use SyntheticFqpr with zeroed values, otherwise one of RealFqpr, RealDualheadFqpr,
           SyntheticFqpr, etc classes.
    skip_dask

    Returns
    -------
    kongs_dat: xarray_conversion BatchRead object

    """
    if dset is None:
        dset = SyntheticFqpr(synth_time=0, synth_heave=0, synth_roll=0, synth_pitch=0, synth_yaw=0,
                             synth_tx_mountroll=0, synth_tx_mountpitch=0, synth_tx_mountyaw=0, synth_rx_mountroll=0,
                             synth_rx_mountpitch=0, synth_rx_mountyaw=0, secs=('999_0_290000', '999_0_300000'))

    kongs_dat = xarray_conversion.BatchRead('', skip_dask=skip_dask)
    kongs_dat.logger = logging.getLogger()
    kongs_dat.logger.setLevel(logging.INFO)
    kongs_dat.xyzrph = dset.xyzrph
    kongs_dat.raw_ping = dset.raw_ping
    kongs_dat.raw_att = dset.raw_att
    kongs_dat.raw_nav = dset.raw_nav
    return kongs_dat
