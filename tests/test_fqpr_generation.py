import os, shutil
import time
import logging
from pytest import approx

from HSTB.kluster import fqpr_generation, fqpr_project, xarray_conversion, fqpr_intelligence
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr
except:  # relative import as tests directory can vary in location depending on how kluster is installed
    from .test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr
from HSTB.kluster.xarray_helpers import interp_across_chunks
from HSTB.kluster.fqpr_convenience import *
from HSTB.kluster.modules.export import generate_export_data
from HSTB.drivers import par3


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


def test_process_testfile():
    """
    Run conversion and basic processing on the test file
    """
    global datapath

    testfile_path, expected_output = get_testfile_paths()
    out = convert_multibeam(testfile_path)
    out = process_multibeam(out, coord_system='NAD83')

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
    assert firstbeam_angle == approx(np.float32(74.640), 0.001)
    assert firstbeam_traveltime == approx(np.float32(0.3360895), 0.000001)
    assert first_counter == 61967
    assert first_dinfo == 2
    assert first_mode == 'FM'
    assert first_modetwo == '__FM'
    assert first_ntx == 3
    assert firstbeam_procstatus == 5
    assert firstbeam_qualityfactor == 42
    assert first_soundspeed == np.float32(1488.6)
    assert first_tiltangle == np.float32(-0.44)
    assert first_delay == approx(np.float32(0.002206038), 0.000001)
    assert first_frequency == 275000
    assert first_yawpitch == 'PY'
    assert firstcorr_angle == approx(np.float32(1.2028906), 0.000001)
    assert firstcorr_altitude == np.float32(0.0)
    assert firstcorr_heave == approx(np.float32(-0.06), 0.01)
    assert firstdepth_offset == approx(np.float32(92.162), 0.001)
    assert first_status == 5
    assert firstrel_azimuth == approx(np.float32(4.703383), 0.00001)
    assert firstrx == approx(np.array([0.7870753, 0.60869384, -0.100021675], dtype=np.float32), 0.00001)
    assert firstthu == approx(np.float32(8.857684), 0.0001)
    assert firsttvu == approx(np.float32(2.4940288), 0.0001)
    assert firsttx == approx(np.array([0.6074468, -0.79435784, 0.0020107413], dtype=np.float32), 0.00001)
    assert firstx == approx(539028.450, 0.001)
    assert firsty == approx(5292783.977, 0.001)
    assert firstz == approx(np.float32(92.742), 0.001)

    assert rp.min_x == 538922.066
    assert rp.min_y == 5292774.566
    assert rp.min_z == 72.961
    assert rp.max_x == 539320.370
    assert rp.max_y == 5293236.823
    assert rp.max_z == 94.294

    datapath = out.multibeam.converted_pth
    out.close()
    out = None


def test_converted_data_content():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)

    testfile_path, expected_output = get_testfile_paths()
    ad = par3.AllRead(testfile_path)
    ad.mapfile()

    # assert that they have the same number of pings
    assert out.multibeam.raw_ping[0].time.shape[0] == ad.map.getnum(78)

    # assert that there are the same number of attitude/navigation packets
    totatt = 0
    for i in range(ad.map.getnum(65)):
        rec = ad.getrecord(65, i)
        totatt += rec.data['Time'].shape[0]
    assert out.multibeam.raw_att.time.shape[0] == totatt

    ad.close()
    out.close()
    out = None


def test_return_xyz():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    x, y, z = out.return_xyz()
    assert x[0] == 539027.325
    assert x[-1] == 539222.21
    assert len(x) == 86400
    assert y[0] == 5292784.603
    assert y[-1] == 5293227.862
    assert len(y) == 86400
    assert z[0] == approx(92.742, 0.001)
    assert z[-1] == approx(92.02, 0.001)
    assert len(z) == 86400
    out.close()
    out = None


def test_return_total_pings():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    pc = out.return_total_pings(min_time=1495563100, max_time=1495563130)
    assert pc == 123
    pc = out.return_total_pings()
    assert pc == 216
    out.close()
    out = None


def test_return_total_soundings():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    ts = out.return_total_soundings(min_time=1495563100, max_time=1495563130)
    assert ts == 49200
    ts = out.return_total_soundings()
    assert ts == 86400
    out.close()
    out = None


def test_return_soundings_in_polygon():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                        [-122.47771027, 47.78949665]])
    x, y, z, tvu, rejected, pointtime, beam = out.return_soundings_in_polygon(polygon)
    assert x.shape == y.shape == z.shape == tvu.shape == rejected.shape == pointtime.shape == beam.shape
    assert x.shape == (1911,)
    out.close()
    out = None


def test_return_cast_dict():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    cdict = out.return_cast_dict()
    assert cdict == {'profile_1495563079': {'location': [47.78890945494799, -122.47711319986821],
                                            'source': 'multibeam', 'time': 1495563079,
                                            'data': [[0.0, 1489.2000732421875], [0.32, 1489.2000732421875], [0.5, 1488.7000732421875],
                                                     [0.55, 1488.300048828125], [0.61, 1487.9000244140625], [0.65, 1488.2000732421875],
                                                     [0.67, 1488.0], [0.79, 1487.9000244140625], [0.88, 1487.9000244140625],
                                                     [1.01, 1488.2000732421875], [1.04, 1488.0999755859375], [1.62, 1488.0999755859375],
                                                     [2.0300000000000002, 1488.300048828125], [2.43, 1488.9000244140625], [2.84, 1488.5],
                                                     [3.25, 1487.7000732421875], [3.67, 1487.2000732421875], [4.45, 1486.800048828125],
                                                     [4.8500000000000005, 1486.800048828125], [5.26, 1486.5999755859375], [6.09, 1485.7000732421875],
                                                     [6.9, 1485.0999755859375], [7.71, 1484.800048828125], [8.51, 1484.0],
                                                     [8.91, 1483.800048828125], [10.13, 1483.7000732421875], [11.8, 1483.0999755859375],
                                                     [12.620000000000001, 1482.9000244140625], [16.79, 1482.9000244140625], [20.18, 1481.9000244140625],
                                                     [23.93, 1481.300048828125], [34.79, 1480.800048828125], [51.15, 1480.800048828125],
                                                     [56.13, 1481.0], [60.67, 1481.5], [74.2, 1481.9000244140625], [12000.0, 1675.800048828125]]}}
    out.close()
    out = None


def test_subset_by_time():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    out.subset_by_time(mintime=1495563100, maxtime=1495563130)
    assert len(out.multibeam.raw_ping[0].time) == 123
    assert len(out.multibeam.raw_att.time) == 3001
    out.restore_subset()
    assert len(out.multibeam.raw_ping[0].time) == 216
    assert len(out.multibeam.raw_att.time) == 5302
    out.close()
    out = None


def test_subset_variables():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    dset = out.subset_variables(['z'], ping_times=(1495563100, 1495563130))

    assert len(dset.time) == 123
    assert dset.z.shape[0] == 123

    assert len(out.multibeam.raw_ping[0].time) == 216
    assert out.multibeam.raw_ping[0].z.shape[0] == 216
    assert len(out.multibeam.raw_att.time) == 5302
    out.close()
    out = None


def test_subset_variables_filter():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    dset = out.subset_variables(['z'], ping_times=(1495563100, 1495563130), filter_by_detection=True)

    assert len(dset.sounding) == 45059
    assert dset.z.shape[0] == 45059

    assert len(out.multibeam.raw_ping[0].time) == 216
    assert out.multibeam.raw_ping[0].z.shape[0] == 216
    assert len(out.multibeam.raw_att.time) == 5302
    out.close()
    out = None


def test_subset_variables_by_line():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    dset = out.subset_variables_by_line(['z'])

    assert list(dset.keys()) == ['0009_20170523_181119_FA2806.all']
    assert len(dset['0009_20170523_181119_FA2806.all'].time) == 216
    assert dset['0009_20170523_181119_FA2806.all'].z.shape[0] == 216
    out.close()
    out = None


def test_intersects():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    assert out.intersects(5293000, 5330000, 538950, 539300, geographic=False)
    assert not out.intersects(5320000, 5330000, 538950, 539300, geographic=False)
    assert out.intersects(47.78895, 47.790, -122.478, -122.479, geographic=True)
    assert not out.intersects(47.8899, 47.890, -122.478, -122.479, geographic=True)
    out.close()
    out = None


def test_return_unique_mode():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    mode = out.return_unique_mode()
    assert mode == ['FM']
    out.close()
    out = None


def test_return_rounded_frequency():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    fq = out.return_rounded_frequency()
    assert fq == [300000]
    out.close()
    out = None


def test_return_lines_for_times():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    lns = out.return_lines_for_times(np.array([1495400000, 1495563100, 1495563132]))
    assert np.array_equal(lns, ['', '0009_20170523_181119_FA2806.all', '0009_20170523_181119_FA2806.all'])
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
    fintel.set_settings({'coord_system': 'NAD83'})
    fintel.add_file(testfile_path)
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
    assert action.kwargs == {}
    assert action.args[2:] == [None, False, True]

    fintel.execute_action()
    action = fintel.action_container.actions[0]
    assert action.text[0:21] == 'Run all processing on'
    assert action.action_type == 'processing'
    assert action.priority == 5
    assert action.is_running is False
    assert len(action.input_files) == 0
    assert action.kwargs == {'run_orientation': True, 'orientation_initial_interpolation': False, 'run_beam_vec': True,
                             'run_svcorr': True, 'add_cast_files': [], 'run_georef': True, 'run_tpu': True, 'use_epsg': False,
                             'use_coord': True, 'epsg': None, 'coord_system': 'NAD83', 'vert_ref': 'waterline'}
    assert isinstance(action.args[0], fqpr_generation.Fqpr)

    assert isinstance(proj.get_dask_client(), Client)
    assert isinstance(proj.build_raw_attitude_for_line('0009_20170523_181119_FA2806.all'), xr.Dataset)
    assert proj.fqpr_instances['em2040_40111_05_23_2017'] == proj.return_line_owner('0009_20170523_181119_FA2806.all')

    fintel.clear()
    datapath = action.args[0].multibeam.converted_pth
    proj.close()
    action.args[0] = None

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
    fq.get_orientation_vectors(dump_data=False, initial_interp=False)

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
    for i in range(len(expected_tx)):
        assert expected_tx[i] == approx(txvecdata[i], 0.000001)

    # check for the expected rx orientation vectors
    for i in range(len(expected_rx)):
        assert expected_rx[i] == approx(rxvecdata[i], 0.000001)

    fq.close()
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
    fq.get_orientation_vectors(dump_data=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False)

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
    for i in range(len(ba_data)):
        assert ba_data[i] == approx(expected_ba[i], 0.0000001)

    # beam depression angle check
    for i in range(len(bda_data)):
        assert bda_data[i] == approx(expected_bda[i], 0.0000001)

    fq.close()
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
    fq.get_orientation_vectors(dump_data=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False)
    fq.sv_correct(dump_data=False)

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
    for i in range(len(x_data)):
        assert x_data[i] == approx(expected_x[i], 0.001)

    # acrosstrack offset check
    for i in range(len(y_data)):
        assert y_data[i] == approx(expected_y[i], 0.001)

    # depth offset check
    for i in range(len(z_data)):
        assert z_data[i] == approx(expected_z[i], 0.001)

    fq.close()
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
    fq.get_orientation_vectors(dump_data=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False)
    fq.sv_correct(dump_data=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False)

    # arrays of computed vectors
    sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
    tstmp = list(fq.intermediate_dat[sysid[0]]['georef'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['georef'][tstmp][0][0] for s in sysid]

    x_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
    y_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]
    z_data = [ld[2].isel(time=0).values[0:3] for ld in loaded_data]

    print('GEOREF {}'.format(dset))
    print([x for y in x_data for x in y.flatten()])
    print([x for y in y_data for x in y.flatten()])
    print([x for y in z_data for x in y.flatten()])

    # easting
    for i in range(len(x_data)):
        assert x_data[i] == approx(expected_x[i], 0.001)

    # northing
    for i in range(len(y_data)):
        assert y_data[i] == approx(expected_y[i], 0.001)

    # depth
    for i in range(len(z_data)):
        assert z_data[i] == approx(expected_z[i], 0.001)

    fq.close()
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

    expected_att = xr.Dataset(
        {'heading': (['time'], np.array([307.8539977551496, 307.90348427192055, 308.6139892100822])),
         'heave': (['time'], np.array([0.009999999776482582, 0.009608692733222632, -0.009999999776482582])),
         'roll': (['time'], np.array([0.4400004684929343, 0.07410809820512047, -4.433999538421631])),
         'pitch': (['time'], np.array([-0.5, -0.5178477924436871, -0.3760000467300415]))},
        coords={'time': np.array([1495563084.455, 1495563084.49, 1495563084.975])})

    # make sure the dask/non-dask methods line up
    assert dask_interp_att.time.values == approx(interp_att.time.values, 0.001)
    assert dask_interp_att.heading.values == approx(interp_att.heading.values, 0.001)
    assert dask_interp_att.heave.values == approx(interp_att.heave.values, 0.001)
    assert dask_interp_att.pitch.values == approx(interp_att.pitch.values, 0.001)
    assert dask_interp_att['roll'].values == approx(interp_att['roll'].values, 0.001)

    # make sure the values line up with what we would expect
    assert dask_interp_att.time.values == approx(expected_att.time.values, 0.001)
    assert dask_interp_att.heading.values == approx(expected_att.heading.values, 0.001)
    assert dask_interp_att.heave.values == approx(expected_att.heave.values, 0.001)
    assert dask_interp_att.pitch.values == approx(expected_att.pitch.values, 0.001)
    assert dask_interp_att['roll'].values == approx(expected_att['roll'].values, 0.001)

    print('Passed: interp_across_chunks')


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
    fq.get_orientation_vectors(dump_data=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False)
    fq.sv_correct(dump_data=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False)

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
                dpth = np.concatenate(
                    [loaded_xyz_data[secs[0]][2].values[tme][~np.isnan(loaded_xyz_data[secs[0]][2].values[tme])],
                     loaded_xyz_data[secs[1]][2].values[tme][~np.isnan(loaded_xyz_data[secs[1]][2].values[tme])],
                     loaded_xyz_data[secs[2]][2].values[tme][~np.isnan(loaded_xyz_data[secs[2]][2].values[tme])]])
                along = np.concatenate(
                    [loaded_sv_data[secs[0]][0].values[tme][~np.isnan(loaded_sv_data[secs[0]][0].values[tme])],
                     loaded_sv_data[secs[1]][0].values[tme][~np.isnan(loaded_sv_data[secs[1]][0].values[tme])],
                     loaded_sv_data[secs[2]][0].values[tme][~np.isnan(loaded_sv_data[secs[2]][0].values[tme])]])
                across = np.concatenate(
                    [loaded_sv_data[secs[0]][1].values[tme][~np.isnan(loaded_sv_data[secs[0]][1].values[tme])],
                     loaded_sv_data[secs[1]][1].values[tme][~np.isnan(loaded_sv_data[secs[1]][1].values[tme])],
                     loaded_sv_data[secs[2]][1].values[tme][~np.isnan(loaded_sv_data[secs[2]][1].values[tme])]])
                angle = np.concatenate(
                    [loaded_ang_data[secs[0]].values[tme][~np.isnan(loaded_ang_data[secs[0]].values[tme])],
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

    fq.close()
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
    return kongs_dat
