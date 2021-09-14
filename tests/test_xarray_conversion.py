import shutil

from HSTB.kluster.xarray_conversion import *
from HSTB.kluster.xarray_conversion import _xarr_is_bit_set, _build_serial_mask, _return_xarray_mintime, \
    _return_xarray_timelength, _divide_xarray_indicate_empty_future, _return_xarray_constant_blocks, \
    _merge_constant_blocks, _assess_need_for_split_correction, _correct_for_splits, _closest_prior_key_value, \
    _closest_key_value
from HSTB.kluster.fqpr_convenience import convert_multibeam, reload_data
from HSTB.kluster import kluster_variables


datapath = ''


def get_testfile_paths():
    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    expected_output = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'converted')
    return testfile, expected_output


def test_return_xyzrph_from_mbes():
    testfile, expected_output = get_testfile_paths()
    assert os.path.exists(testfile)
    xyzrph, sonarmodel, serialnum = return_xyzrph_from_mbes(testfile)
    assert xyzrph['beam_opening_angle']['1495563079'] == 1.0
    assert xyzrph['heading_patch_error']['1495563079'] == 0.5
    assert xyzrph['heading_sensor_error']['1495563079'] == 0.02
    assert xyzrph['heave_error']['1495563079'] == 0.05
    assert xyzrph['horizontal_positioning_error']['1495563079'] == 1.5
    assert xyzrph['imu_h']['1495563079'] == 0.400
    assert xyzrph['latency']['1495563079'] == 0.000
    assert xyzrph['imu_p']['1495563079'] == -0.180
    assert xyzrph['imu_r']['1495563079'] == -0.160
    assert xyzrph['imu_x']['1495563079'] == 0.000
    assert xyzrph['imu_y']['1495563079'] == 0.000
    assert xyzrph['imu_z']['1495563079'] == 0.000
    assert xyzrph['latency_patch_error']['1495563079'] == 0.0
    assert xyzrph['pitch_patch_error']['1495563079'] == 0.1
    assert xyzrph['pitch_sensor_error']['1495563079'] == 0.001
    assert xyzrph['roll_patch_error']['1495563079'] == 0.1
    assert xyzrph['roll_sensor_error']['1495563079'] == 0.001
    assert xyzrph['rx_h']['1495563079'] == 0.000
    assert xyzrph['rx_p']['1495563079'] == 0.000
    assert xyzrph['rx_r']['1495563079'] == 0.000
    assert xyzrph['rx_x']['1495563079'] == -0.100
    assert xyzrph['rx_x_0']['1495563079'] == 0.011
    assert xyzrph['rx_x_1']['1495563079'] == 0.011
    assert xyzrph['rx_x_2']['1495563079'] == 0.011
    assert xyzrph['rx_y']['1495563079'] == -0.304
    assert xyzrph['rx_y_0']['1495563079'] == 0.0
    assert xyzrph['rx_y_1']['1495563079'] == 0.0
    assert xyzrph['rx_y_2']['1495563079'] == 0.0
    assert xyzrph['rx_z']['1495563079'] == -0.016
    assert xyzrph['rx_z_0']['1495563079'] == -0.006
    assert xyzrph['rx_z_1']['1495563079'] == -0.006
    assert xyzrph['rx_z_2']['1495563079'] == -0.006
    assert xyzrph['separation_model_error']['1495563079'] == 0.0
    assert xyzrph['surface_sv_error']['1495563079'] == 0.5
    assert xyzrph['timing_latency_error']['1495563079'] == 0.001
    assert xyzrph['tx_h']['1495563079'] == 0.000
    assert xyzrph['tx_p']['1495563079'] == 0.000
    assert xyzrph['tx_r']['1495563079'] == 0.000
    assert xyzrph['tx_to_antenna_x']['1495563079'] == 0.000
    assert xyzrph['tx_to_antenna_y']['1495563079'] == 0.000
    assert xyzrph['tx_to_antenna_z']['1495563079'] == 0.000
    assert xyzrph['tx_x']['1495563079'] == 0.000
    assert xyzrph['tx_x_0']['1495563079'] == 0.0
    assert xyzrph['tx_x_1']['1495563079'] == 0.0
    assert xyzrph['tx_x_2']['1495563079'] == 0.0
    assert xyzrph['tx_y']['1495563079'] == 0.000
    assert xyzrph['tx_y_0']['1495563079'] == -0.0554
    assert xyzrph['tx_y_1']['1495563079'] == 0.0131
    assert xyzrph['tx_y_2']['1495563079'] == 0.0554
    assert xyzrph['tx_z']['1495563079'] == 0.000
    assert xyzrph['tx_z_0']['1495563079'] == -0.012
    assert xyzrph['tx_z_1']['1495563079'] == -0.006
    assert xyzrph['tx_z_2']['1495563079'] == -0.012
    assert xyzrph['vertical_positioning_error']['1495563079'] == 1.0
    assert xyzrph['vessel_speed_error']['1495563079'] == 0.1
    assert xyzrph['waterline']['1495563079'] == -0.640
    assert xyzrph['waterline_error']['1495563079'] == 0.02
    assert xyzrph['x_offset_error']['1495563079'] == 0.2
    assert xyzrph['y_offset_error']['1495563079'] == 0.2
    assert xyzrph['z_offset_error']['1495563079'] == 0.2


def test_xarr_is_bit_set():
    tst = xr.DataArray(np.arange(10))
    # 4 5 6 7 all have the third bit set
    ans = np.array([False, False, False, False, True, True, True, True, False, False])
    assert np.array_equal(_xarr_is_bit_set(tst, 3), ans)


def test_build_serial_mask():
    rec = {'ping': {'serial_num': np.array([40111, 40111, 40112, 40112, 40111, 40111])}}
    ids, msk = _build_serial_mask(rec)
    assert ids == ['40111', '40112']
    assert np.array_equal(msk[0], np.array([0, 1, 4, 5]))
    assert np.array_equal(msk[1], np.array([2, 3]))


def test_return_xarray_mintime():
    tstone = xr.DataArray(np.arange(10), coords={'time': np.arange(10)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(10)})
    assert _return_xarray_mintime(tstone) == 0
    assert _return_xarray_mintime(tsttwo) == 0


def test_return_xarray_timelength():
    tstone = xr.DataArray(np.arange(10), coords={'time': np.arange(10)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(10)})
    assert _return_xarray_timelength(tsttwo) == 10


def test_divide_xarray_indicate_empty_future():
    assert _divide_xarray_indicate_empty_future(None) is False
    assert _divide_xarray_indicate_empty_future(xr.Dataset({'tst': []}, coords={'time': []})) is False
    assert _divide_xarray_indicate_empty_future(xr.Dataset({'tst': [1, 2, 3]}, coords={'time': [1, 2, 3]})) is True


def test_return_xarray_constant_blocks():
    tstone = xr.DataArray(np.arange(100), coords={'time': np.arange(100)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(100)})
    x1 = tsttwo.isel(time=slice(0, 33))
    x2 = tsttwo.isel(time=slice(33, 66))
    x3 = tsttwo.isel(time=slice(66, 100))
    xarrs = [x1, x2, x3]
    xlens = [33, 33, 34]
    chunks, totallen = _return_xarray_constant_blocks(xlens, xarrs, 10)

    assert totallen == 100
    assert chunks == [[[0, 10, x1]], [[10, 20, x1]], [[20, 30, x1]], [[30, 33, x1], [0, 7, x2]], [[7, 17, x2]],
                      [[17, 27, x2]], [[27, 33, x2], [0, 4, x3]], [[4, 14, x3]], [[14, 24, x3]], [[24, 34, x3]]]

    chunkdata = [x[2] for y in chunks for x in y]
    expected_data = [x1, x1, x1, x1, x2, x2, x2, x2, x3, x3, x3, x3]
    assert all([(c == chk).all() for c, chk in zip(chunkdata, expected_data)])


def test_merge_constant_blocks():
    tstone = xr.DataArray(np.arange(100), coords={'time': np.arange(100)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(100)})
    newblocks = [[0, 3, tsttwo], [10, 13, tsttwo], [20, 23, tsttwo]]
    merged = _merge_constant_blocks(newblocks)
    assert np.array_equal(np.array([0, 1, 2, 10, 11, 12, 20, 21, 22]), merged.tstone.values)


def test_assess_need_for_split_correction():
    tstone = xr.DataArray(np.arange(100), coords={'time': np.arange(100)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(100)})
    tstthree = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(99, 199)})
    assert _assess_need_for_split_correction(tsttwo, tstthree) is True

    tstfour = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(150, 250)})
    assert _assess_need_for_split_correction(tsttwo, tstfour) is False


def test_correct_for_splits():
    tstone = xr.DataArray(np.arange(100), coords={'time': np.arange(100)}, dims=['time'])
    tsttwo = xr.Dataset(data_vars={'tstone': tstone}, coords={'time': np.arange(100)})

    assert _correct_for_splits(tsttwo, True).tstone.values[0] == 1
    assert _correct_for_splits(tsttwo, False).tstone.values[0] == 0


def test_closest_prior_key_value():
    tstmps = [100.0, 1000.0, 10000.0, 100000.0, 1000000.0]
    key = 80584.3
    assert _closest_prior_key_value(tstmps, key) == 10000.0


def test_closest_key_value():
    tstmps = [100.0, 1000.0, 10000.0, 100000.0, 1000000.0]
    key = 80584.3
    assert _closest_key_value(tstmps, key) == 100000.0


def test_batch_read_configure_options():
    opts = batch_read_configure_options()
    expected_opts = {
        'ping': {'chunksize': kluster_variables.ping_chunk_size, 'chunks': kluster_variables.ping_chunks,
                 'combine_attributes': True, 'output_arrs': [], 'time_arrs': [], 'final_pths': None, 'final_attrs': None},
        'attitude': {'chunksize': kluster_variables.attitude_chunk_size, 'chunks': kluster_variables.att_chunks,
                     'combine_attributes': False, 'output_arrs': [], 'time_arrs': [], 'final_pths': None, 'final_attrs': None}}
    assert opts == expected_opts


def test_build_converted_data():
    global datapath

    testfile, expected_output = get_testfile_paths()
    assert os.path.exists(testfile)
    out = convert_multibeam(testfile)
    assert os.path.exists(expected_output)
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


def test_return_all_profiles():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    pkeys, pcasts, pcasttimes, pcastlocs = out.multibeam.return_all_profiles()
    assert pkeys == ['profile_1495563079']
    assert pcasts == [[[0.0, 0.32, 0.5, 0.55, 0.61, 0.65, 0.67, 0.79, 0.88, 1.01, 1.04, 1.62, 2.0300000000000002, 2.43, 2.84, 3.25, 3.67,
                        4.45, 4.8500000000000005, 5.26, 6.09, 6.9, 7.71, 8.51, 8.91, 10.13, 11.8, 12.620000000000001, 16.79, 20.18, 23.93,
                        34.79, 51.15, 56.13, 60.67, 74.2, 12000.0],
                       [1489.2000732421875, 1489.2000732421875, 1488.7000732421875, 1488.300048828125, 1487.9000244140625, 1488.2000732421875,
                        1488.0, 1487.9000244140625, 1487.9000244140625, 1488.2000732421875, 1488.0999755859375, 1488.0999755859375, 1488.300048828125,
                        1488.9000244140625, 1488.5, 1487.7000732421875, 1487.2000732421875, 1486.800048828125, 1486.800048828125, 1486.5999755859375,
                        1485.7000732421875, 1485.0999755859375, 1484.800048828125, 1484.0, 1483.800048828125, 1483.7000732421875, 1483.0999755859375,
                        1482.9000244140625, 1482.9000244140625, 1481.9000244140625, 1481.300048828125, 1480.800048828125, 1480.800048828125, 1481.0,
                        1481.5, 1481.9000244140625, 1675.800048828125]]]
    assert pcasttimes == [1495563079]
    assert pcastlocs == [[47.78890945494799, -122.47711319986821]]

    out.close()
    out = None


def test_is_dual_head():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    assert not out.multibeam.is_dual_head()

    out.close()
    out = None


def test_return_tpu_parameters():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    params = out.multibeam.return_tpu_parameters('1495563079')
    assert params == {'tx_to_antenna_x': 0.0, 'tx_to_antenna_y': 0.0, 'tx_to_antenna_z': 0.0, 'heave_error': 0.05,
                      'roll_sensor_error': 0.001, 'pitch_sensor_error': 0.001, 'heading_sensor_error': 0.02, 'x_offset_error': 0.2,
                      'y_offset_error': 0.2, 'z_offset_error': 0.2, 'surface_sv_error': 0.5, 'roll_patch_error': 0.1, 'pitch_patch_error': 0.1,
                      'heading_patch_error': 0.5, 'latency_patch_error': 0.0, 'timing_latency_error': 0.001, 'separation_model_error': 0.0,
                      'waterline_error': 0.02, 'vessel_speed_error': 0.1, 'horizontal_positioning_error': 1.5, 'vertical_positioning_error': 1.0,
                      'beam_opening_angle': 1.3}

    out.close()
    out = None


def test_return_system_time_indexed_array():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    sysidx = out.multibeam.return_system_time_indexed_array()
    assert len(sysidx) == 1  # there is only one head for this sonar.
    tstmp_list = sysidx[0]
    assert len(tstmp_list) == 1  # there is only one installation parameter entry for this dataset
    parameters_list = tstmp_list[0]
    sonar_idxs = parameters_list[0]
    assert sonar_idxs.shape == out.multibeam.raw_ping[0].time.shape  # first element are the indices for the applicable data
    assert sonar_idxs.all()
    sonar_tstmp = parameters_list[1]
    assert sonar_tstmp == '1495563079'  # second element is the utc timestamp for the applicable installation parameters entry
    sonar_txrx = parameters_list[2]
    assert sonar_txrx == ['tx', 'rx']  # third element are the prefixes used to look up the installation parameters

    subset_start = float(out.multibeam.raw_ping[0].time[50])
    subset_end = float(out.multibeam.raw_ping[0].time[100])
    sysidx = out.multibeam.return_system_time_indexed_array(subset_time=[subset_start, subset_end])
    tstmp_list = sysidx[0]
    parameters_list = tstmp_list[0]
    sonar_idxs = parameters_list[0]
    assert np.count_nonzero(sonar_idxs) == 51

    out.close()
    out = None


def test_return_utm_zone_number():
    if not os.path.exists(datapath):
        print('Please run test_process_testfile first')
    out = reload_data(datapath)
    assert out.multibeam.return_utm_zone_number() == '10N'

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
