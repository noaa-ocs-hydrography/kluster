import os

from HSTB.kluster.fqpr_vessel import VesselFile, get_overlapping_timestamps, compare_dict_data, carry_over_optional, \
    create_new_vessel_file, only_retain_earliest_entry


def get_test_vesselfile():
    """
    return the necessary paths for the testfile tests

    Returns
    -------
    str
        absolute file path to the test file
    """

    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'vessel_file.kfc')
    return testfile


def test_save_empty_file():
    testfile = get_test_vesselfile()
    vf = create_new_vessel_file(testfile)
    assert vf.data == {}
    assert vf.source_file == testfile
    assert os.path.exists(testfile)


def test_open_empty_file():
    testfile = get_test_vesselfile()
    vf = VesselFile(testfile)
    assert vf.data == {}
    assert vf.source_file == testfile
    assert os.path.exists(testfile)


def test_update_empty():
    testfile = get_test_vesselfile()
    vf = VesselFile(testfile)
    vf.update('123', {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                      'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}})
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}}}
    vf.save(testfile)
    vf = VesselFile(testfile)
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}}}


def test_update_existing():
    testfile = get_test_vesselfile()
    vf = VesselFile(testfile)
    vf.update('345', {'beam_opening_angle': {"1234": 3.0, '1244': 3.5, '1254': 4.0},
                      'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}})
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                       '345': {'beam_opening_angle': {"1234": 3.0, '1244': 3.5, '1254': 4.0},
                               'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}
    vf.save(testfile)
    vf = VesselFile(testfile)
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                       '345': {'beam_opening_angle': {"1234": 3.0, '1244': 3.5, '1254': 4.0},
                               'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}


def test_overwrite_existing():
    testfile = get_test_vesselfile()
    vf = VesselFile(testfile)
    vf.update('345', {'beam_opening_angle': {"1234": 999}})
    #  no change made, will always try to carry over the last tpu entry
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                       '345': {'beam_opening_angle': {"1234": 4.0, '1244': 3.5, '1254': 4.0},
                               'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}
    #  force the overwrite
    vf.update('345', {'beam_opening_angle': {"1234": 999}}, carry_over_tpu=False)
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                       '345': {'beam_opening_angle': {"1234": 999, '1244': 3.5, '1254': 4.0},
                               'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}
    vf.save(testfile)
    vf = VesselFile(testfile)
    assert vf.data == {'123': {'beam_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                               'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                       '345': {'beam_opening_angle': {"1234": 999, '1244': 3.5, '1254': 4.0},
                               'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}


def test_return_data():
    testfile = get_test_vesselfile()
    vf = VesselFile(testfile)
    new_data = vf.return_data('345', 1239, 1250)
    assert new_data == {'beam_opening_angle': {"1234": 999, '1244': 3.5},
                        'rx_x': {"1234": 1.345, '1244': 2.456}}


def test_return_singletimestamp():
    vf = VesselFile()
    vf.update('123', {'beam_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}})
    new_data = vf.return_data('123', 1239, 1250)
    assert new_data == {'beam_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}
    new_data = vf.return_data('123', 1230, 1235)
    assert new_data == {'beam_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}
    # not a valid range, outside of the 60 second buffer
    new_data = vf.return_data('123', 1000, 1100)
    assert not new_data
    # data that is after the timestamp uses the closest previous timestamp
    new_data = vf.return_data('123', 1300, 1400)
    assert new_data == {'beam_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}


def test_vessel_cleanup():
    testfile = get_test_vesselfile()
    os.remove(testfile)
    assert not os.path.exists(testfile)


def test_overlapping_timestamps():
    timestamps = [1584426525, 1584429900, 1584430000, 1584438532, 1597569340]
    starttime = 1584429999
    endtime = 1590000000
    tstmps = get_overlapping_timestamps(timestamps, starttime, endtime)
    assert tstmps == ['1584429900', '1584430000', '1584438532']


def test_compare_dict():
    data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
    data_two = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}

    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert identical_offsets
    assert identical_angles
    assert identical_tpu
    assert data_matches
    assert not new_waterline

    data_two["roll_patch_error"]["1584438532"] = 999
    # now fails tpu check and data match check
    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert identical_offsets
    assert identical_angles
    assert not identical_tpu
    assert not data_matches
    assert not new_waterline

    data_two["rx_h"]["1584438532"] = 999
    # now fails all three
    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert identical_offsets
    assert not identical_angles
    assert not identical_tpu
    assert not data_matches
    assert not new_waterline

    data_two["rx_x"]["1584438532"] = 999
    # now fails all four
    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert not identical_offsets
    assert not identical_angles
    assert not identical_tpu
    assert not data_matches
    assert not new_waterline

    data_two["waterline"]["1584426525"] = 999
    # detect a new waterline value, only looks at the first timestamp
    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert not identical_offsets
    assert not identical_angles
    assert not identical_tpu
    assert not data_matches
    assert new_waterline

    data_one = {"roll_patch_error": {"999999999": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"999999999": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"999999999": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                "rx_x": {"999999999": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
    data_two = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
    # data match check just looks at the values
    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one, data_two)
    assert not identical_offsets
    assert not identical_angles
    assert not identical_tpu
    assert data_matches
    assert not new_waterline


def test_carry_over_optional():
    data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    data_two = {"roll_patch_error": {"1584426525": 999, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 999, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    new_data_two = carry_over_optional(data_one, data_two)
    assert new_data_two == data_one
    assert data_two["roll_patch_error"]["1584426525"] == 0.1


def test_only_retain():
    data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    only_retain_earliest_entry(data_one)
    assert data_one == {'roll_patch_error': {'1584426525': 0.1},
                        'roll_sensor_error': {'1584426525': 0.0005},
                        'rx_h': {'1584426525': 359.576}}
    data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.001, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    only_retain_earliest_entry(data_one)
    assert data_one == {'roll_patch_error': {'1584426525': 0.1, '1584438532': 0.1},
                        'roll_sensor_error': {'1584426525': 0.0005, '1584438532': 0.001},
                        'rx_h': {'1584426525': 359.576, '1584438532': 359.576}}