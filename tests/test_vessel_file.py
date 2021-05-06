import os

from HSTB.kluster.fqpr_vessel import VesselFile, get_overlapping_timestamps, compare_dict_data, carry_over_optional


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
    vf = VesselFile()
    vf.save(testfile)
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
    try:  # not a valid range, outside of the 60 second buffer
        new_data = vf.return_data('123', 1000, 1100)
        assert False
    except ValueError:
        assert True
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
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    data_two = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    assert compare_dict_data(data_one, data_two)
    data_two["roll_patch_error"]["1584438532"] = 999
    # still passes after changing an optional parameter
    assert compare_dict_data(data_one, data_two)
    data_two["rx_h"]["1584438532"] = 999
    assert not compare_dict_data(data_one, data_two)


def test_carry_over_optional():
    data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    data_two = {"roll_patch_error": {"1584426525": 999, "1584438532": 0.1, "1597569340": 0.1},
                "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 999, "1597569340": 0.0005},
                "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
    new_data_two = carry_over_optional(data_one, data_two)
    assert new_data_two == data_one