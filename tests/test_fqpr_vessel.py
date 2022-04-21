import os
import shutil
import tempfile
import unittest
from HSTB.kluster.fqpr_vessel import VesselFile, get_overlapping_timestamps, compare_dict_data, carry_over_optional, \
    create_new_vessel_file, only_retain_earliest_entry, convert_from_fqpr_xyzrph, convert_from_vessel_xyzrph, \
    split_by_timestamp, trim_xyzrprh_to_times

test_xyzrph = {'antenna_x': {'1626354881': '0.000'}, 'antenna_y': {'1626354881': '0.000'},
               'antenna_z': {'1626354881': '0.000'}, 'imu_h': {'1626354881': '0.000'},
               'latency': {'1626354881': '0.000'}, 'imu_p': {'1626354881': '0.000'},
               'imu_r': {'1626354881': '0.000'}, 'imu_x': {'1626354881': '0.000'},
               'imu_y': {'1626354881': '0.000'}, 'imu_z': {'1626354881': '0.000'},
               'rx_r': {'1626354881': '0.030'}, 'rx_p': {'1626354881': '0.124'},
               'rx_h': {'1626354881': '0.087'}, 'rx_x': {'1626354881': '1.234'},
               'rx_y': {'1626354881': '0.987'}, 'rx_z': {'1626354881': '0.543'},
               'rx_x_0': {'1626354881': '0.204'}, 'rx_x_1': {'1626354881': '0.204'},
               'rx_x_2': {'1626354881': '0.204'}, 'rx_y_0': {'1626354881': '0.0'},
               'rx_y_1': {'1626354881': '0.0'}, 'rx_y_2': {'1626354881': '0.0'},
               'rx_z_0': {'1626354881': '-0.0315'}, 'rx_z_1': {'1626354881': '-0.0315'},
               'rx_z_2': {'1626354881': '-0.0315'}, 'tx_r': {'1626354881': '0.090'},
               'tx_p': {'1626354881': '-0.123'}, 'tx_h': {'1626354881': '-0.050'},
               'tx_x': {'1626354881': '1.540'}, 'tx_y': {'1626354881': '-0.987'},
               'tx_z': {'1626354881': '1.535'}, 'tx_x_0': {'1626354881': '0.002'},
               'tx_x_1': {'1626354881': '0.002'}, 'tx_x_2': {'1626354881': '0.002'},
               'tx_y_0': {'1626354881': '-0.1042'}, 'tx_y_1': {'1626354881': '0.0'},
               'tx_y_2': {'1626354881': '0.1042'}, 'tx_z_0': {'1626354881': '-0.0149'},
               'tx_z_1': {'1626354881': '-0.006'}, 'tx_z_2': {'1626354881': '-0.0149'},
               'waterline': {'1626354881': '0.200'}}


class TestFqprVessel(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.clsFolder = os.path.join(tempfile.tempdir, 'TestFqprVessel')
        try:
            os.mkdir(cls.clsFolder)
        except FileExistsError:
            shutil.rmtree(cls.clsFolder)
            os.mkdir(cls.clsFolder)

    def setUp(self) -> None:
        self.testfile = os.path.join(tempfile.mkdtemp(dir=self.clsFolder), 'vessel_file.kfc')
        self.vf = create_new_vessel_file(self.testfile)
        self.key = ['123', '345']
        self.data = [
            {'rx_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
             'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
            {'rx_opening_angle': {"1234": 3.0, '1244': 3.5, '1254': 4.0},
             'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}]
        self.data_dict = {self.key[0]: self.data[0], self.key[1]: self.data[1]}

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.clsFolder)

    def test_save_empty_file(self):
        assert self.vf.data == {}
        assert self.vf.source_file == self.testfile
        assert os.path.exists(self.testfile)

    def test_open_empty_file(self):
        vf = VesselFile(self.testfile)
        assert vf.data == {}
        assert vf.source_file == self.testfile
        assert os.path.exists(self.testfile)

    def test_update_empty(self):
        self.vf.update(self.key[0], self.data[0])
        assert self.vf.data == {self.key[0]: self.data[0]}
        self.vf.save(self.testfile)
        self.vf = VesselFile(self.testfile)
        assert self.vf.data == {self.key[0]: self.data[0]}

    def test_update_existing(self):
        self.vf.update(self.key[0], self.data[0])
        self.vf.update(self.key[1], self.data[1])
        assert self.vf.data == self.data_dict
        self.vf.save(self.testfile)
        self.vf = VesselFile(self.testfile)
        assert self.vf.data == self.data_dict

    def test_overwrite_existing(self):
        self.vf.update(self.key[0], self.data[0])
        self.vf.update(self.key[1], self.data[1])
        self.vf.save(self.testfile)
        self.vf.update(self.key[1], {'rx_opening_angle': {"1234": 999}})
        # self.data_dict.update('rx_opening_angle').update('1234') = 999
        #  no change made, will always try to carry over the last tpu entry
        assert self.vf.data == {'123': {'rx_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                                        'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                                '345': {'rx_opening_angle': {"1234": 4.0, '1244': 3.5, '1254': 4.0},
                                        'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}
        #  force the overwrite
        self.vf.update('345', {'rx_opening_angle': {"1234": 999}}, carry_over_tpu=False)
        assert self.vf.data == {'123': {'rx_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                                        'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                                '345': {'rx_opening_angle': {"1234": 999, '1244': 3.5, '1254': 4.0},
                                        'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}
        self.vf.save(self.testfile)
        self.vf = VesselFile(self.testfile)
        assert self.vf.data == {'123': {'rx_opening_angle': {"1234": 1.0, '1244': 1.5, '1254': 2.0},
                                        'rx_x': {"1234": 0.345, '1244': 0.456, '1254': 0.789}},
                                '345': {'rx_opening_angle': {"1234": 999, '1244': 3.5, '1254': 4.0},
                                        'rx_x': {"1234": 1.345, '1244': 2.456, '1254': 3.789}}}

    def test_return_data(self):
        self.vf.update(self.key[1], self.data[1])
        self.vf.save(self.testfile)
        new_data = self.vf.return_data('345', 1239, 1250)
        assert new_data == {'rx_opening_angle': {"1234": 3.0, '1244': 3.5},
                            'rx_x': {"1234": 1.345, '1244': 2.456}}

    def test_return_singletimestamp(self):
        self.vf.update('123', {'rx_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}})
        new_data = self.vf.return_data('123', 1239, 1250)
        assert new_data == {'rx_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}
        new_data = self.vf.return_data('123', 1230, 1235)
        assert new_data == {'rx_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}
        # not a valid range, outside of the 60 second buffer
        new_data = self.vf.return_data('123', 1000, 1100)
        assert not new_data
        # data that is after the timestamp uses the closest previous timestamp
        new_data = self.vf.return_data('123', 1300, 1400)
        assert new_data == {'rx_opening_angle': {"1234": 1.0}, 'rx_x': {"1234": 0.345}}

    def test_split_by_timestamp(self):
        data_one = {"latency": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                    "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                    "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
        new_data = split_by_timestamp(data_one)
        assert new_data[0] == {'latency': {'1584426525': 0.1},
                               'roll_patch_error': {'1584426525': 0.1},
                               'roll_sensor_error': {'1584426525': 0.0005},
                               'rx_h': {'1584426525': 359.576},
                               'rx_x': {'1584426525': 1.1},
                               'waterline': {'1584426525': 1.1}}
        assert new_data[1] == {'latency': {'1584438532': 0.1},
                               'roll_patch_error': {'1584438532': 0.1},
                               'roll_sensor_error': {'1584438532': 0.0005},
                               'rx_h': {'1584438532': 359.576},
                               'rx_x': {'1584438532': 1.2},
                               'waterline': {'1584438532': 1.2}}
        assert new_data[2] == {'latency': {'1597569340': 0.1},
                               'roll_patch_error': {'1597569340': 0.1},
                               'roll_sensor_error': {'1597569340': 0.0005},
                               'rx_h': {'1597569340': 359.576},
                               'rx_x': {'1597569340': 1.3},
                               'waterline': {'1597569340': 1.3}}

    def test_trim_xyzrph_to_times(self):
        data_one = {"latency": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                    "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                    "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
        assert trim_xyzrprh_to_times(data_one, 1584426525, 1584426530) == {'latency': {'1584426525': 0.1},
                                                                           'roll_patch_error': {'1584426525': 0.1},
                                                                           'roll_sensor_error': {'1584426525': 0.0005},
                                                                           'rx_h': {'1584426525': 359.576},
                                                                           'rx_x': {'1584426525': 1.1},
                                                                           'waterline': {'1584426525': 1.1}}
        assert trim_xyzrprh_to_times(data_one, 1584438532, 1584438542) == {'latency': {'1584438532': 0.1},
                                                                           'roll_patch_error': {'1584438532': 0.1},
                                                                           'roll_sensor_error': {'1584438532': 0.0005},
                                                                           'rx_h': {'1584438532': 359.576},
                                                                           'rx_x': {'1584438532': 1.2},
                                                                           'waterline': {'1584438532': 1.2}}
        assert trim_xyzrprh_to_times(data_one, 1584426525, 1584438542) == {'latency': {'1584426525': 0.1, '1584438532': 0.1},
                                                                           'roll_patch_error': {'1584426525': 0.1, '1584438532': 0.1},
                                                                           'roll_sensor_error': {'1584426525': 0.0005, '1584438532': 0.0005},
                                                                           'rx_h': {'1584426525': 359.576, '1584438532': 359.576},
                                                                           'rx_x': {'1584426525': 1.1, '1584438532': 1.2},
                                                                           'waterline': {'1584426525': 1.1, '1584438532': 1.2}}
        assert trim_xyzrprh_to_times(data_one, 1584426522, 1584426525) is None

    def test_overlapping_timestamps(self):
        timestamps = [1584426525, 1584429900, 1584430000, 1584438532, 1597569340]
        starttime = 1584429999
        endtime = 1590000000
        tstmps = get_overlapping_timestamps(timestamps, starttime, endtime)
        assert tstmps == ['1584429900', '1584430000', '1584438532']

    def test_compare_dict(self):
        data_one = {"latency": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                    "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                    "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}
        data_two = {"latency": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576},
                    "rx_x": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3},
                    "waterline": {"1584426525": 1.1, "1584438532": 1.2, "1597569340": 1.3}}

        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert identical_offsets
        assert identical_angles
        assert identical_tpu
        assert data_matches
        assert not new_waterline

        data_two["latency"]["1584426525"] = 1.0
        # detect a new latency value, only looks at the first timestamp
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert identical_offsets
        assert not identical_angles
        assert identical_tpu
        assert not data_matches
        assert not new_waterline
        data_two["latency"]["1584426525"] = 0.1

        data_two["roll_patch_error"]["1584438532"] = 999
        # now fails tpu check and data match check
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert identical_offsets
        assert identical_angles
        assert not identical_tpu
        assert not data_matches
        assert not new_waterline

        data_two["rx_h"]["1584438532"] = 999
        # now fails all three
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert identical_offsets
        assert not identical_angles
        assert not identical_tpu
        assert not data_matches
        assert not new_waterline

        data_two["rx_x"]["1584438532"] = 999
        # now fails all four
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert not identical_offsets
        assert not identical_angles
        assert not identical_tpu
        assert not data_matches
        assert not new_waterline

        data_two["waterline"]["1584426525"] = 999
        # detect a new waterline value, only looks at the first timestamp
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
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
        identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(data_one,
                                                                                                            data_two)
        assert not identical_offsets
        assert not identical_angles
        assert not identical_tpu
        assert data_matches
        assert not new_waterline

    def test_carry_over_optional(self):
        data_one = {"roll_patch_error": {"1584426525": 0.1, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 0.0005, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
        data_two = {"roll_patch_error": {"1584426525": 999, "1584438532": 0.1, "1597569340": 0.1},
                    "roll_sensor_error": {"1584426525": 0.0005, "1584438532": 999, "1597569340": 0.0005},
                    "rx_h": {"1584426525": 359.576, "1584438532": 359.576, "1597569340": 359.576}}
        new_data_two = carry_over_optional(data_one, data_two)
        assert new_data_two == data_one
        assert data_two["roll_patch_error"]["1584426525"] == 0.1

    def test_only_retain(self):
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

    def test_convert_from_fqpr_xyzrph(self):
        vess_xyzrph = convert_from_fqpr_xyzrph(test_xyzrph, 'em2040', '123', 'test.all')
        assert list(vess_xyzrph.keys()) == ['123']
        assert vess_xyzrph['123'].pop('sonar_type') == {'1626354881': 'em2040'}
        assert vess_xyzrph['123'].pop('source') == {'1626354881': 'test.all'}
        assert vess_xyzrph['123'] == test_xyzrph

    def test_convert_from_vessel_xyzrph(self):
        vess_xyzrph = convert_from_fqpr_xyzrph(test_xyzrph, 'em2040', '123', 'test.all')
        backconvert_xyzrph, sonar_type, system_identifier, source = convert_from_vessel_xyzrph(vess_xyzrph)
        assert backconvert_xyzrph == [test_xyzrph]
        assert sonar_type == [{'1626354881': 'em2040'}]
        assert system_identifier == ['123']
        assert source == [{'1626354881': 'test.all'}]
