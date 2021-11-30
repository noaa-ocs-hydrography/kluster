import os
import shutil
import unittest
from datetime import datetime, timezone

from HSTB.kluster.fqpr_intelligence import FqprIntel

# TODO: Mock
from HSTB.kluster.fqpr_project import create_new_project

#TODO: clean up
class TestFqprIntelligence(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.filename = '0009_20170523_181119_FA2806.all'
        cls.svname = '2020_036_182635.svp'
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', cls.filename)
        cls.testsv = os.path.join(os.path.dirname(cls.testfile), cls.svname)
        cls.expected_data_folder = 'em2040_40111_05_23_2017'
        cls.expected_data_folder_path = os.path.join(os.path.dirname(cls.testfile), cls.expected_data_folder)

    @classmethod
    def tearDownClass(cls) -> None:
        proj_path = os.path.join(os.path.dirname(cls.testfile), 'kluster_project.json')
        if os.path.exists(proj_path):
            os.remove(proj_path)
        vessel_file = os.path.join(os.path.dirname(cls.testfile), 'vessel_file.kfc')
        if os.path.exists(vessel_file):
            os.remove(vessel_file)
        if os.path.exists(cls.expected_data_folder_path):
            shutil.rmtree(cls.expected_data_folder_path)

    def setUp(self) -> None:
        self.proj_path = os.path.join(os.path.dirname(self.testfile), 'kluster_project.json')
        self.vessel_file = os.path.join(os.path.dirname(self.testfile), 'vessel_file.kfc')

        self.proj = create_new_project(os.path.dirname(self.testfile))
        self.proj.add_vessel_file(self.vessel_file)
        self.fintel = FqprIntel(self.proj)
        self.fintel.set_settings({'coord_system': 'NAD83'})

    def tearDown(self) -> None:
        self.fintel.clear()
        self.proj.close()
        os.remove(self.proj_path)
        os.remove(self.vessel_file)

    def test_intel_add_multibeam(self):
        updated_type, new_data, new_project = self.fintel.add_file(self.testfile)

        assert updated_type == 'multibeam'
        assert new_data['file_path'] == self.testfile
        assert new_data['type'] == 'kongsberg_all'
        assert new_data['data_start_time_utc'] == datetime(2017, 5, 23, 18, 11, 19, 364000, tzinfo=timezone.utc)
        assert new_data['data_end_time_utc'] == datetime(2017, 5, 23, 18, 12, 13, 171000, tzinfo=timezone.utc)
        assert new_data['primary_serial_number'] == 40111
        assert new_data['secondary_serial_number'] == 0
        assert new_data['sonar_model_number'] == 'em2040'
        assert 'last_modified_time_utc' in new_data  # can't check content of this, depends on the env
        assert 'created_time_utc' in new_data  # can't check content of this, depends on the env
        assert 'time_added' in new_data  # can't check content of this, depends on the env
        assert new_data['unique_id'] == 0
        assert new_data['file_name'] == self.filename
        assert not new_project

        assert self.fintel.multibeam_intel.line_groups == {self.expected_data_folder_path: [self.testfile]}
        assert self.fintel.multibeam_intel.unmatched_files == {}
        assert self.fintel.multibeam_intel.file_name == {self.testfile: self.filename}
        assert self.fintel.multibeam_intel.matching_fqpr[self.testfile] == ''

        self.fintel.clear()

        assert self.fintel.multibeam_intel.line_groups == {}
        assert self.fintel.multibeam_intel.unmatched_files == {}
        assert self.fintel.multibeam_intel.file_name == {}
        assert self.fintel.multibeam_intel.matching_fqpr == {}

    def test_intel_remove_multibeam(self):
        updated_type, new_data, new_project = self.fintel.add_file(self.testfile)

        assert updated_type == 'multibeam'  # file was added

        updated_type, uid = self.fintel.remove_file(self.testfile)
        assert updated_type == 'multibeam'
        assert uid == 0

        assert self.fintel.multibeam_intel.line_groups == {}
        assert self.fintel.multibeam_intel.unmatched_files == {}
        assert self.fintel.multibeam_intel.file_name == {}
        assert self.fintel.multibeam_intel.matching_fqpr == {}

    def test_intel_add_sv(self):
        updated_type, new_data, new_project = self.fintel.add_file(self.testsv)
        assert updated_type == 'svp'
        assert new_data['file_path'] == self.testsv
        assert new_data['type'] == 'caris_svp'
        assert new_data['profiles'] == [[(0.031, 1487.619079),
                                         (1.031, 1489.224413),
                                         (2.031, 1490.094255),
                                         (3.031, 1490.282542),
                                         (4.031, 1490.455471),
                                         (5.031, 1490.606669),
                                         (6.031, 1490.694613),
                                         (7.031, 1490.751968),
                                         (8.031, 1490.811492),
                                         (9.031, 1490.869682),
                                         (10.031, 1490.923819),
                                         (11.031, 1490.981475),
                                         (12.031, 1491.058214),
                                         (13.031, 1491.107904),
                                         (14.031, 1491.156586),
                                         (15.031, 1491.22292),
                                         (16.031, 1491.26239),
                                         (17.031, 1491.306912),
                                         (18.031, 1491.355384),
                                         (19.031, 1491.414501),
                                         (20.031, 1491.45854),
                                         (21.031, 1491.480412),
                                         (22.031, 1491.504141),
                                         (23.031, 1491.519287)]]
        assert new_data['number_of_profiles'] == 1
        assert new_data['number_of_layers'] == [24]
        assert new_data['julian_day'] == ['2020-036']
        assert new_data['time_utc'] == [datetime(2020, 2, 5, 18, 26, tzinfo=timezone.utc)]
        assert new_data['time_utc_seconds'] == [1580927160.0]
        assert new_data['latitude'] == [37.85094444]
        assert new_data['longitude'] == [-122.46491667]
        assert new_data['source_epsg'] == [4326]
        assert new_data['utm_zone'] == [10]
        assert new_data['utm_hemisphere'] == ['N']
        assert 'last_modified_time_utc' in new_data  # can't check content of this, depends on the env
        assert 'created_time_utc' in new_data  # can't check content of this, depends on the env
        assert 'time_added' in new_data  # can't check content of this, depends on the env
        assert new_data['unique_id'] == 0
        assert new_data['file_name'] == self.svname
        assert not new_project

        assert self.fintel.svp_intel.file_paths == [self.testsv]
        assert self.fintel.svp_intel.file_path == {self.svname: self.testsv}
        assert self.fintel.svp_intel.file_name == {self.testsv: self.svname}
        assert self.fintel.svp_intel.unique_id_reverse == {0: self.testsv}
        assert self.fintel.svp_intel.type == {self.testsv: 'caris_svp'}

        self.fintel.clear()

        assert self.fintel.svp_intel.file_paths == []
        assert self.fintel.svp_intel.file_path == {}
        assert self.fintel.svp_intel.file_name == {}
        assert self.fintel.svp_intel.unique_id_reverse == {}
        assert self.fintel.svp_intel.type == {}

    def test_intel_remove_sv(self):
        updated_type, new_data, new_project = self.fintel.add_file(self.testsv)
        assert updated_type == 'svp'  # file was added

        updated_type, uid = self.fintel.remove_file(self.testsv)
        assert updated_type == 'svp'
        assert uid == 0

        assert self.fintel.svp_intel.file_paths == []
        assert self.fintel.svp_intel.file_path == {}
        assert self.fintel.svp_intel.file_name == {}
        assert self.fintel.svp_intel.unique_id_reverse == {}
        assert self.fintel.svp_intel.type == {}

    #todo: get working
    def test_intel_modes(self):
        proj = create_new_project(os.path.dirname(self.testfile))
        fintel = FqprIntel(proj)
        fintel.set_settings({'coord_system': 'NAD83'})

        # convert multibeam file
        fintel.execute_action()
        # normal mode will have a new processing action for that day
        assert fintel.has_actions
        assert fintel.action_container.actions[0].text == 'Run all processing on em2040_40111_05_23_2017'
        # convert only will have no actions, since we've already converted
        fintel.set_auto_processing_mode('convert_only')
        assert not fintel.has_actions
        # concatenate will have a new action to only convert this one line
        fintel.set_auto_processing_mode('concatenate')
        assert fintel.has_actions
        assert fintel.action_container.actions[0].text == 'Run all processing on em2040_40111_05_23_2017'
        assert fintel.action_container.actions[0].kwargs['only_this_line'] == self.filename

    def test_intel_vessel_file(self):
        self.fintel.add_file(self.testfile)

        # convert multibeam file
        self.fintel.execute_action()
        vf = self.fintel.project.return_vessel_file()
        converted_fqpr = list(self.fintel.project.fqpr_instances.values())[0]
        # after conversion, the offsets from this converted data will be stored in the vessel file
        expected_offsets = {'beam_opening_angle': {'1495563079': 1.3}, 'heading_patch_error': {'1495563079': 0.5},
                            'heading_sensor_error': {'1495563079': 0.02}, 'heave_error': {'1495563079': 0.05},
                            'horizontal_positioning_error': {'1495563079': 1.5}, 'imu_h': {'1495563079': 0.4},
                            'latency': {'1495563079': 0.0}, 'imu_p': {'1495563079': -0.18},
                            'imu_r': {'1495563079': -0.16}, 'imu_x': {'1495563079': 0.0},
                            'imu_y': {'1495563079': 0.0}, 'imu_z': {'1495563079': 0.0},
                            'latency_patch_error': {'1495563079': 0.0}, 'pitch_patch_error': {'1495563079': 0.1},
                            'pitch_sensor_error': {'1495563079': 0.001}, 'roll_patch_error': {'1495563079': 0.1},
                            'roll_sensor_error': {'1495563079': 0.001}, 'rx_h': {'1495563079': 0.0},
                            'rx_p': {'1495563079': 0.0}, 'rx_r': {'1495563079': 0.0},
                            'rx_x': {'1495563079': -0.1}, 'rx_x_0': {'1495563079': 0.011},
                            'rx_x_1': {'1495563079': 0.011},
                            'rx_x_2': {'1495563079': 0.011}, 'rx_y': {'1495563079': -0.304},
                            'rx_y_0': {'1495563079': 0.0},
                            'rx_y_1': {'1495563079': 0.0}, 'rx_y_2': {'1495563079': 0.0},
                            'rx_z': {'1495563079': -0.016},
                            'rx_z_0': {'1495563079': -0.006}, 'rx_z_1': {'1495563079': -0.006},
                            'rx_z_2': {'1495563079': -0.006}, 'separation_model_error': {'1495563079': 0.0},
                            'sonar_type': {'1495563079': 'em2040'}, 'source': {'1495563079': 'em2040_40111_05_23_2017'},
                            'surface_sv_error': {'1495563079': 0.5}, 'timing_latency_error': {'1495563079': 0.001},
                            'tx_h': {'1495563079': 0.0}, 'tx_p': {'1495563079': 0.0}, 'tx_r': {'1495563079': 0.0},
                            'tx_to_antenna_x': {'1495563079': 0.0}, 'tx_to_antenna_y': {'1495563079': 0.0},
                            'tx_to_antenna_z': {'1495563079': 0.0}, 'tx_x': {'1495563079': 0.0},
                            'tx_x_0': {'1495563079': 0.0}, 'tx_x_1': {'1495563079': 0.0}, 'tx_x_2': {'1495563079': 0.0},
                            'tx_y': {'1495563079': 0.0}, 'tx_y_0': {'1495563079': -0.0554},
                            'tx_y_1': {'1495563079': 0.0131}, 'tx_y_2': {'1495563079': 0.0554},
                            'tx_z': {'1495563079': 0.0},
                            'tx_z_0': {'1495563079': -0.012}, 'tx_z_1': {'1495563079': -0.006},
                            'tx_z_2': {'1495563079': -0.012}, 'vertical_positioning_error': {'1495563079': 1.0},
                            'vessel_speed_error': {'1495563079': 0.1}, 'waterline': {'1495563079': -0.64},
                            'waterline_error': {'1495563079': 0.02}, 'x_offset_error': {'1495563079': 0.2},
                            'y_offset_error': {'1495563079': 0.2}, 'z_offset_error': {'1495563079': 0.2}}

        assert vf.data[converted_fqpr.multibeam.raw_ping[0].system_identifier] == expected_offsets
        self.fintel.execute_action()

        assert not self.fintel.has_actions

        tpu_action_text = 'Process em2040_40111_05_23_2017 only computing TPU'

        vf.update('40111', {'beam_opening_angle': {'1495563079': 999}}, carry_over_tpu=False)
        vf.save()
        self.fintel.regenerate_actions()
        # after regenerating actions, we have a new compute tpu action since we changed this tpu value
        self.assert_actions(tpu_action_text)

        vf.update('40111', {'rx_p': {'1495563079': 999}})
        vf.save()
        self.fintel.regenerate_actions()
        # after regenerating actions, we have a new all processing action since we changed a patch test angle
        self.assert_actions('Run all processing on em2040_40111_05_23_2017')

        vf.update('40111', {'rx_p': {'1495563079': 0.0}})
        vf.save()
        self.fintel.regenerate_actions()
        # after regenerating actions, we are back to the compute tpu action, since we reverted the patch test change
        self.assert_actions(tpu_action_text)

        vf.update('40111', {'rx_x': {'1495563079': 999}})
        vf.save()
        self.fintel.regenerate_actions()
        # after regenerating actions, we have a new georeferencing action since we changed a lever arm, it overrides the tpu
        # action, as we will do a tpu process after georeferencing for the lever arm change anyway
        self.assert_actions('Process em2040_40111_05_23_2017 starting with sound velocity')

        vf.update('40111', {'rx_x': {'1495563079': -0.1}})
        vf.save()
        self.fintel.regenerate_actions()
        # after regenerating actions, we are back to the compute tpu action, since we reverted the lever arm change
        self.assert_actions(tpu_action_text)

        converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = 999
        self.fintel.keep_waterline_changes = False
        self.fintel.regenerate_actions()
        # after regenerating actions, we have no new action as we have disabled retaining waterline changes
        self.assert_actions(tpu_action_text, -0.64)

        converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = 999
        self.fintel.keep_waterline_changes = True
        self.fintel.regenerate_actions()
        # after regenerating actions, we have a new sound velocity process as we adjusted the existing waterline value, and
        #   waterline changes in existing data are honored.
        self.assert_actions('Process em2040_40111_05_23_2017 starting with sound velocity', 999)

        # reverting the waterline action requires regenerating actions twice for now...
        converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = -0.64
        self.fintel.keep_waterline_changes = True
        self.fintel.regenerate_actions()
        self.fintel.regenerate_actions()
        # after regenerating actions, we have a new sound velocity process as we adjusted the existing waterline value, and
        #   waterline changes in existing data are honored.
        self.assert_actions(tpu_action_text, -0.64)

        # reverting the tpu action
        vf.update('40111', {'beam_opening_angle': {'1495563079': 1.3}}, carry_over_tpu=False)
        vf.save()
        self.fintel.regenerate_actions()
        assert not self.fintel.has_actions

    def assert_actions(self, text: str, waterline_val: float = None):
        assert self.fintel.has_actions
        assert self.fintel.action_container.actions[0].text == text
        if waterline_val:
            vf = self.fintel.project.return_vessel_file()
            assert vf.data['40111']['waterline']['1495563079'] == waterline_val


    def test_folder_monitoring(self):
        """
        Test fqpr intelligence by kicking off a folder monitoring session, finding the test multibeam file, and checking
        the resulting actions to see if the conversion action matches expectations.
        """
        self.fintel.add_file(self.testfile)

        assert str(self.fintel.action_container) == "FqprActionContainer: 1 actions of types: ['multibeam']"
        assert len(self.fintel.action_container.actions) == 1

        action = self.fintel.action_container.actions[0]
        assert action.text[0:25] == 'Convert 1 multibeam lines'
        assert action.action_type == 'multibeam'
        assert action.priority == 1
        assert action.is_running == False
        assert len(action.input_files) == 1
        assert action.kwargs == {}
        assert action.args[2:] == [None, False, True]

        self.fintel.execute_action()
        action = self.fintel.action_container.actions[0]
        assert action.text[0:21] == 'Run all processing on'
        assert action.action_type == 'processing'
        assert action.priority == 5
        assert action.is_running is False
        assert len(action.input_files) == 0
        assert action.kwargs == {'run_orientation': True, 'orientation_initial_interpolation': False,
                                 'run_beam_vec': True,
                                 'run_svcorr': True, 'add_cast_files': [], 'run_georef': True, 'run_tpu': True,
                                 'use_epsg': False,
                                 'use_coord': True, 'epsg': None, 'coord_system': 'NAD83', 'vert_ref': 'waterline'}
        assert self.proj.fqpr_instances['em2040_40111_05_23_2017'] == self.proj.return_line_owner(
            '0009_20170523_181119_FA2806.all')


    # some issue with pytest hanging when we use the folder monitoring stuff
    # not sure what to do here, stopping/joining the observer is what the docs say to do

    # def test_intel_monitor(self):
    #     testfile, testsv, expected_data_folder, expected_data_folder_path = get_testfile_paths()
    #
    #     proj_path = os.path.join(os.path.dirname(testfile), 'kluster_project.json')
    #     if os.path.exists(proj_path):
    #         os.remove(proj_path)
    #     proj = create_new_project(os.path.dirname(testfile))
    #     fintel = FqprIntel(proj)
    #
    #     fintel.start_folder_monitor(os.path.dirname(testfile))
    #     time.sleep(5)
    #     if not fintel.svp_intel.file_paths or not fintel.multibeam_intel.file_paths:  # might need a bit longer
    #         time.sleep(10)
    #     fintel.stop_folder_monitor(os.path.dirname(testfile))
    #
    #     assert fintel.svp_intel.file_paths == [testsv]
    #     assert fintel.svp_intel.file_path == {self.svname: testsv}
    #     assert fintel.svp_intel.file_name == {testsv: self.svname}
    #     assert fintel.svp_intel.type == {testsv: 'caris_svp'}
    #
    #     assert fintel.multibeam_intel.line_groups == {expected_data_folder_path: [testfile]}
    #     assert fintel.multibeam_intel.unmatched_files == {}
    #     assert fintel.multibeam_intel.file_name == {testfile: self.filename}
    #     assert fintel.multibeam_intel.matching_fqpr[testfile] == ''
    #
    #     fintel.clear()
    #     proj.close()
    #     fintel = None
    #     proj = None
    #     cleanup_after_tests()
