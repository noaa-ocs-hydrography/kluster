import time
import shutil

from HSTB.kluster.fqpr_intelligence import *
from HSTB.kluster.fqpr_project import *


def get_testfile_paths():
    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    testsv = os.path.join(os.path.dirname(testfile), '2020_036_182635.svp')
    expected_data_folder = 'em2040_40111_05_23_2017'
    expected_data_folder_path = os.path.join(os.path.dirname(testfile), expected_data_folder)
    return testfile, testsv, expected_data_folder, expected_data_folder_path


def cleanup_after_tests():
    testfile, testsv, expected_data_folder, expected_data_folder_path = get_testfile_paths()
    proj_path = os.path.join(os.path.dirname(testfile), 'kluster_project.json')
    os.remove(proj_path)
    vessel_file = os.path.join(os.path.dirname(testfile), 'vessel_file.kfc')
    os.remove(vessel_file)

    if os.path.exists(expected_data_folder_path):
        shutil.rmtree(expected_data_folder_path)


def setup_intel():
    testfile, testsv, expected_data_folder, expected_data_folder_path = get_testfile_paths()

    proj_path = os.path.join(os.path.dirname(testfile), 'kluster_project.json')
    vessel_file = os.path.join(os.path.dirname(testfile), 'vessel_file.kfc')

    if os.path.exists(proj_path):
        os.remove(proj_path)
    if os.path.exists(vessel_file):
        os.remove(vessel_file)
    proj = create_new_project(os.path.dirname(testfile))
    proj.add_vessel_file(vessel_file)
    fintel = FqprIntel(proj)
    return proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path


def test_intel_add_multibeam():
    proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path = setup_intel()
    updated_type, new_data, new_project = fintel.add_file(testfile)

    assert os.path.exists(proj_path)
    assert os.path.exists(vessel_file)

    assert updated_type == 'multibeam'
    assert new_data['file_path'] == testfile
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
    assert new_data['file_name'] == '0009_20170523_181119_FA2806.all'
    assert not new_project

    assert fintel.multibeam_intel.line_groups == {expected_data_folder_path: [testfile]}
    assert fintel.multibeam_intel.unmatched_files == {}
    assert fintel.multibeam_intel.file_name == {testfile: '0009_20170523_181119_FA2806.all'}
    assert fintel.multibeam_intel.matching_fqpr[testfile] == ''

    fintel.clear()

    assert fintel.multibeam_intel.line_groups == {}
    assert fintel.multibeam_intel.unmatched_files == {}
    assert fintel.multibeam_intel.file_name == {}
    assert fintel.multibeam_intel.matching_fqpr == {}

    proj.close()
    fintel = None
    proj = None
    cleanup_after_tests()


def test_intel_remove_multibeam():
    proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path = setup_intel()

    updated_type, new_data, new_project = fintel.add_file(testfile)
    assert updated_type == 'multibeam'  # file was added

    updated_type, uid = fintel.remove_file(testfile)
    assert updated_type == 'multibeam'
    assert uid == 0

    assert fintel.multibeam_intel.line_groups == {}
    assert fintel.multibeam_intel.unmatched_files == {}
    assert fintel.multibeam_intel.file_name == {}
    assert fintel.multibeam_intel.matching_fqpr == {}

    fintel.clear()
    proj.close()
    fintel = None
    proj = None
    cleanup_after_tests()


def test_intel_add_sv():
    proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path = setup_intel()
    updated_type, new_data, new_project = fintel.add_file(testsv)

    assert os.path.exists(proj_path)

    assert updated_type == 'svp'
    assert new_data['file_path'] == testsv
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
    assert new_data['file_name'] == '2020_036_182635.svp'
    assert not new_project

    assert fintel.svp_intel.file_paths == [testsv]
    assert fintel.svp_intel.file_path == {'2020_036_182635.svp': testsv}
    assert fintel.svp_intel.file_name == {testsv: '2020_036_182635.svp'}
    assert fintel.svp_intel.unique_id_reverse == {0: testsv}
    assert fintel.svp_intel.type == {testsv: 'caris_svp'}

    fintel.clear()

    assert fintel.svp_intel.file_paths == []
    assert fintel.svp_intel.file_path == {}
    assert fintel.svp_intel.file_name == {}
    assert fintel.svp_intel.unique_id_reverse == {}
    assert fintel.svp_intel.type == {}

    proj.close()
    fintel = None
    proj = None
    cleanup_after_tests()


def test_intel_remove_sv():
    proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path = setup_intel()
    updated_type, new_data, new_project = fintel.add_file(testsv)
    assert updated_type == 'svp'  # file was added

    updated_type, uid = fintel.remove_file(testsv)
    assert updated_type == 'svp'
    assert uid == 0

    assert fintel.svp_intel.file_paths == []
    assert fintel.svp_intel.file_path == {}
    assert fintel.svp_intel.file_name == {}
    assert fintel.svp_intel.unique_id_reverse == {}
    assert fintel.svp_intel.type == {}

    fintel.clear()
    proj.close()
    fintel = None
    proj = None
    cleanup_after_tests()


def test_intel_vessel_file():
    proj, fintel, proj_path, vessel_file, testfile, testsv, expected_data_folder_path = setup_intel()
    updated_type, new_data, new_project = fintel.add_file(testfile)
    # convert multibeam file
    fintel.execute_action()
    vf = fintel.project.return_vessel_file()
    converted_fqpr = list(fintel.project.fqpr_instances.values())[0]
    # after conversion, the offsets from this converted data will be stored in the vessel file
    expected_offsets = {'beam_opening_angle': {'1495563079': 1.3}, 'heading_patch_error': {'1495563079': 0.5},
                        'heading_sensor_error': {'1495563079': 0.02}, 'heave_error': {'1495563079': 0.05},
                        'horizontal_positioning_error': {'1495563079': 1.5}, 'imu_h': {'1495563079': 0.4},
                        'latency': {'1495563079': 0.0}, 'imu_p': {'1495563079': -0.18},
                        'imu_r': {'1495563079': -0.16}, 'imu_x': {'1495563079': 0.0},
                        'imu_y': {'1495563079': 0.0}, 'imu_z': {'1495563079': 0.0},
                        'latency_patch_error': {'1495563079': 0.0}, 'pitch_patch_error': {'1495563079': 0.1},
                        'pitch_sensor_error': {'1495563079': 0.0005}, 'roll_patch_error': {'1495563079': 0.1},
                        'roll_sensor_error': {'1495563079': 0.0005}, 'rx_h': {'1495563079': 0.0},
                        'rx_p': {'1495563079': 0.0}, 'rx_r': {'1495563079': 0.0},
                        'rx_x': {'1495563079': -0.1}, 'rx_x_0': {'1495563079': 0.011}, 'rx_x_1': {'1495563079': 0.011},
                        'rx_x_2': {'1495563079': 0.011}, 'rx_y': {'1495563079': -0.304}, 'rx_y_0': {'1495563079': 0.0},
                        'rx_y_1': {'1495563079': 0.0}, 'rx_y_2': {'1495563079': 0.0}, 'rx_z': {'1495563079': -0.016},
                        'rx_z_0': {'1495563079': -0.006}, 'rx_z_1': {'1495563079': -0.006},
                        'rx_z_2': {'1495563079': -0.006}, 'separation_model_error': {'1495563079': 0.0},
                        'sonar_type': {'1495563079': 'em2040'}, 'source': {'1495563079': 'em2040_40111_05_23_2017'},
                        'surface_sv_error': {'1495563079': 0.5}, 'timing_latency_error': {'1495563079': 0.001},
                        'tx_h': {'1495563079': 0.0}, 'tx_p': {'1495563079': 0.0}, 'tx_r': {'1495563079': 0.0},
                        'tx_to_antenna_x': {'1495563079': 0.0}, 'tx_to_antenna_y': {'1495563079': 0.0},
                        'tx_to_antenna_z': {'1495563079': 0.0}, 'tx_x': {'1495563079': 0.0},
                        'tx_x_0': {'1495563079': 0.0}, 'tx_x_1': {'1495563079': 0.0}, 'tx_x_2': {'1495563079': 0.0},
                        'tx_y': {'1495563079': 0.0}, 'tx_y_0': {'1495563079': -0.0554},
                        'tx_y_1': {'1495563079': 0.0131}, 'tx_y_2': {'1495563079': 0.0554}, 'tx_z': {'1495563079': 0.0},
                        'tx_z_0': {'1495563079': -0.012}, 'tx_z_1': {'1495563079': -0.006},
                        'tx_z_2': {'1495563079': -0.012}, 'vertical_positioning_error': {'1495563079': 1.0},
                        'vessel_speed_error': {'1495563079': 0.1}, 'waterline': {'1495563079': -0.64},
                        'waterline_error': {'1495563079': 0.02}, 'x_offset_error': {'1495563079': 0.2},
                        'y_offset_error': {'1495563079': 0.2}, 'z_offset_error': {'1495563079': 0.2}}

    assert vf.data[converted_fqpr.multibeam.raw_ping[0].system_identifier] == expected_offsets
    fintel.execute_action()

    assert not fintel.has_actions

    vf.update('40111', {'beam_opening_angle': {'1495563079': 999}}, carry_over_tpu=False)
    vf.save()
    fintel.regenerate_actions()
    # after regenerating actions, we have a new compute tpu action since we changed this tpu value
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 only computing TPU'

    vf.update('40111', {'rx_p': {'1495563079': 999}})
    vf.save()
    fintel.regenerate_actions()
    # after regenerating actions, we have a new all processing action since we changed a patch test angle
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Run all processing on em2040_40111_05_23_2017'

    vf.update('40111', {'rx_p': {'1495563079': 0.0}})
    vf.save()
    fintel.regenerate_actions()
    # after regenerating actions, we are back to the compute tpu action, since we reverted the patch test change
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 only computing TPU'

    vf.update('40111', {'rx_x': {'1495563079': 999}})
    vf.save()
    fintel.regenerate_actions()
    # after regenerating actions, we have a new georeferencing action since we changed a lever arm, it overrides the tpu
    # action, as we will do a tpu process after georeferencing for the lever arm change anyway
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 starting with sound velocity'

    vf.update('40111', {'rx_x': {'1495563079': -0.1}})
    vf.save()
    fintel.regenerate_actions()
    # after regenerating actions, we are back to the compute tpu action, since we reverted the lever arm change
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 only computing TPU'

    converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = 999
    fintel.regenerate_actions(keep_waterline_changes=False)
    # after regenerating actions, we have no new action as we have disabled retaining waterline changes
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 only computing TPU'
    vf = fintel.project.return_vessel_file()
    assert vf.data['40111']['waterline']['1495563079'] == -0.64

    converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = 999
    fintel.regenerate_actions(keep_waterline_changes=True)
    # after regenerating actions, we have a new sound velocity process as we adjusted the existing waterline value, and
    #   waterline changes in existing data are honored.
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 starting with sound velocity'
    vf = fintel.project.return_vessel_file()
    assert vf.data['40111']['waterline']['1495563079'] == 999

    # reverting the waterline action requires regenerating actions twice for now...
    converted_fqpr.multibeam.raw_ping[0].attrs['xyzrph']['waterline']['1495563079'] = -0.64
    fintel.regenerate_actions(keep_waterline_changes=True)
    fintel.regenerate_actions(keep_waterline_changes=True)
    # after regenerating actions, we have a new sound velocity process as we adjusted the existing waterline value, and
    #   waterline changes in existing data are honored.
    assert fintel.has_actions
    assert fintel.action_container.actions[0].text == 'Process em2040_40111_05_23_2017 only computing TPU'
    vf = fintel.project.return_vessel_file()
    assert vf.data['40111']['waterline']['1495563079'] == -0.64

    # reverting the tpu action
    vf.update('40111', {'beam_opening_angle': {'1495563079': 1.3}}, carry_over_tpu=False)
    vf.save()
    fintel.regenerate_actions()
    assert not fintel.has_actions

    fintel.clear()
    proj.close()
    fintel = None
    proj = None
    cleanup_after_tests()

# some issue with pytest hanging when we use the folder monitoring stuff
# not sure what to do here, stopping/joining the observer is what the docs say to do

# def test_intel_monitor():
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
#     assert fintel.svp_intel.file_path == {'2020_036_182635.svp': testsv}
#     assert fintel.svp_intel.file_name == {testsv: '2020_036_182635.svp'}
#     assert fintel.svp_intel.type == {testsv: 'caris_svp'}
#
#     assert fintel.multibeam_intel.line_groups == {expected_data_folder_path: [testfile]}
#     assert fintel.multibeam_intel.unmatched_files == {}
#     assert fintel.multibeam_intel.file_name == {testfile: '0009_20170523_181119_FA2806.all'}
#     assert fintel.multibeam_intel.matching_fqpr[testfile] == ''
#
#     fintel.clear()
#     proj.close()
#     fintel = None
#     proj = None
#     cleanup_after_tests()
