import os

from HSTB.kluster.fqpr_actions import *


def get_testfile_paths():
    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    expected_output = os.path.join(os.path.dirname(testfile), 'converted')
    testsv = os.path.join(os.path.dirname(testfile), '2020_036_182635.svp')
    return testfile, testsv, expected_output


def test_build_multibeam_action():
    testfile, testsv, expected_output = get_testfile_paths()
    act = build_multibeam_action(expected_output, [testfile], settings={'parallel_write': False})
    assert act.args == [[testfile], expected_output, None, False, True]
    assert act.kwargs == {'parallel_write': False}
    assert act.input_files == [testfile]
    assert act.action_type == 'multibeam'
    assert act.priority == 1
    assert act.is_running is False


def test_update_kwargs_for_multibeam():
    testfile, testsv, expected_output = get_testfile_paths()
    sets = update_kwargs_for_multibeam(expected_output, [testfile], settings={'parallel_write': False})

    assert sets['args'] == [[testfile], expected_output, None, False, True]
    assert sets['kwargs'] == {'parallel_write': False}
    assert sets['tooltip_text'] == testfile
    assert sets['input_files'] == [testfile]


def test_build_svp_action():
    testfile, testsv, expected_output = get_testfile_paths()
    empty_fq = fqpr_generation.Fqpr()
    act = build_svp_action(expected_output, empty_fq, [testsv])
    assert act.args == [empty_fq, [testsv]]
    assert act.kwargs is None
    assert act.input_files == [testsv]
    assert act.action_type == 'svp'
    assert act.priority == 3
    assert act.is_running is False


def test_update_kwargs_for_svp():
    testfile, testsv, expected_output = get_testfile_paths()
    empty_fq = fqpr_generation.Fqpr()
    sets = update_kwargs_for_svp(expected_output, empty_fq, [testsv])

    assert sets['args'] == [empty_fq, [testsv]]
    assert sets['tooltip_text'] == testsv
    assert sets['input_files'] == [testsv]
