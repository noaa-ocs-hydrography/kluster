import os
import shutil
import unittest
import tempfile

from HSTB.kluster import fqpr_generation

from HSTB.kluster.fqpr_actions import build_multibeam_action, update_kwargs_for_multibeam, build_svp_action, \
    update_kwargs_for_svp


class TestFqprActions(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')
        cls.expected_output = os.path.join(tempfile.tempdir, 'TestFqprAction')
        os.mkdir(cls.expected_output)
        cls.testsv = os.path.join(os.path.dirname(cls.testfile), '2020_036_182635.svp')

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.expected_output)

    def test_build_multibeam_action(self):
        act = build_multibeam_action(self.expected_output, [self.testfile], settings={'parallel_write': False})
        assert act.args == [[self.testfile], self.expected_output, None, False, True]
        assert act.kwargs == {'parallel_write': False}
        assert act.input_files == [self.testfile]
        assert act.action_type == 'multibeam'
        assert act.priority == 1
        assert act.is_running is False

    def test_update_kwargs_for_multibeam(self):
        sets = update_kwargs_for_multibeam(self.expected_output, [self.testfile], settings={'parallel_write': False})

        assert sets['args'] == [[self.testfile], self.expected_output, None, False, True]
        assert sets['kwargs'] == {'parallel_write': False}
        assert sets['tooltip_text'] == self.testfile
        assert sets['input_files'] == [self.testfile]

    def test_build_svp_action(self):
        empty_fq = fqpr_generation.Fqpr()
        act = build_svp_action(self.expected_output, empty_fq, [self.testsv])
        assert act.args == [empty_fq, [self.testsv]]
        assert act.kwargs is None
        assert act.input_files == [self.testsv]
        assert act.action_type == 'svp'
        assert act.priority == 3
        assert act.is_running is False
        empty_fq.close()

    def test_update_kwargs_for_svp(self):
        empty_fq = fqpr_generation.Fqpr()
        sets = update_kwargs_for_svp(self.expected_output, empty_fq, [self.testsv])

        assert sets['args'] == [empty_fq, [self.testsv]]
        assert sets['tooltip_text'] == self.testsv
        assert sets['input_files'] == [self.testsv]
        empty_fq.close()
