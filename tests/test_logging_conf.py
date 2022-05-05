import logging
import unittest
import os

from HSTB.kluster.logging_conf import *


class TestLoggingConf(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', 'test_log.txt')
        cls.logger = None

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(cls.testfile):
            os.remove(cls.testfile)

    def tearDown(self) -> None:
        if self.logger:
            for handler in self.logger.handlers:
                handler.close()

    def test_return_logger(self):
        self.logger = return_logger('test_log', None)

        assert isinstance(self.logger, logging.Logger)
        assert self.logger.level == logging.INFO
        assert self.logger.name[0:9] == 'test_log_'

    def test_return_log_name(self):
        assert return_log_name() == 'logfile.txt'
        assert return_log_name(True)[:7] == 'logfile'
        assert len(return_log_name(True)) == 22

    def test_logger_remove_file_handlers(self):
        self.logger = return_logger('test_log', None)
        assert len(self.logger.handlers) == 2
        add_file_handler(self.logger, self.testfile)
        assert len(self.logger.handlers) == 3
        logger_remove_file_handlers(self.logger)
        assert len(self.logger.handlers) == 2

    def test_add_file_handler(self):
        self.logger = return_logger('test_log', None)
        assert len(self.logger.handlers) == 2
        add_file_handler(self.logger, self.testfile)
        assert len(self.logger.handlers) == 3
        add_file_handler(self.logger, self.testfile, remove_existing=False)
        assert len(self.logger.handlers) == 4
        add_file_handler(self.logger, self.testfile, remove_existing=True)
        assert len(self.logger.handlers) == 3

    def test_logfile_matches(self):
        self.logger = return_logger('test_log', None)
        add_file_handler(self.logger, self.testfile)
        assert logfile_matches(self.logger, self.testfile)
        assert not logfile_matches(self.logger, self.testfile + 'test')

    def test_logger_has_file_handler(self):
        self.logger = return_logger('test_log', None)
        assert not logger_has_file_handler(self.logger)
        add_file_handler(self.logger, self.testfile)
        assert logger_has_file_handler(self.logger)
