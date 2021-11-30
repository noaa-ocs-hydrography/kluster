import logging
import unittest

from HSTB.kluster.logging_conf import return_logger


class TestLoggingConf(unittest.TestCase):

    def test_return_logger(self):
        logger = return_logger('test_log', None)

        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.INFO
        assert logger.name[0:9] == 'test_log_'
