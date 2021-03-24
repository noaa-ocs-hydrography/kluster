from HSTB.kluster.logging_conf import *


def test_return_logger():
    nme = 'test_log'
    logfile = None

    logger = return_logger(nme, logfile)

    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert logger.name[0:9] == 'test_log_'
    logger = None
