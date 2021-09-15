import logging
import sys
import os
from datetime import datetime

loglevel = logging.INFO
log_counter = 0


class StdErrFilter(logging.Filter):
    """
    filter out messages that are not CRITICAL or ERROR or WARNING
    """
    def filter(self, rec):
        return rec.levelno in (logging.CRITICAL, logging.ERROR, logging.WARNING)


class StdOutFilter(logging.Filter):
    """
    filter out messages that are not DEBUG or INFO
    """
    def filter(self, rec):
        return rec.levelno in (logging.DEBUG, logging.INFO)


def return_log_name():
    return 'logfile_{}.txt'.format(int(datetime.utcnow().timestamp()))


def return_logger(name, logfile):
    """
    Built to support logging within the kluster system.  Each instance of each class gets a separate logger, identified
    by the name attribute passed in here.  This is important because each instance of the kluster processing that is
    running should be driven to that instance's log file.  So we need them all segregated.

    If logfile is included, the file handler is added to the log so that the output is also driven to file.

    I disable the root logger by clearing out it's handlers because it always gets a default stderr log handler that
    ends up duplicating messages.  Since I want the stderr messages formatted nicely, I want to setup that handler \
    myself.

    Parameters
    ----------
    name: str, identifier used to name the logger instance.  Currently something like 'fqpr_generation_3252' where the
          integer appended is the unique id of the logger instance
    logfile: str, path to the log file where you want the output driven to

    Returns
    -------
    logger: logging.Logger instance for the provided name/logfile

    """
    global log_counter
    fmat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger(name + str('_') + str(log_counter))
    log_counter += 1
    logger.setLevel(loglevel)

    consolelogger = logging.StreamHandler(sys.stdout)
    consolelogger.setLevel(loglevel)
    #consolelogger.setFormatter(logging.Formatter(fmat))
    consolelogger.addFilter(StdOutFilter())

    errorlogger = logging.StreamHandler(sys.stderr)
    errorlogger.setLevel(logging.WARNING)
    #errorlogger.setFormatter(logging.Formatter(fmat))
    errorlogger.addFilter(StdErrFilter())

    logger.addHandler(consolelogger)
    logger.addHandler(errorlogger)

    if logfile is not None:
        filelogger = logging.FileHandler(logfile)
        filelogger.setLevel(loglevel)
        filelogger.setFormatter(logging.Formatter(fmat))
        logger.addHandler(filelogger)

    # eliminate the root logger handlers, it will have a default stderr pointing handler that ends up duplicating all the logs to console
    logging.getLogger().handlers = []

    return logger
