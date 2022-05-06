import logging
import sys
import os
from datetime import datetime

loglevel = logging.INFO
log_counter = 0


class LoggerClass:
    """
    Basic class for logging.  Include a logging.logger instance to use that, or set silent to true to disable print
    messages entirely.  Use of Logger will trump silent.
    """

    def __init__(self, silent=False, logger=None):
        self.silent = silent
        self.logger = logger

    def print_msg(self, msg: str, loglvl: int = logging.INFO):
        """
        Either print to console, print using logger, or do not print at all, if self.silent = True

        Parameters
        ----------
        msg
            message contents as string
        loglvl
            one of the logging enum values, logging.info or logging.warning as example
        """

        if self.logger is not None:
            if not isinstance(loglvl, int):
                raise ValueError('Log level must be an int (see logging enum), found {}'.format(loglvl))
            self.logger.log(loglvl, msg)
        elif self.silent:
            pass
        else:
            print(msg)


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


def return_log_name(timestamped: bool = False):
    """
    Return the log file name that we use throughout kluster.  Includes the utctimstamp in seconds as a unique id

    Parameters
    ----------
    timestamped
        if True, returns a timestamped log name

    Returns
    -------
    str
        log file name
    """

    if not timestamped:
        return 'logfile.txt'
    else:
        return 'logfile_{}.txt'.format(int(datetime.utcnow().timestamp()))


def return_logger(name, logfile: str = None):
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
    fmat = '%(asctime)s - %(levelname)s - %(message)s'
    logger = logging.getLogger(name + str('_') + str(log_counter))
    log_counter += 1
    logger.setLevel(loglevel)

    consolelogger = logging.StreamHandler(sys.stdout)
    consolelogger.setLevel(loglevel)
    consolelogger.setFormatter(logging.Formatter(fmat))
    consolelogger.addFilter(StdOutFilter())

    errorlogger = logging.StreamHandler(sys.stderr)
    errorlogger.setLevel(logging.WARNING)
    errorlogger.setFormatter(logging.Formatter(fmat))
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


def logger_remove_file_handlers(logger: logging.Logger):
    """
    Remove all existing file handlers from the logger instance

    Parameters
    ----------
    logger
        logger instance

    Returns
    -------
    logging.Logger
    """

    removethese = []
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            removethese.append(handler)
    for handler in removethese:
        logger.removeHandler(handler)
    return logger


def add_file_handler(logger: logging.Logger, logfile: str, remove_existing: bool = True):
    """
    Take an existing logger and add a new file handler to it, to save the log output to file.  If you remove_existing,
    will remove all existing file handlers from the logger.

    Parameters
    ----------
    logger
        logger instance
    logfile
        file path to where you want to save the log ouput
    remove_existing
        if True, removes all existing file handlers from the log

    Returns
    -------
    logging.Logger
    """

    if remove_existing:
        logger_remove_file_handlers(logger)
    filelogger = logging.FileHandler(logfile)
    filelogger.setLevel(loglevel)
    fmat = '%(asctime)s - %(levelname)s - %(message)s'
    filelogger.setFormatter(logging.Formatter(fmat))
    logger.addHandler(filelogger)
    return logger


def logfile_matches(logger: logging.Logger, logfile: str):
    """
    Check if the provided file path matches an existing file handler destination

    Parameters
    ----------
    logger
        logger instance
    logfile
        file path to where you want to save the log ouput

    Returns
    -------
    bool
        True if the logfile matches a baseFilename in an existing file handler
    """
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if os.path.normpath(handler.baseFilename) == os.path.normpath(logfile):
                return True
    return False


def logger_has_file_handler(logger: logging.Logger):
    """
    Check if this logger contains a file handler

    Parameters
    ----------
    logger
        logger instance

    Returns
    -------
    bool
        True if it contains a file handler
    """
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            return True
    return False
