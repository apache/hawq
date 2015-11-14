#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
'''
Greenplum logging facilities.

This Module contains some helper functions for setting up the 
python builtin logging module.  Tools and libraries are expected
to centralize configuration of logging through these functions.

Typical usage:

  from gppylib import gplog

  logger = gplog.setup_tool_logging(EXECNAME, hostname, username, logdir)

  if options.verbose:
    gplog.enable_verbose_logging()

  if options.quiet:
    gplog.quiet_stdout_logging()

  logger.info("Start myTool")
  ...

'''
import datetime
import logging
import os
import sys

#------------------------------- Public Interface --------------------------------
def get_default_logger():
    """
    Return the singleton default logger.

    If a logger has not yet been established it creates one that:
      - Logs output to stdout
      - Does not setup file logging.

    Typicial usage would be to call one of the setup_*_logging() functions
    at the beginning of a script in order to establish the exact type of
    logging desired, afterwhich later calls to get_default_logger() can be
    used to return a reference to the logger.
    """
    global _LOGGER, _SOUT_HANDLER
    if _LOGGER is None:
        _LOGGER = logging.getLogger('default')
        f = _get_default_formatter()
        _SOUT_HANDLER = EncodingStreamHandler(sys.stdout)
        _SOUT_HANDLER.setFormatter(f)
        _LOGGER.addHandler(_SOUT_HANDLER)
        _LOGGER.setLevel(logging.INFO)
    return _LOGGER

def get_unittest_logger():
    """
    Returns a singleton logger for use by gppylib unittests:
      - Does not setup stdout logging
      - Logs output to a file named "unittest.log" in the current directory.

    Much like get_default_logger, except that the default logger it creates
    (if one does not already exist) is different.

    Note: perhaps the interface for this should be cleaned up.  It would be
    more consistent to gave a single get_default_logger() method and supply
    a setup_unittest_logging() function.
    """
    global _LOGGER, _SOUT_HANDLER
    if _LOGGER is None:
        _LOGGER = logging.getLogger('default')
        filename="unittest.log"
        _set_file_logging(filename)
    return _LOGGER


def setup_helper_tool_logging(appName,hostname,userName):
    """ 
    Returns a singleton logger for use by helper tools:
      - Logs output to stdout
      - Does not log output to a file
    """
    logger = get_default_logger()
    logger.name="%s:%s" % (hostname,userName)
    return logger

def setup_hawq_tool_logging(appName,hostname,userName,logdir=None,nonuser=False):
    """
    Returns a singleton logger for standard Greenplum tools:
      - Logs output to stdout
      - Logs output to a file, typically in ~/hawqAdminLogs
    """
    global _DEFAULT_FORMATTER
    global _APP_NAME_FOR_DEFAULT_FORMAT

    loggerName ="%s:%s" % (hostname,userName)
    if nonuser:
        appName=appName + "_" + loggerName
    _APP_NAME_FOR_DEFAULT_FORMAT = appName

    log_filename = _enable_hawqadmin_logging(appName,logdir)

    #
    # now reset the default formatter (someone may have called get_default_logger before calling setup_tool_logging)
    #
    logger = get_default_logger()
    logger.name = loggerName
    _DEFAULT_FORMATTER = None
    f = _get_default_formatter()
    _SOUT_HANDLER.setFormatter(f)
    _FILE_HANDLER.setFormatter(f)

    return logger, log_filename

def setup_tool_logging(appName,hostname,userName,logdir=None,nonuser=False):
    """
    Returns a singleton logger for standard Greenplum tools:
      - Logs output to stdout
      - Logs output to a file, typically in ~/hawqAdminLogs
    """
    global _DEFAULT_FORMATTER
    global _APP_NAME_FOR_DEFAULT_FORMAT

    loggerName ="%s:%s" % (hostname,userName)
    if nonuser:
        appName=appName + "_" + loggerName
    _APP_NAME_FOR_DEFAULT_FORMAT = appName

    _enable_gpadmin_logging(appName,logdir)

    #
    # now reset the default formatter (someone may have called get_default_logger before calling setup_tool_logging)
    #
    logger = get_default_logger()
    logger.name = loggerName
    _DEFAULT_FORMATTER = None
    f = _get_default_formatter()
    _SOUT_HANDLER.setFormatter(f)
    _FILE_HANDLER.setFormatter(f)

    return logger

def enable_verbose_logging():
    """
    Increases the log level to be verbose.
     - Applies to all logging handlers (stdout/file).
    """
    _LOGGER.setLevel(logging.DEBUG)


def quiet_stdout_logging():
    """ 
    Reduce log level for stdout logging 
    """
    global _SOUT_HANDLER
    _SOUT_HANDLER.setLevel(logging.WARN)

def very_quiet_stdout_logging():
    """ 
    Reduce log level to critical for stdout logging 
    """
    global _SOUT_HANDLER
    _SOUT_HANDLER.setLevel(logging.CRITICAL)

def logging_is_verbose():
    """
    Returns true if the logging level has been set to verbose
    """
    return _LOGGER.getEffectiveLevel() == logging.DEBUG    
    
def logging_is_quiet():
    """
    Returns true if the logging level has been set to quiet.
    """
    # Todo: Currently this checks the default LOGGER, the 
    # quiet_stdout_logging() function only sets it on the stdout
    # logging handler.   So typical usage will never return true.
    return _LOGGER.getEffectiveLevel() == logging.WARN

def get_logfile():
    """
    Returns the name of the file we are logging to, if any.
    """
    global _FILENAME
    return _FILENAME

def log_literal(logger, lvl, msg):
    """
    Logs a message to a specified logger bypassing the normal formatter
    and writing the message exactly as passed.

    The intended purpose of this is for logging messages returned from
    remote backends that have already been formatted.
    """
    
    # We assume the logger is using the two global handlers
    global _SOUT_HANDLER
    global _FILE_HANDLER

    # Switch to the literal formatter
    #
    # Note: the logger may or may not actually make use of both formatters,
    # but it is safe to always set both even if only one of them is used.
    f = _get_literal_formatter()
    _SOUT_HANDLER.setFormatter(f)
    _FILE_HANDLER.setFormatter(f)

    # Log the message
    logger.log(lvl, msg)
    
    # Restore default formatter
    f = _get_default_formatter()
    _SOUT_HANDLER.setFormatter(f)
    _FILE_HANDLER.setFormatter(f)
    
    return
    
def get_logger_if_verbose():
    if logging_is_verbose():
        return get_default_logger()
    return None

    
#------------------------------- Private --------------------------------    

#evil global
_LOGGER=None
_FILENAME=None
_DEFAULT_FORMATTER=None
_LITERAL_FORMATTER=None
_SOUT_HANDLER=None
_FILE_HANDLER=None
_APP_NAME_FOR_DEFAULT_FORMAT=os.path.split(sys.argv[0])[-1]

def _set_file_logging(filename): 
    """
    Establishes a file output HANDLER for the default formater.
    
    NOTE: internal use only
    """
    global _LOGGER, _SOUT_HANDLER, _FILENAME, _FILE_HANDLER
    _FILENAME=filename   
    _FILE_HANDLER = EncodingFileHandler( filename, 'a')
    _FILE_HANDLER.setFormatter(_get_default_formatter())
    _LOGGER.addHandler(_FILE_HANDLER)


def _get_default_formatter():
    """
    Returns the default formatter, constructing it if needed.
    The default formatter formats things using Greenplum standard logging:
      <date>:<pid> <programname>:<hostname>:<username>:[LEVEL]:-message
    NOTE: internal use only
    """
    global _DEFAULT_FORMATTER
    global _APP_NAME_FOR_DEFAULT_FORMAT

    if _DEFAULT_FORMATTER == None:
        formatStr = "%(asctime)s:%(programname)s:%(name)s-[%(levelname)-s]:-%(message)s"
        appName = _APP_NAME_FOR_DEFAULT_FORMAT.replace("%", "") # to make sure we don't produce a format string
        formatStr = formatStr.replace("%(programname)s", "%06d %s" % (os.getpid(), appName))
        _DEFAULT_FORMATTER = logging.Formatter(formatStr,"%Y%m%d:%H:%M:%S")
    return _DEFAULT_FORMATTER

def _get_literal_formatter():
    """
    Returns the literal formatter, constructing it if needed.

    The literal formatter formats the input string exactly as it was received.
    It is only used by the log_literal() function.
    
    NOTE: internal use only
    """
    global _LITERAL_FORMATTER
    if _LITERAL_FORMATTER == None:
        _LITERAL_FORMATTER = logging.Formatter()
    return _LITERAL_FORMATTER
    
def _enable_hawqadmin_logging(name,logdir=None):
    """
    Sets up the file output handler for the default logger.
      - if logdir is not specified it uses ~/hawqAdminLogs
      - the file is constructed as appended with "<logdir>/<name>_<date>.log"

    NOTE: internal use only
    """
    global _FILE_HANDLER
        
    get_default_logger()
    now = datetime.date.today()
    
    if logdir is None:
        homeDir=os.path.expanduser("~")
        gpadmin_logs_dir=homeDir + "/hawqAdminLogs"
    else:
        gpadmin_logs_dir=logdir
    
    if not os.path.exists(gpadmin_logs_dir):
        # It looks like in ICM, the dir may exists.
        try:
            os.mkdir(gpadmin_logs_dir)
        except OSError, e:
            pass
    
    filename = "%s/%s_%s.log" % (gpadmin_logs_dir,name, now.strftime("%Y%m%d"))
    _set_file_logging(filename)
    return filename

def _enable_gpadmin_logging(name,logdir=None):
    """
    Sets up the file output handler for the default logger.
      - if logdir is not specified it uses ~/hawqAdminLogs
      - the file is constructed as appended with "<logdir>/<name>_<date>.log"

    NOTE: internal use only
    """
    global _FILE_HANDLER
        
    get_default_logger()
    now = datetime.date.today()
    
    if logdir is None:
        homeDir=os.path.expanduser("~")
        gpadmin_logs_dir=homeDir + "/hawqAdminLogs"
    else:
        gpadmin_logs_dir=logdir
    
    if not os.path.exists(gpadmin_logs_dir):
        # It looks like in ICM, the dir may exists.
        try:
            os.mkdir(gpadmin_logs_dir)
        except OSError, e:
            pass
    
    filename = "%s/%s_%s.log" % (gpadmin_logs_dir,name, now.strftime("%Y%m%d"))
    _set_file_logging(filename)


class EncodingFileHandler(logging.FileHandler):
    """This handler makes sure that the encoding of the message is utf-8 before
    passing it along to the FileHandler.  This will prevent encode/decode
    errors later on."""
    
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)
        
    def emit(self, record):
        if not isinstance(record.msg, str) and not isinstance(record.msg, unicode):
            record.msg = str(record.msg)
        if not isinstance(record.msg, unicode): 
            record.msg = unicode(record.msg, 'utf-8')
        logging.FileHandler.emit(self, record)
            
class EncodingStreamHandler(logging.StreamHandler):
    """This handler makes sure that the encoding of the message is utf-8 before
    passing it along to the StreamHandler.  This will prevent encode/decode
    errors later on."""
    
    def __init__(self, strm=None):
        logging.StreamHandler.__init__(self, strm)
        
    def emit(self, record):
        if not isinstance(record.msg, str) and not isinstance(record.msg, unicode):
            record.msg = str(record.msg)
        if not isinstance(record.msg, unicode): 
            record.msg = unicode(record.msg, 'utf-8')
        logging.StreamHandler.emit(self, record)

    
