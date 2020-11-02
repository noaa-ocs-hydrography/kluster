import os
from HSTB import __file__


def is_pydro():
    """
    Quick check to see if kluster is being run from Pydro.  If so, the paths we build in this module will be valid.

    Returns
    -------
    True if this environment is within Pydroxl
    """

    try:
        retrieve_scripts_folder()
        return True
    except RuntimeError:
        return False


def retrieve_noaa_folder_path():
    """
    Helper function to retrieve the path to the NOAA folder in PydroXL

    Returns
    -------
    str
        folder path to the NOAA folder as string
    """

    folder_path = os.path.realpath(os.path.dirname(__file__))
    if not os.path.exists(folder_path):
        raise RuntimeError("the folder does not exist: %s" % folder_path)
    return folder_path


def retrieve_install_prefix():
    """
    Helper function to retrieve the install prefix path for PydroXL

    Returns
    -------
    str
        folder path to the base Pydro folder
    """

    noaa_folder = retrieve_noaa_folder_path()
    folder_path = os.path.realpath(os.path.join(noaa_folder, os.pardir, os.pardir, os.pardir, os.pardir))
    if not os.path.exists(folder_path):
        raise RuntimeError("the folder does not exist: %s" % folder_path)
    return folder_path


def retrieve_scripts_folder():
    """
    Helper function to retrieve the path to the "Scripts" folder in PydroXL

    Returns
    -------
    str
        folder path to the Pydro scripts folder
    """

    install_prefix = retrieve_install_prefix()
    folder_path = os.path.realpath(os.path.join(install_prefix, "Scripts"))
    if not os.path.exists(folder_path):
        raise RuntimeError("the folder does not exist: %s" % folder_path)
    return folder_path


def retrieve_activate_batch():
    """
    Helper function to retrieve the path to the "activate.bat" batch file in PydroXL

    Returns
    -------
    str
        file path to the activate batch file
    """

    scripts_prefix = retrieve_scripts_folder()
    file_path = os.path.realpath(os.path.join(scripts_prefix, "activate.bat"))
    if not os.path.exists(file_path):
        raise RuntimeError("the file does not exist: %s" % file_path)
    return file_path
