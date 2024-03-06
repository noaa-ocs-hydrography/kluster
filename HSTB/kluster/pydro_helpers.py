import os
from HSTB.resources import path_to_NOAA, path_to_root_env, path_to_conda, path_to_supplementals


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


def retrieve_vdatum_folder_path():
    """
    Helper function to retrieve the path to the Vdatum folder in PydroXL, if it exists

    Returns
    -------
    str
        folder path to the supplementals/vdatum folder as string
    """

    folder_path = path_to_supplementals('VDatum')
    if not os.path.exists(folder_path):
        return None
    return folder_path


def retrieve_noaa_folder_path():
    """
    Helper function to retrieve the path to the NOAA folder in PydroXL

    Returns
    -------
    str
        folder path to the NOAA folder as string
    """

    folder_path = path_to_NOAA()
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

    folder_path = path_to_root_env()
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

    folder_path = path_to_conda()
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
