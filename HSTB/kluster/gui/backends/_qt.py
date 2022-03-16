import sys, os


qgis_enabled = True
found_path = ''
try:
    import qgis  # this appears to work with 3.18, so the rest is not necessary
    found_path = os.path.dirname(qgis.__file__)
except (ImportError, ModuleNotFoundError):
    # qgis conda installs to the library\python directory not to site_packages
    # this is where qgis should be if you pip install kluster and kluster is in the site_packages directory
    klusterfolder_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    env_base_path = os.path.dirname(sys.executable)
    qgis_path = os.path.join(env_base_path, 'Library', 'python')
    qgis_path_pydro = ''
    if not os.path.exists(os.path.join(qgis_path, 'qgis')):
        # try based on where qgis should be if Kluster is run from the Pydro directory structure
        sitepackages, envname = os.path.split(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(klusterfolder_path)))))
        envs = os.path.join(os.path.dirname(os.path.dirname(sitepackages)), 'envs')
        if os.path.exists(envs):  # must be a pydro env
            if os.path.exists(os.path.join(envs, 'kluster_test')):  # hack added to test a new environment
                envfolder = 'kluster_test'
                qgis_path_pydro = os.path.join(envs, envfolder, 'Library', 'python')
                print(f'WARNING: Found a kluster_test environment, adding {qgis_path_pydro} to path')
            elif envname.lower() == 'python38':
                envfolder = 'Pydro38'
                qgis_path_pydro = os.path.join(envs, envfolder, 'Library', 'python')
            else:
                # raise EnvironmentError('_qt: unexpected environment name {}, only supports Python38 for now'.format(envname))
                qgis_enabled = False
        else:
            # err = r'Unable to find the Library\python directory where qgis should exist, expected it here'
            # err += ' {} or in the Pydro envs folder here {}'.format(qgis_path, envs)
            # raise EnvironmentError(err)
            qgis_enabled = False

    for pth in [qgis_path, qgis_path_pydro]:
        if not os.path.exists(os.path.join(pth, 'qgis')):
            # raise EnvironmentError(r'Unable to find the "qgis" folder in the generated qgis_path: {}'.format(qgis_path))
            qgis_enabled = False
        else:
            sys.path.append(pth)
            found_path = pth
            qgis_enabled = True
            break

if qgis_enabled:
    try:
        # only allow qgis.pyqt (pyqt5) for now to support qgis in 2dview
        from qgis.PyQt import QtGui, QtWidgets, QtCore, QtXml
        from qgis.PyQt.QtCore import pyqtSignal as Signal, pyqtSlot as Slot
        from qgis import core as qgis_core
        from qgis import gui as qgis_gui
        # from qgis import processing as qgis_processing  # need to figure out why this causes an 'application path not initialized issue'
    except ImportError:
        try:
            from qgis.core import Qgis as _tempqgis
            vers = _tempqgis.QGIS_VERSION
            del _tempqgis
        except:
            vers = 'Unknown'
        raise EnvironmentError('Unable to load qgis.core, used QGIS found {}, version={}'.format(found_path, vers))

    os.environ['PYDRO_GUI_FORCE_PYQT'] = 'True'  # for registryhelpers
    backend = 'PyQt5'
    qgis_enabled = True
    incompatible_modules = [ky for ky in sys.modules if ky.find('PySide') > -1]
    for ky in incompatible_modules:
        del sys.modules[ky]
else:
    qgis_core = None
    qgis_gui = None
    qgis_processing = None
    if 'PySide2' in sys.modules:
        # PySide2
        from PySide2 import QtGui, QtWidgets, QtCore, QtXml
        from PySide2.QtCore import Signal, Slot

        backend = 'PySide2'
        qgis_enabled = False
        incompatible_modules = [ky for ky in sys.modules if ky.find('PyQt') > -1]
        for ky in incompatible_modules:
            del sys.modules[ky]
    else:
        # PyQt5
        from PyQt5 import QtGui, QtWidgets, QtCore, QtXml
        from PyQt5.QtCore import pyqtSignal as Signal, pyqtSlot as Slot

        backend = 'PyQt5'
        qgis_enabled = False
        incompatible_modules = [ky for ky in sys.modules if ky.find('PySide') > -1]
        for ky in incompatible_modules:
            del sys.modules[ky]

