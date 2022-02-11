# -*- mode: python ; coding: utf-8 -*-

# pip install -v --no-cache-dir numcodecs to avoid blosc import errors


import sys, os
import glob
from PyInstaller.compat import is_win, is_darwin, is_linux
from PyInstaller.utils.hooks import collect_submodules
import distributed
import vispy.glsl
import vispy.io

block_cipher = None

specfile_path = os.path.join(SPECPATH, 'kluster_main.spec')
pydro_python_env_name = 'Pydro38'


# this should work with new environments in conda via github actions AND with the pydro environment
klusterfolder_path = os.path.dirname(os.path.dirname(specfile_path))  # 'kluster'
assert os.path.split(klusterfolder_path)[1] == 'kluster'
try:  # this is for the Pydro directory structure
    base_conda_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(klusterfolder_path)))))))
    env_base_path = os.path.join(base_conda_path, 'envs', pydro_python_env_name)
    assert os.path.exists(env_base_path)
except AssertionError:  # this is for a fresh conda install of kluster
    base_conda_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(klusterfolder_path))))))
    env_base_path = os.path.join(base_conda_path, 'envs', 'kluster_test')
    assert os.path.exists(env_base_path)
env_folder = os.path.join(base_conda_path, 'envs')
assert os.path.exists(env_folder)

data_files = [
	(os.path.join(env_base_path, 'Library', 'bin', 'designer.exe'), os.path.join("Library", "bin")),  # random file just to make the bin directory
	(os.path.join(klusterfolder_path, 'gui', 'vessel_stl_files'), os.path.join("HSTB", "kluster", "gui", "vessel_stl_files")),
	(os.path.join(klusterfolder_path, 'images'), os.path.join("HSTB", "kluster", "images")),
	(os.path.join(klusterfolder_path, 'background'), os.path.join("HSTB", "kluster", "background")),
	(os.path.join(klusterfolder_path, 'docbuild'), os.path.join("HSTB", "kluster", "docbuild")),
	(os.path.join(env_base_path, 'Lib', 'site-packages', 'vispy', 'util', 'fonts'), os.path.join("vispy", "util", "fonts")),
	(os.path.join(env_base_path, 'Library', 'bin', 'libiomp5md.dll'), "."),
	(os.path.join(env_base_path, 'Library', 'bin', 'mkl_intel_thread.1.dll'), "."),
    (os.path.dirname(vispy.glsl.__file__), os.path.join("vispy", "glsl")),
    (os.path.join(os.path.dirname(vispy.io.__file__), "_data"), os.path.join("vispy", "io", "_data")),
    (os.path.join(os.path.dirname(distributed.__file__)), "distributed"),
    (os.path.join(env_base_path, 'Lib', 'site-packages', 'numcodecs', 'blosc.cp38-win_amd64.pyd'), "numcodecs"),
	(os.path.join(env_base_path, 'Lib', 'site-packages', 'numcodecs', 'compat_ext.cp38-win_amd64.pyd'), "numcodecs"),
	(os.path.join(env_base_path, 'Lib', 'site-packages', 'osgeo', '_gdal.cp38-win_amd64.pyd'), "osgeo"),
	(os.path.join(env_base_path, 'Lib', 'site-packages', 'pyqtgraph', 'console'), os.path.join('pyqtgraph', 'console'))
]

qgis_dlls = glob.glob(os.path.join(env_base_path, 'Library', 'plugins', '*.dll'))
qgis_data_files = [(fil, "qgis_plugins") for fil in qgis_dlls]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'bin', 'exiv2.dll'), ".")]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'bin', 'expat.dll'), ".")]

# these appear to be necessary for the WMS layers to work, removing them breaks this functionality in kluster
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'resources', 'qgis.db'), "resources")]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'resources', 'qgis_global_settings.ini'), "resources")]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'resources', 'spatialite.db'), "resources")]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'resources', 'srs.db'), "resources")]
qgis_data_files += [(os.path.join(env_base_path, 'Library', 'resources', 'symbology-style.xml'), "resources")]

data_files += qgis_data_files
for fil in data_files:
    try:
        assert os.path.exists(fil[0])
    except:
        print('Unable to find file: {}'.format(fil[0]))
        sys.exit()

hidden_imports = [
	"PyQt5.QtPrintSupport", 
	"PyQt5.QtSql", 
	"PyQt5.QtXml",
    "vispy.ext._bundled.six",
    "vispy.app.backends._pyqt5",
    "sqlalchemy.ext.baked"
    
]

if is_win:
    hidden_imports += collect_submodules("encodings")

a = Analysis([os.path.join(klusterfolder_path, 'gui', 'kluster_main.py')],
             pathex=[os.path.join(klusterfolder_path, 'gui'),
			         os.path.join(env_base_path, 'Library', 'bin'),
					 os.path.join(env_base_path, 'Lib', 'site-packages', 'shiboken2')],
             binaries=[],
             datas=data_files,
             hiddenimports=hidden_imports,
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name="kluster_main",
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True,
          icon=os.path.join(klusterfolder_path, 'images', 'kluster_img.ico'))
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name="kluster_main")
