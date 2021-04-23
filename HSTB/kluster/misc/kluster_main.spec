# -*- mode: python ; coding: utf-8 -*-

# pip install -v --no-cache-dir numcodecs to avoid blosc import errors


import sys
import glob
from PyInstaller.compat import is_win, is_darwin, is_linux
from PyInstaller.utils.hooks import collect_submodules
import vispy.glsl
import vispy.io
import distributed

block_cipher = None

pydro_path = r"C:\Pydro21_Dev"
pydro_env = os.path.join(pydro_path, 'envs', 'Pydro38')
pydro_sp = os.path.join(pydro_path, 'NOAA', 'site-packages', 'Python38')
assert os.path.exists(pydro_env)
assert os.path.exists(pydro_sp)

data_files = [
	(os.path.join(pydro_env, 'Library', 'bin', 'acyclic.exe'), os.path.join("Library", "bin")),  # random file just to make the bin directory
	(os.path.join(pydro_sp, 'git_repos', 'hstb_kluster', 'HSTB', 'kluster', 'gui', 'vessel_stl_files'), os.path.join("HSTB", "kluster", "gui", "vessel_stl_files")),
	(os.path.join(pydro_sp, 'git_repos', 'hstb_kluster', 'HSTB', 'kluster', 'images'), os.path.join("HSTB", "kluster", "images")),
	(os.path.join(pydro_sp, 'git_repos', 'hstb_kluster', 'HSTB', 'kluster', 'background'), os.path.join("HSTB", "kluster", "background")),
	(os.path.join(pydro_env, 'Lib', 'site-packages', 'vispy', 'util', 'fonts'), os.path.join("vispy", "util", "fonts")),
	(os.path.join(pydro_env, 'Library', 'bin', 'libiomp5md.dll'), "."),
	(os.path.join(pydro_env, 'Library', 'bin', 'mkl_intel_thread.dll'), "."),
    (os.path.dirname(vispy.glsl.__file__), os.path.join("vispy", "glsl")),
    (os.path.join(os.path.dirname(vispy.io.__file__), "_data"), os.path.join("vispy", "io", "_data")),
    (os.path.join(os.path.dirname(distributed.__file__)), "distributed"),
    (os.path.join(pydro_env, 'Lib', 'site-packages', 'numcodecs', 'blosc.cp38-win_amd64.pyd'), "numcodecs"),
	(os.path.join(pydro_env, 'Lib', 'site-packages', 'numcodecs', 'compat_ext.cp38-win_amd64.pyd'), "numcodecs")
]

qgis_dlls = glob.glob(os.path.join(pydro_env, 'Library', 'plugins', '*.dll'))
qgis_data_files = [(fil, "qgis_plugins") for fil in qgis_dlls]
qgis_data_files += [(os.path.join(pydro_env, 'Library', 'bin', 'exiv2.dll'), ".")]
qgis_data_files += [(os.path.join(pydro_env, 'Library', 'bin', 'expat.dll'), ".")]

data_files += qgis_data_files
for fil in data_files:
    assert os.path.exists(fil[0])


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

a = Analysis(["kluster_main.py"],
             pathex=["C:\\Pydro21_Dev\\NOAA\\site-packages\\Python38\\git_repos\\hstb_kluster\\HSTB\\kluster\\gui",
			         "C:\\Pydro21_Dev\\envs\\Pydro38\\Library\\bin",
					 "C:\\Pydro21_Dev\\envs\\Pydro38\\Lib\\site-packages\\shiboken2"],
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
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name="kluster_main")
