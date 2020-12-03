# -*- mode: python ; coding: utf-8 -*-

# had to downgrade to PySide2 5.14.1 to avoid shiboken import errors https://bugreports.qt.io/browse/PYSIDE-1257
#   supposed to be fixed in 5.15.2 but apparently people still see it not working     

# pip install -v --no-cache-dir numcodecs to avoid blosc import errors


import sys
from PyInstaller.compat import is_win, is_darwin, is_linux
from PyInstaller.utils.hooks import collect_submodules
import vispy.glsl
import vispy.io
import distributed

block_cipher = None

data_files = [
    (os.path.dirname(vispy.glsl.__file__), os.path.join("vispy", "glsl")),
    (os.path.join(os.path.dirname(vispy.io.__file__), "_data"), os.path.join("vispy", "io", "_data")),
    (os.path.join(os.path.dirname(distributed.__file__)), 'distributed'),
    ("C:\\PydroXL_19\\envs\\kluster_test\\Lib\\site-packages\\numcodecs\\blosc.cp38-win_amd64.pyd", "numcodecs"),
]

hidden_imports = [
    "vispy.ext._bundled.six",
    "vispy.app.backends._pyqt5",
    "sqlalchemy.ext.baked"
    
]

if is_win:
    hidden_imports += collect_submodules("encodings")
	
a = Analysis(['kluster_main.py'],
             pathex=['C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui', 
			         'C:\\PydroXL_19\\envs\\kluster_test\\Lib\\site-packages\\shiboken2',
					 'C:\\PydroXL_19\\pkgs\\tbb-2020.1-he980bc4_0\\Library\\bin'],
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
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='kluster_main',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
