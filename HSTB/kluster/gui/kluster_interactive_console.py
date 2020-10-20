# https://bitbucket.org/japczynski/pythonconsole/src/master/PythonConsole.py
# https://pyqtgraph.readthedocs.io/en/latest/widgets/consolewidget.html

from PySide2 import QtWidgets
import pyqtgraph as pg
from pyqtgraph import console
import numpy as np
import xarray as xr
import sys


class KlusterConsole(console.ConsoleWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setWindowTitle('Kluster Console')
        self.runCmd('import os, sys')
        self.runCmd('from HSTB.kluster.fqpr_convenience import *')
        self.runCmd("print('Python %s on %s' % (sys.version, sys.platform))")


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = KlusterConsole()
    test_window.show()
    sys.exit(app.exec_())
