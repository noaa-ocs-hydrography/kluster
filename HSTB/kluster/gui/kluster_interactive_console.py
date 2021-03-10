# https://bitbucket.org/japczynski/pythonconsole/src/master/PythonConsole.py
# https://pyqtgraph.readthedocs.io/en/latest/widgets/consolewidget.html

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from pyqtgraph import console
import sys


class KlusterConsole(console.ConsoleWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setWindowTitle('Kluster Console')
        self.runCmd('import os, sys')
        self.runCmd('from HSTB.kluster.fqpr_convenience import *')
        self.runCmd("print('Python %s on %s' % (sys.version, sys.platform))")


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    test_window = KlusterConsole()
    test_window.show()
    sys.exit(app.exec_())
