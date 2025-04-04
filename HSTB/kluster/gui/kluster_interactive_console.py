# https://bitbucket.org/japczynski/pythonconsole/src/master/PythonConsole.py
# https://pyqtgraph.readthedocs.io/en/latest/widgets/consolewidget.html

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from pyqtgraph import console
import sys
import os
import HSTB.kluster.fqpr_convenience as fqpr_convenience


class KlusterConsole(console.ConsoleWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setWindowTitle('Kluster Console')
        self.output.insertPlainText('Python Console \n')
        self.output.insertPlainText('Python %s on %s \n' % (sys.version, sys.platform))
        self.output.insertPlainText('import HSTB.kluster.fqpr_convience as fqpr_convenience \n')
        namespace = {'fqpr_convenience': fqpr_convenience}
        self.localNamespace = namespace
        # self.runCmd('import os, sys')
        # self.runCmd('from HSTB.kluster.fqpr_convenience import *')
        # self.runCmd("print('Python %s on %s' % (sys.version, sys.platform))")


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    test_window = KlusterConsole()
    test_window.show()
    sys.exit(app.exec_())
