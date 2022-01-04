import os
from datetime import datetime, timezone

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from matplotlib.backends.backend_qt5agg import (FigureCanvasQTAgg, NavigationToolbar2QT as NavigationToolbar)
import matplotlib.pyplot as plt


class ManageSurfaceDialog(QtWidgets.QWidget):
    """
    Dialog contains a summary of the surface data and some options for altering the data contained within.
    """
    update_surface = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setMinimumWidth(500)
        self.setMinimumHeight(400)

        self.setWindowTitle('Manage Surface')
        layout = QtWidgets.QVBoxLayout()

        self.basicdata = QtWidgets.QTextEdit()
        self.basicdata.setReadOnly(True)
        self.basicdata.setText('')
        layout.addWidget(self.basicdata)

        self.managelabel = QtWidgets.QLabel('Manage: ')
        layout.addWidget(self.managelabel)

        self.setLayout(layout)
        self.surf = None

    def populate(self, surf):
        self.surf = surf
        self.managelabel.setText('Manage: {}'.format(os.path.split(surf.output_folder)[1]))
        self.basicdata.setText(surf.__repr__())


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManageSurfaceDialog()
    from HSTB.kluster.fqpr_convenience import reload_surface
    dlog.populate(reload_surface(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710_20211216_225746"))
    dlog.show()
    app.exec_()
