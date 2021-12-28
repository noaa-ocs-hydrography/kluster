import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal


class ManageDataDialog(QtWidgets.QDialog):
    """
    Dialog contains a summary of the Fqpr data and some options for altering the data contained within.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    refresh_fqpr = Signal(object, object)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setMinimumWidth(500)
        self.setMinimumHeight(400)

        self.setWindowTitle('Manage Data')
        layout = QtWidgets.QVBoxLayout()

        self.basicdata = QtWidgets.QTextEdit()
        self.basicdata.setReadOnly(True)
        self.basicdata.setText('')
        layout.addWidget(self.basicdata)

        self.managelabel = QtWidgets.QLabel('Manage: ')
        layout.addWidget(self.managelabel)

        buttontwo = QtWidgets.QHBoxLayout()
        self.sbetbutton = QtWidgets.QPushButton(' Remove SBET ')
        buttontwo.addWidget(self.sbetbutton)
        self.svpbutton = QtWidgets.QPushButton(' Manage SVP ')
        buttontwo.addWidget(self.svpbutton)
        buttontwo.addStretch()
        layout.addLayout(buttontwo)

        self.sbetbutton.clicked.connect(self.remove_sbet)

        self.setLayout(layout)
        self.fqpr = None

    def populate(self, fqpr):
        self.fqpr = fqpr
        self.managelabel.setText('Manage: {}'.format(os.path.split(fqpr.output_folder)[1]))
        self.basicdata.setText(fqpr.__repr__())

    def remove_sbet(self, e):
        if self.fqpr is not None:
            if 'nav_files' in self.fqpr.multibeam.raw_ping[0].attrs:
                newstate = RemoveSBETDialog(list(self.fqpr.multibeam.raw_ping[0].nav_files.keys())).run()
                if newstate:
                    self.fqpr.remove_post_processed_navigation()
                    self.refresh_fqpr.emit(self.fqpr, self)
            else:
                print('No SBET files found')
        else:
            print('No data found')


class RemoveSBETDialog(QtWidgets.QMessageBox):
    def __init__(self, navfiles: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Remove SBET')
        self.setText('Remove all SBETs and associated error data from this converted instance?\n\nCurrently includes:\n{}'.format('\n'.join(navfiles)) +
                     '\n\nWARNING: Removing SBETs will generate a new georeferencing action to reprocess with multibeam navigation.')
        self.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        self.setDefaultButton(QtWidgets.QMessageBox.Yes)

    def run(self):
        result = self.exec_()
        if result == QtWidgets.QMessageBox.No:
            return False
        else:
            return True


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManageDataDialog()
    dlog.show()
    if dlog.exec_():
        pass
