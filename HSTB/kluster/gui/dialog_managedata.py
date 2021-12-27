import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal


class ManageDataDialog(QtWidgets.QDialog):
    """
    Dialog contains a summary of the Fqpr data and some options for altering the data contained within.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
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

        buttonone = QtWidgets.QHBoxLayout()
        self.navbutton = QtWidgets.QPushButton(" Set Navigation Source ")
        buttonone.addWidget(self.navbutton)
        buttonone.addStretch()
        layout.addLayout(buttonone)

        buttontwo = QtWidgets.QHBoxLayout()
        self.sbetbutton = QtWidgets.QPushButton(' Manage SBET ')
        buttontwo.addWidget(self.sbetbutton)
        self.svpbutton = QtWidgets.QPushButton(' Manage SVP ')
        buttontwo.addWidget(self.svpbutton)
        buttontwo.addStretch()
        layout.addLayout(buttontwo)

        self.setLayout(layout)
        self.fqpr = None

    def populate(self, fqpr):
        self.fqpr = fqpr
        self.managelabel.setText('Manage: {}'.format(os.path.split(fqpr.output_folder)[1]))
        self.basicdata.setText(fqpr.__repr__())


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManageDataDialog()
    dlog.show()
    if dlog.exec_():
        pass
