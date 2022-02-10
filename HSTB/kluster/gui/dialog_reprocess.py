import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster import kluster_variables


class ReprocessDialog(QtWidgets.QDialog):
    """
    Dialog for managing the sound velocity profiles currently within this kluster converted data instance
    """
    remove_cast_sig = Signal(str)

    def __init__(self, curstatus: int, fq_path: str, parent=None):
        super().__init__(parent)
        self.curstatus = curstatus
        self.fq_path = fq_path
        self.newstatus = None

        self.setWindowTitle('Reprocess')
        layout = QtWidgets.QVBoxLayout()

        self.descrip_lbl = QtWidgets.QLabel(
            f'Set the current processing status of:\n\n{fq_path}\n\nNote: Processing status cannot be set to a more advanced '
            'status, can only be rolled back')
        layout.addWidget(self.descrip_lbl)

        self.curstatus_lbl = QtWidgets.QLabel(f'Current Status: {kluster_variables.status_lookup[curstatus]}')
        layout.addWidget(self.curstatus_lbl)

        newstatus_layout = QtWidgets.QHBoxLayout()
        self.newstatus_lbl = QtWidgets.QLabel('New Status: ')
        newstatus_layout.addWidget(self.newstatus_lbl)
        statkeys = []
        for ky, val in kluster_variables.status_reverse_lookup.items():
            if val < curstatus and val > 0:
                statkeys.append(ky)
        self.newstatus_dropdown = QtWidgets.QComboBox()
        self.newstatus_dropdown.addItems(statkeys)
        newstatus_layout.addWidget(self.newstatus_dropdown)
        layout.addLayout(newstatus_layout)

        hlayout_two = QtWidgets.QHBoxLayout()
        hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        hlayout_two.addWidget(self.ok_button)
        hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        hlayout_two.addWidget(self.cancel_button)
        hlayout_two.addStretch(1)
        layout.addLayout(hlayout_two)

        self.setLayout(layout)

        self.ok_button.clicked.connect(self.set_status)
        self.cancel_button.clicked.connect(self.cancel_status)

    def set_status(self):
        self.newstatus = self.newstatus_dropdown.currentText()
        self.newstatus = kluster_variables.status_reverse_lookup[self.newstatus]
        self.canceled = False
        self.accept()

    def cancel_status(self):
        self.newstatus = None
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ReprocessDialog(4, 'test_path')
    dlog.show()
    if dlog.exec_():
        print(dlog.newstatus)
