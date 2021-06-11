import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import TwoListWidget
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables


class SurfaceDataDialog(QtWidgets.QDialog):
    """
    Dialog for managing surface data, accessed when you right click on a surface and view the surface data.  You can
    add or remove days of multibeam data from this surface using this dialog, and optionally regrid the surface.
    """

    def __init__(self, parent=None, title=''):
        super().__init__(parent)

        self.toplayout = QtWidgets.QVBoxLayout()
        self.setWindowTitle('Update Surface Data')

        self.listdata = TwoListWidget(title, 'Current Containers', 'Possible Containers')
        self.toplayout.addWidget(self.listdata)

        self.update_checkbox = QtWidgets.QCheckBox('Update Existing Container Data')
        self.update_checkbox.setToolTip('Check this box to update all asterisk (*) marked containers in this surface for changes.\n' +
                                        'Updating means the container will be removed and then added back into the surface.  This must\n' +
                                        'be done for changes made in Kluster to take effect in the surface.')
        self.update_checkbox.setChecked(True)
        self.toplayout.addWidget(self.update_checkbox)
        self.regrid_checkbox = QtWidgets.QCheckBox('Rebuild Gridded Data')
        self.regrid_checkbox.setToolTip('Check this box to immediately grid all new/updated containers after hitting OK')
        self.regrid_checkbox.setChecked(True)
        self.toplayout.addWidget(self.regrid_checkbox)

        self.use_dask_checkbox = QtWidgets.QCheckBox('Process in Parallel')
        self.use_dask_checkbox.setToolTip('With this checked, gridding will be done in parallel using the Dask Client.  Assuming you have multiple\n' +
                                          'tiles, this should improve performance significantly.  You may experience some instability, although this\n' +
                                          'current implementation has not shown any during testing.')
        self.toplayout.addWidget(self.use_dask_checkbox)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")
        self.toplayout.addWidget(self.status_msg)

        self.hlayout_button = QtWidgets.QHBoxLayout()
        self.hlayout_button.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_button.addWidget(self.ok_button)
        self.hlayout_button.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_button.addWidget(self.cancel_button)
        self.hlayout_button.addStretch(1)
        self.toplayout.addLayout(self.hlayout_button)

        self.setLayout(self.toplayout)

        self.original_current = []
        self.original_possible = []

        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

    def setup(self, current_containers: list, possible_containers: list):
        for cont in current_containers:
            self.listdata.add_left_list(cont)
        for cont in possible_containers:
            self.listdata.add_right_list(cont)
        self.original_current = current_containers
        self.original_possible = possible_containers

    def return_processing_options(self):
        current_containers = self.listdata.return_left_list_data()
        possible_containers = self.listdata.return_right_list_data()
        update_container = self.update_checkbox.isChecked()
        regrid_container = self.regrid_checkbox.isChecked()

        add_fqpr = []
        remove_fqpr = []
        if update_container:  # an update is simply a remove/add of a container
            needs_update = [cont[:-1] for cont in current_containers if cont[-1] == '*']
            for nu in needs_update:
                add_fqpr.append(nu)
                remove_fqpr.append(nu)
        for curr in self.original_current:
            if curr not in current_containers:
                if curr[-1] == '*':
                    curr = curr[:-1]
                remove_fqpr.append(curr)
        for newcurr in current_containers:
            if newcurr in self.original_possible:
                add_fqpr.append(newcurr)
        return add_fqpr, remove_fqpr, {'regrid': regrid_container, 'use_dask': self.use_dask_checkbox.isChecked()}

    def start_processing(self):
        if not self.listdata.return_left_list_data():
            self.status_msg.setText('Error: You must include at least one point source to continue')
        else:
            self.canceled = False
            self.accept()

    def cancel_processing(self):
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = SurfaceDataDialog()
    dlog.setup(['a', 'b', 'c*'], ['d', 'e', 'f'])
    dlog.show()
    if dlog.exec_():
        print(dlog.return_processing_options())