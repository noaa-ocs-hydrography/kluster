import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import TwoTreeWidget, SaveStateDialog
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables


class SurfaceDataDialog(SaveStateDialog):
    """
    Dialog for managing surface data, accessed when you right click on a surface and view the surface data.  You can
    add or remove days of multibeam data from this surface using this dialog, and optionally regrid the surface.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='surface_data')

        self.toplayout = QtWidgets.QVBoxLayout()
        self.setWindowTitle('Update Surface Data')

        self.listdata = TwoTreeWidget(title, 'In the Surface', 'Possible Containers')
        self.mark_for_update_button = QtWidgets.QPushButton('Mark Update')
        self.mark_for_update_button.setToolTip('Mark one of the "In the Surface" containers as needing to be re-added to the grid')
        self.mark_for_update_button.setDisabled(True)
        self.listdata.center_layout.addWidget(self.mark_for_update_button)
        self.listdata.center_layout.addStretch()
        self.toplayout.addWidget(self.listdata)

        self.update_checkbox = QtWidgets.QCheckBox('Update Existing Container Data')
        self.update_checkbox.setToolTip('Check this box to update all asterisk (*) marked containers in this surface for changes.\n' +
                                        'Updating means the container will be removed and then added back into the surface.  This must\n' +
                                        'be done for changes made in Kluster to take effect in the surface.')
        self.update_checkbox.setChecked(True)
        self.toplayout.addWidget(self.update_checkbox)

        self.regrid_layout = QtWidgets.QHBoxLayout()
        self.regrid_checkbox = QtWidgets.QCheckBox('Re-Grid Data')
        self.regrid_checkbox.setToolTip('Check this box to immediately grid all/updated containers after hitting OK')
        self.regrid_checkbox.setChecked(True)
        self.regrid_layout.addWidget(self.regrid_checkbox)
        self.regrid_options = QtWidgets.QComboBox()
        self.regrid_options.addItems(['The whole grid', 'Only where points have changed'])
        self.regrid_options.setToolTip('Controls what parts of the grid get re-gridded on running this tool\n\n' +
                                       'The whole grid - will regrid the whole grid, generally this is not needed\n' +
                                       'Only where points have changed - will only update the grid where containers have been removed or added')
        self.regrid_options.setCurrentText('Only where points have changed')
        self.regrid_layout.addWidget(self.regrid_options)
        self.regrid_layout.addStretch()
        self.toplayout.addLayout(self.regrid_layout)

        # self.use_dask_checkbox = QtWidgets.QCheckBox('Process in Parallel')
        # self.use_dask_checkbox.setToolTip('With this checked, gridding will be done in parallel using the Dask Client.  Assuming you have multiple\n' +
        #                                   'tiles, this should improve performance significantly.  You may experience some instability, although this\n' +
        #                                   'current implementation has not shown any during testing.')
        # self.toplayout.addWidget(self.use_dask_checkbox)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
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

        self.original_current = {}
        self.original_possible = {}
        self.canceled = False

        self.mark_for_update_button.clicked.connect(self.mark_for_update)
        self.listdata.left_tree.clicked.connect(self.enable_markbutton)
        self.listdata.right_tree.clicked.connect(self.disable_markbutton)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

        self.text_controls = [['regrid_options', self.regrid_options]]
        self.checkbox_controls = [['update_checkbox', self.update_checkbox], ['regrid_checkbox', self.regrid_checkbox]]
        self.read_settings()

    def mark_for_update(self):
        curitem = self.listdata.left_tree.selectedIndexes()[0]
        curitem_text = curitem.data()

        if curitem_text in self.listdata.left_tree.tree_data['Data'][1:]:  # selected a container, mark all lines for update
            idx = self.listdata.left_tree.tree_data['Data'][1:].index(curitem_text)
            container = self.listdata.left_tree.tree_data['Data'][0].child(idx)
            for cnt in range(container.rowCount()):
                container_child = container.child(cnt)
                curitem_text = container_child.text()
                if curitem_text[-1] == '*':
                    container_child.setText(curitem_text[:-1])
                else:
                    container_child.setText(curitem_text + '*')
        elif curitem.parent().data() in self.listdata.left_tree.tree_data['Data'][1:]:  # selected a line, mark for update
            idx = self.listdata.left_tree.tree_data['Data'][1:].index(curitem.parent().data())
            container = self.listdata.left_tree.tree_data['Data'][0].child(idx)
            child_idx = [rw for rw in range(container.rowCount()) if container.child(rw).text() == curitem_text]
            if child_idx:
                container_child = container.child(child_idx[0])
                curitem_text = container_child.text()
                if curitem_text[-1] == '*':
                    container_child.setText(curitem_text[:-1])
                else:
                    container_child.setText(curitem_text + '*')

    def enable_markbutton(self):
        if self.listdata.left_tree.selectedIndexes():
            self.mark_for_update_button.setDisabled(False)
            self.listdata.right_tree.selectionModel().clearSelection()
        else:
            self.mark_for_update_button.setDisabled(True)

    def disable_markbutton(self):
        if self.listdata.right_tree.selectedIndexes():
            self.mark_for_update_button.setDisabled(True)
            self.listdata.left_tree.selectionModel().clearSelection()

    def setup(self, current_containers: dict, possible_containers: dict):
        self.listdata.add_left_tree(current_containers)
        self.listdata.add_right_tree(possible_containers)
        self.original_current = current_containers
        self.original_possible = possible_containers
        self.listdata.expand_all()

    def return_processing_options(self):
        current_containers = self.listdata.return_left_tree_data()
        update_container = self.update_checkbox.isChecked()
        regrid_container = self.regrid_checkbox.isChecked()
        regrid_option = self.regrid_options.currentText()
        if regrid_option == 'The whole grid':
            regrid_option = 'full'
        elif regrid_option == 'Only where points have changed':
            regrid_option = 'update'
        else:
            raise ValueError("Expected regrid option to be one of ['The whole grid', 'Only where points have changed'], found {}".format(regrid_option))

        # build out the logic for which lines/containers need to be added/removed
        add_fqpr = {}
        remove_fqpr = {}
        if update_container:  # an update is simply a remove/add of a container
            for container, lines in current_containers.items():
                needs_update = [ln[:-1] for ln in lines if ln[-1] == '*']
                if needs_update:
                    add_fqpr[container] = needs_update
                    remove_fqpr[container] = needs_update
        for container, lines in self.original_current.items():
            fmatlines = [ln[:-1] for ln in lines if ln[-1] == '*'] + [ln for ln in lines if ln[-1] != '*']
            remove_lines = []
            if container in current_containers:
                remove_lines = [ln for ln in fmatlines if ln not in current_containers[container]]
            if container not in current_containers and container not in remove_fqpr:  # whole container was removed
                remove_fqpr[container] = fmatlines
            elif remove_lines:  # some original lines are no longer there
                if container in remove_fqpr:
                    remove_fqpr[container] += [ln for ln in remove_lines if ln not in remove_fqpr[container]]
                else:
                    remove_fqpr[container] = remove_lines
        for container, lines in current_containers.items():
            fmatlines = [ln[:-1] for ln in lines if ln[-1] == '*'] + [ln for ln in lines if ln[-1] != '*']
            new_lines = []
            if container in self.original_current:
                new_lines = [ln for ln in fmatlines if ln not in self.original_current[container]]
            if container not in self.original_current and container not in add_fqpr:  # whole container was added
                add_fqpr[container] = fmatlines
            elif new_lines:  # some new lines are there
                if container in add_fqpr:
                    add_fqpr[container] += [ln for ln in new_lines if ln not in add_fqpr[container]]
                else:
                    add_fqpr[container] = new_lines
        add_container = list(add_fqpr.keys())
        add_lines = list(add_fqpr.values())
        remove_container = list(remove_fqpr.keys())
        remove_lines = list(remove_fqpr.values())
        if not add_container:
            add_container = None
            add_lines = None
        if not remove_container:
            remove_container = None
            remove_lines = None
        return add_container, add_lines, remove_container, remove_lines, {'regrid': regrid_container, 'use_dask': False,
                                                                          'regrid_option': regrid_option}

    def start_processing(self):
        if not self.listdata.return_left_tree_data():
            self.status_msg.setText('Error: You must include at least one point source to continue')
        else:
            self.canceled = False
            self.save_settings()
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
    dlog.setup({'a': ['1', '2', '3'], 'b': ['4', '5'], 'c': ['6']}, {'d': ['7', '8']})
    dlog.show()
    if dlog.exec_():
        print(dlog.return_processing_options())