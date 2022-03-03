from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster import kluster_variables


class FilterDialog(SaveStateDialog):
    """
    Dialog allows for providing fqpr data for filtering, and the selection of a filter algorithm.  Will
    spawn a separate dialog for the parameters required by the filter.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, filters: list, parent=None, settings=None):
        super().__init__(parent, settings, widgetname='filter')

        self.setWindowTitle('Filter Soundings')
        layout = QtWidgets.QVBoxLayout()

        self.basic_filter_group = QtWidgets.QGroupBox('Load from the following datasets:')
        self.basic_filter_group.setCheckable(True)
        self.basic_filter_group.setChecked(True)
        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.input_fqpr.setMinimumWidth(600)
        self.hlayout_zero.addWidget(self.input_fqpr)
        self.basic_filter_group.setLayout(self.hlayout_zero)

        self.line_filter = QtWidgets.QCheckBox('Load from selected lines')
        self.line_filter.setChecked(False)

        self.points_view_filter = QtWidgets.QCheckBox('Load points in Points View')
        self.points_view_filter.setChecked(False)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Run filter: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.filter_opts = QtWidgets.QComboBox()
        self.filter_opts.addItems(filters)
        self.hlayout_one.addWidget(self.filter_opts)
        self.hlayout_one.addStretch()

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_two.addWidget(self.ok_button)
        self.hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_two.addWidget(self.cancel_button)
        self.hlayout_two.addStretch(1)

        layout.addWidget(self.basic_filter_group)
        layout.addWidget(self.line_filter)
        layout.addWidget(self.points_view_filter)
        layout.addWidget(QtWidgets.QLabel(' '))
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False

        self.basic_filter_group.toggled.connect(self._handle_basic_checked)
        self.line_filter.toggled.connect(self._handle_line_checked)
        self.points_view_filter.toggled.connect(self._handle_points_checked)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.filter_opts.currentTextChanged.connect(self._event_update_status)
        self.ok_button.clicked.connect(self.start_filter)
        self.cancel_button.clicked.connect(self.cancel_filter)

        self.text_controls = [['filter_ops', self.filter_opts]]
        self.checkbox_controls = [['basic_filter_group', self.basic_filter_group], ['line_filter', self.line_filter],
                                  ['points_view_filter', self.points_view_filter]]
        self.read_settings()
        self._event_update_status(self.filter_opts.currentText())

    def _handle_basic_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_filter.setChecked(False)
            self.points_view_filter.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color: " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_line_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.basic_filter_group.setChecked(False)
            self.points_view_filter.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_points_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_filter.setChecked(False)
            self.basic_filter_group.setChecked(False)
            if not self.fqpr_inst:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
                self.status_msg.setText('Error: Ensure you have one of the datasets that contain these points listed in "filter from the following datasets"')
            else:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
                self.status_msg.setText('')

    def _event_update_fqpr_instances(self):
        """
        Method for connecting the input_fqpr signal with the update_fqpr_instances
        """

        self.update_fqpr_instances()

    def _event_update_status(self, combobox_text: str):
        """
        Update the status message if an Error presents itself.  Also controls the OK button, to prevent kicking off
        a process if we know it isn't going to work

        Parameters
        ----------
        combobox_text
            value of the combobox as text
        """

        self.status_msg.setText('')
        self.ok_button.setEnabled(True)

    def update_fqpr_instances(self, addtl_files=None):
        """
        Used through kluster_main to update the list_widget with new multibeam files to process

        Parameters
        ----------
        addtl_files: optional, list, list of multibeam files (string file paths)

        """
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]
        if self.points_view_filter.isChecked():
            self._handle_points_checked(True)

    def start_filter(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if self.basic_filter_group.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No data provided')
        elif self.points_view_filter.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: You must provide at least one dataset that the points come from before filtering')
        elif not self.basic_filter_group.isChecked() and not self.line_filter.isChecked() and not self.points_view_filter.isChecked():
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: You must select one of the three filter modes (filter datasets, filter lines, filter points)')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()

    def cancel_filter(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = FilterDialog(['test'])
    dlog.show()
    if dlog.exec_():
        pass
