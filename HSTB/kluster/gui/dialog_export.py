from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster.pydro_helpers import is_pydro
from HSTB.kluster import kluster_variables


class ExportDialog(SaveStateDialog):
    """
    Dialog allows for providing fqpr data for exporting and the desired export type, in self.export_opts.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='export')

        self.setWindowTitle('Export Soundings')
        layout = QtWidgets.QVBoxLayout()

        self.basic_export_group = QtWidgets.QGroupBox('Export from the following datasets:')
        self.basic_export_group.setCheckable(True)
        self.basic_export_group.setChecked(True)
        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.input_fqpr.setMinimumWidth(600)
        self.hlayout_zero.addWidget(self.input_fqpr)
        self.basic_export_group.setLayout(self.hlayout_zero)

        self.line_export = QtWidgets.QCheckBox('Export selected lines')
        self.line_export.setChecked(False)

        self.points_view_export = QtWidgets.QCheckBox('Export points in Points View')
        self.points_view_export.setChecked(False)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Export to: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.export_opts = QtWidgets.QComboBox()
        # self.export_opts.addItems(['csv', 'las', 'entwine'])  need to add entwine to the env
        self.export_opts.addItems(['csv', 'las'])
        self.hlayout_one.addWidget(self.export_opts)
        self.csvdelimiter_lbl = QtWidgets.QLabel('Delimiter')
        self.hlayout_one.addWidget(self.csvdelimiter_lbl)
        self.csvdelimiter_dropdown = QtWidgets.QComboBox(self)
        self.csvdelimiter_dropdown.addItems(['comma', 'space'])
        self.hlayout_one.addWidget(self.csvdelimiter_dropdown)
        self.hlayout_one.addStretch()

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.zdirect_check = QtWidgets.QCheckBox('Make Z Positive Down')
        self.zdirect_check.setChecked(True)
        self.hlayout_one_one.addWidget(self.zdirect_check)
        self.hlayout_one_one.addStretch()

        self.hlayout_one_three = QtWidgets.QHBoxLayout()
        self.filter_chk = QtWidgets.QCheckBox('Filter Rejected')
        self.filter_chk.setChecked(True)
        self.hlayout_one_three.addWidget(self.filter_chk)
        self.byidentifier_chk = QtWidgets.QCheckBox('Separate Files by Sector/Frequency')
        self.byidentifier_chk.setChecked(False)
        self.hlayout_one_three.addWidget(self.byidentifier_chk)
        self.hlayout_one_three.addStretch()

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

        layout.addWidget(self.basic_export_group)
        layout.addWidget(self.line_export)
        layout.addWidget(self.points_view_export)
        layout.addWidget(QtWidgets.QLabel(' '))
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_one_one)
        layout.addLayout(self.hlayout_one_three)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False

        self.basic_export_group.toggled.connect(self._handle_basic_checked)
        self.line_export.toggled.connect(self._handle_line_checked)
        self.points_view_export.toggled.connect(self._handle_points_checked)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.export_opts.currentTextChanged.connect(self._event_update_status)
        self.ok_button.clicked.connect(self.start_export)
        self.cancel_button.clicked.connect(self.cancel_export)

        self.text_controls = [['export_ops', self.export_opts], ['csvdelimiter_dropdown', self.csvdelimiter_dropdown]]
        self.checkbox_controls = [['basic_export_group', self.basic_export_group], ['line_export', self.line_export],
                                  ['points_view_export', self.points_view_export], ['zdirect_check', self.zdirect_check],
                                  ['filter_chk', self.filter_chk], ['byidentifier_chk', self.byidentifier_chk]]
        self.read_settings()
        self._event_update_status(self.export_opts.currentText())

    def _handle_basic_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_export.setChecked(False)
            self.points_view_export.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color: " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_line_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.basic_export_group.setChecked(False)
            self.points_view_export.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_points_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_export.setChecked(False)
            self.basic_export_group.setChecked(False)
            if not self.fqpr_inst:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
                self.status_msg.setText('Error: Ensure you have one of the datasets that contain these points listed in "Export from the following datasets"')
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

        self._show_hide_options(combobox_text)
        if combobox_text == 'entwine':
            ispydro = is_pydro()
            if ispydro:  # If this is the pydro environment, we know it has Entwine
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
                self.status_msg.setText('Pydro found, entwine export allowed')
                self.ok_button.setEnabled(True)
            else:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
                self.status_msg.setText('Error: Pydro not found, entwine export is not allowed')
                self.ok_button.setEnabled(False)
        else:
            self.status_msg.setText('')
            self.ok_button.setEnabled(True)
        if combobox_text == 'csv':
            self.zdirect_check.show()
        else:
            self.zdirect_check.hide()

    def _show_hide_options(self, combobox_text):
        if combobox_text == 'csv':
            self.csvdelimiter_dropdown.show()
            self.csvdelimiter_lbl.show()
        else:
            self.csvdelimiter_dropdown.hide()
            self.csvdelimiter_lbl.hide()

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
        if self.points_view_export.isChecked():
            self._handle_points_checked(True)

    def start_export(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if self.basic_export_group.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No data provided')
        elif self.points_view_export.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: You must provide at least one dataset that the points come from before exporting')
        elif not self.basic_export_group.isChecked() and not self.line_export.isChecked() and not self.points_view_export.isChecked():
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: You must select one of the three export modes (export datasets, export lines, export points)')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()

    def cancel_export(self):
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
    dlog = ExportDialog()
    dlog.show()
    if dlog.exec_():
        pass
