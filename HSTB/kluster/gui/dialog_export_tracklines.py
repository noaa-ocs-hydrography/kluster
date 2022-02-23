from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables


class ExportTracklinesDialog(SaveStateDialog):
    """
    Dialog allows for providing fqpr data for exporting and the desired export type, in self.export_opts.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='export_tracklines')

        self.setWindowTitle('Export Tracklines')
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

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Export to: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.export_opts = QtWidgets.QComboBox()
        self.export_opts.addItems(['geopackage'])
        self.hlayout_one.addWidget(self.export_opts)
        self.hlayout_one.addStretch()

        self.output_msg = QtWidgets.QLabel('Export to the following:')

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.output_text = QtWidgets.QLineEdit('', self)
        self.output_text.setMinimumWidth(400)
        self.output_text.setReadOnly(True)
        self.hlayout_one_one.addWidget(self.output_text)
        self.output_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one_one.addWidget(self.output_button)

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
        layout.addWidget(QtWidgets.QLabel(' '))
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.output_msg)
        layout.addLayout(self.hlayout_one_one)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False

        self.basic_export_group.toggled.connect(self._handle_basic_checked)
        self.line_export.toggled.connect(self._handle_line_checked)
        self.output_button.clicked.connect(self.output_file_browse)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.ok_button.clicked.connect(self.start_export)
        self.cancel_button.clicked.connect(self.cancel_export)

        self.text_controls = [['export_ops', self.export_opts], ['output_text', self.output_text]]
        self.checkbox_controls = [['basic_export_group', self.basic_export_group], ['line_export', self.line_export]]
        self.read_settings()

    @property
    def export_format(self):
        if self.export_opts.currentText() == 'geopackage':
            return 'GPKG'

    def _handle_basic_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_export.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color: " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_line_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.basic_export_group.setChecked(False)
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def output_file_browse(self):
        curr_opts = self.export_opts.currentText().lower()
        if curr_opts == 'geopackage':
            titl = 'Select output geopackage file'
            ffilter = "gpkg file|*.gpkg"
        else:
            raise ValueError('dialog_export_tracklines: unrecognized method: {}'.format(curr_opts))

        # dirpath will be None or a string
        msg, outpth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster', DefaultFile=self.output_text.text(),
                                                            Title=titl, fFilter=ffilter, AppName='\\reghelp')
        if outpth:
            self.output_text.setText(outpth)

    def _event_update_fqpr_instances(self):
        """
        Method for connecting the input_fqpr signal with the update_fqpr_instances
        """

        self.update_fqpr_instances()

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
        self.status_msg.setText('')

    def start_export(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if self.basic_export_group.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No data provided')
        elif not self.basic_export_group.isChecked() and not self.line_export.isChecked():
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
    dlog = ExportTracklinesDialog()
    dlog.show()
    if dlog.exec_():
        pass
