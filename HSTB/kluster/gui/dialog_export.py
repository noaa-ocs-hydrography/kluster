from PySide2 import QtWidgets

from HSTB.kluster.gui.common_widgets import BrowseListWidget


class ExportDialog(QtWidgets.QDialog):
    """
    Dialog allows for providing fqpr data for exporting and the desired export type, in self.export_opts.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Export Soundings')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Export from the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Export to: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.export_opts = QtWidgets.QComboBox()
        self.export_opts.addItems(['csv', 'las', 'entwine'])
        self.export_opts.setMaximumWidth(100)
        self.hlayout_one.addWidget(self.export_opts)
        self.hlayout_one.addStretch()

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : red; }")

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_two.addWidget(self.ok_button)
        self.hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_two.addWidget(self.cancel_button)
        self.hlayout_two.addStretch(1)

        layout.addWidget(self.input_msg)
        layout.addLayout(self.hlayout_zero)
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False

        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.ok_button.clicked.connect(self.start_export)
        self.cancel_button.clicked.connect(self.cancel_export)

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

    def start_export(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if not self.fqpr_inst:
            self.status_msg.setText('Error: No data provided')
        else:
            self.canceled = False
            self.accept()

    def cancel_export(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = ExportDialog()
    dlog.show()
    if dlog.exec_():
        pass
