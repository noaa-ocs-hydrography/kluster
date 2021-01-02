from PySide2 import QtWidgets

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.shared import RegistryHelpers


class SurfaceDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Generate New Surface')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Run surface generation on the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.surface_box = QtWidgets.QGroupBox('New Surface')
        self.surf_layout = QtWidgets.QVBoxLayout()
        self.surf_msg = QtWidgets.QLabel('Select from the following options:')
        self.surf_layout.addWidget(self.surf_msg)

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.surf_method_lbl = QtWidgets.QLabel('Method: ')
        self.hlayout_one_one.addWidget(self.surf_method_lbl)
        self.surf_method = QtWidgets.QComboBox()
        self.surf_method.addItems(['nearest', 'linear', 'cubic'])
        self.surf_method.setMaximumWidth(100)
        self.hlayout_one_one.addWidget(self.surf_method)
        self.surf_resolution_lbl = QtWidgets.QLabel('Resolution: ')
        self.hlayout_one_one.addWidget(self.surf_resolution_lbl)
        self.surf_resolution = QtWidgets.QLineEdit('')
        self.surf_resolution.setInputMask('000.0;_')
        self.surf_resolution.setMaximumWidth(40)
        self.hlayout_one_one.addWidget(self.surf_resolution)
        self.soundings_per_node_lbl = QtWidgets.QLabel('Required Soundings/Node: ')
        self.hlayout_one_one.addWidget(self.soundings_per_node_lbl)
        self.soundings_per_node = QtWidgets.QLineEdit('')
        self.soundings_per_node.setInputMask('0;_')
        self.soundings_per_node.setMaximumWidth(13)
        self.soundings_per_node.setText('5')
        self.soundings_per_node.setDisabled(True)
        self.hlayout_one_one.addWidget(self.soundings_per_node)
        self.hlayout_one_one.addStretch(1)
        self.surf_layout.addLayout(self.hlayout_one_one)

        self.output_msg = QtWidgets.QLabel('Select the output path for the surface')
        self.surf_layout.addWidget(self.output_msg)

        self.hlayout_one_two = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('', self)
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout_one_two.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one_two.addWidget(self.browse_button)
        self.surf_layout.addLayout(self.hlayout_one_two)

        self.surface_box.setLayout(self.surf_layout)
        self.hlayout_one.addWidget(self.surface_box)

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
        self.output_pth = None

        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.browse_button.clicked.connect(self.file_browse)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

    def _event_update_fqpr_instances(self):
        self.update_fqpr_instances()

    def update_fqpr_instances(self, addtl_files=None):
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]

    def file_browse(self):
        msg, self.output_pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                                     Title='Select output surface path',
                                                                     AppName='kluster', bMulti=False,
                                                                     bSave=True, fFilter='numpy npz (*.npz)')
        if self.output_pth is not None:
            self.fil_text.setText(self.output_pth)

    def return_processing_options(self):
        if not self.canceled:
            # pull CRS from the first fqpr instance, as we've already checked to ensure they are identical
            opts = {'fqpr_inst': self.fqpr_inst, 'resolution': float(self.surf_resolution.text()),
                    'method': self.surf_method.currentText(), 'soundings_per_node': int(self.soundings_per_node.text()),
                    'output_path': self.fil_text.text()}
        else:
            opts = None
        return opts

    def start_processing(self):
        if not self.surf_resolution.text() or not self.fil_text.text():
            self.status_msg.setText('Error: You must complete all dialog options to proceed')
        else:
            self.canceled = False
            self.accept()

    def cancel_processing(self):
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = SurfaceDialog()
    dlog.show()
    if dlog.exec_():
        print(dlog.return_processing_options())
