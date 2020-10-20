import os
import sys
from PySide2 import QtWidgets, QtGui, QtCore

from HSTB.shared import RegistryHelpers
from HSTB.kluster.gui.common_widgets import BrowseListWidget


class ConversionDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Multibeam Conversion')
        layout = QtWidgets.QVBoxLayout()

        self.start_msg = QtWidgets.QLabel('Select the output directory for the converted data:')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('', self)
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout_one.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one.addWidget(self.browse_button)

        self.tree_msg = QtWidgets.QLabel('Output will look like this:')

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.output_tree = QtWidgets.QTextEdit('', self)
        self.output_tree.setReadOnly(True)
        self.output_tree.setMinimumHeight(110)
        self.output_tree.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Expanding)
        self.hlayout_two.addWidget(self.output_tree)

        self.infiles_msg = QtWidgets.QLabel('Input Files include:')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.mbesfiles = BrowseListWidget(self)
        self.mbesfiles.setup(registry_key='kluster', app_name='klusterbrowse', supported_file_extension='.all',
                             multiselect=True, filebrowse_title='Select .all files',
                             filebrowse_filter='Kongsberg (*.all)')
        self.hlayout_three.addWidget(self.mbesfiles)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : red; }")

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.hlayout_four.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_four.addWidget(self.ok_button)
        self.hlayout_four.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_four.addWidget(self.cancel_button)
        self.hlayout_four.addStretch(1)

        layout.addWidget(self.start_msg)
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.tree_msg)
        layout.addLayout(self.hlayout_two)
        layout.addWidget(self.infiles_msg)
        layout.addLayout(self.hlayout_three)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_four)
        self.setLayout(layout)

        self.output_pth = None
        self.cancelled = False

        self.update_output_structure_view()

        self.multibeam_files = []

        self.mbesfiles.files_updated.connect(self._event_update_multibeam_files)
        self.browse_button.clicked.connect(self.file_browse)
        self.ok_button.clicked.connect(self.return_path)
        self.cancel_button.clicked.connect(self.return_without_path)

    def _event_update_multibeam_files(self, e):
        self.update_multibeam_files()

    def update_multibeam_files(self, addtl_files=None):
        if addtl_files is not None:
            self.mbesfiles.add_new_files(addtl_files)
        self.multibeam_files = [self.mbesfiles.list_widget.item(i).text() for i in range(self.mbesfiles.list_widget.count())]

    def file_browse(self):
        # dirpath will be None or a string
        msg, self.output_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                                Title='Select output directory', AppName='\\reghelp')
        if self.output_pth is not None:
            self.fil_text.setText(self.output_pth)
        self.update_output_structure_view()

    def update_output_structure_view(self):
        if (self.output_pth is None) or (not self.output_pth):
            self.output_tree.setText('')
        else:
            # if the folder exists but is not empty, we are going to be adding a timestamp so that we never convert
            #   into an existing storage folder, illustrate this below with suffix
            suffix = ''
            if os.path.exists(self.output_pth) and os.listdir(self.output_pth):
                suffix = '_XXXXXX'
            file_structure = '{}\n'.format(self.output_pth + suffix)
            file_structure += r' - {}\attitude.zarr'.format(self.output_pth + suffix) + '\n'
            file_structure += r' - {}\fqpr.zarr'.format(self.output_pth + suffix) + '\n'
            file_structure += r' - {}\navigation.zarr'.format(self.output_pth + suffix) + '\n'
            file_structure += r' - {}\ping_xxxx.zarr'.format(self.output_pth + suffix) + '\n'
            file_structure += r' - {}\logfile_XXXX.txt'.format(self.output_pth + suffix) + '\n'
            self.output_tree.setText(file_structure)
            self.status_msg.setText('')

    def return_path(self):
        if self.output_pth is not None:
            self.accept()
        else:
            self.status_msg.setText('Error: You must either Browse to a directory or hit Cancel')

    def return_without_path(self):
        self.cancelled = True
        self.accept()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = ConversionDialog()
    dlog.show()
    dlog.update_multibeam_files(['testone.all', 'testtwo.all', 'testthree.all', 'testfour.all', 'testfive.all'])
    if dlog.exec_():
        print(dlog.output_pth)
        print(dlog.multibeam_files)
