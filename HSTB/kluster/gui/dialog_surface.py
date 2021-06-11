import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables


class SurfaceDialog(QtWidgets.QDialog):
    """
    Dialog for selecting surfacing options that we want to use to generate a new surface.
    """
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
        self.input_fqpr.setMinimumWidth(500)
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.surf_layout = QtWidgets.QVBoxLayout()
        self.surf_msg = QtWidgets.QLabel('Select from the following options:')
        self.surf_layout.addWidget(self.surf_msg)

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.surf_method_lbl = QtWidgets.QLabel('Method: ')
        self.hlayout_one_one.addWidget(self.surf_method_lbl)
        self.surf_method = QtWidgets.QComboBox()
        self.surf_method.addItems(['Mean', 'Shoalest'])
        self.surf_method.setMaximumWidth(100)
        self.surf_method.setToolTip('The algorithm used when gridding, will use this to determine the depth/uncertainty value of the cell')
        self.hlayout_one_one.addWidget(self.surf_method)
        self.hlayout_one_one.addStretch()
        self.grid_type_lbl = QtWidgets.QLabel('Grid Type: ')
        self.hlayout_one_one.addWidget(self.grid_type_lbl)
        self.grid_type = QtWidgets.QComboBox()
        self.grid_type.addItems(['Single Resolution', 'Variable Resolution Tile'])
        self.grid_type.setToolTip('Single Resolution grids are a single large grid that covers the whole survey area\n' +
                                  'that is then tiled (tile size) where each tile is the same resolution.  Single resolution\n' +
                                  'grids are simple to compute but will struggle when there is a lot of depth change in your\n' +
                                  'survey area.\n\nVariable Resolution Tile grid is a large grid that encompasses other smaller\n' +
                                  'grids (tile size) that can contain tiles (subtile size) of any resolution (that is a power of two).\n' +
                                  "Setting the resolution to auto will allow each subtile to determine it's own resolution.")
        self.hlayout_one_one.addWidget(self.grid_type)
        self.surf_layout.addLayout(self.hlayout_one_one)

        self.hlayout_singlerez_one = QtWidgets.QHBoxLayout()
        self.single_rez_tile_size_lbl = QtWidgets.QLabel('Tile Size (meters): ')
        self.hlayout_singlerez_one.addWidget(self.single_rez_tile_size_lbl)
        self.single_rez_tile_size = QtWidgets.QComboBox()
        self.single_rez_tile_size.addItems(['2048', '1024', '512', '256', '128'])
        self.single_rez_tile_size.setCurrentText('1024')
        self.single_rez_tile_size.setToolTip('The size of the single resolution tile in meters.  A larger value will improve performance\n' +
                                             'if the survey area is very large, but it is recommended to leave this at 1024 most of the time.\n' +
                                             'Lowering it will reduce performance, but might improve efficiency for small survey areas.')
        self.hlayout_singlerez_one.addWidget(self.single_rez_tile_size)
        self.hlayout_singlerez_one.addStretch()
        self.single_rez_resolution_lbl = QtWidgets.QLabel('Resolution: ')
        self.hlayout_singlerez_one.addWidget(self.single_rez_resolution_lbl)
        self.single_rez_resolution = QtWidgets.QComboBox()
        self.single_rez_resolution.addItems(['AUTO', '0.25', '0.50', '1.0', '2.0', '4.0', '8.0', '16.0', '32.0', '64.0', '128.0'])
        self.single_rez_resolution.setCurrentText('AUTO')
        self.single_rez_resolution.setToolTip('The resolution of the single resolution tile in meters.  Higher resolution values allow for a more detailed grid,\n' +
                                              'but will produce holes in the grid if there is not enough data.  Auto will follow the NOAA specifications guidance,\n' +
                                              'using the depth to resolution lookup table.')
        self.hlayout_singlerez_one.addWidget(self.single_rez_resolution)
        self.surf_layout.addLayout(self.hlayout_singlerez_one)

        self.hlayout_variabletile_one = QtWidgets.QHBoxLayout()
        self.variabletile_tile_size_lbl = QtWidgets.QLabel('Tile Size (meters): ')
        self.hlayout_variabletile_one.addWidget(self.variabletile_tile_size_lbl)
        self.variabletile_tile_size = QtWidgets.QComboBox()
        self.variabletile_tile_size.addItems(['2048', '1024', '512', '256', '128'])
        self.variabletile_tile_size.setCurrentText('1024')
        self.variabletile_tile_size.setToolTip('The size of the subgrid in the variable resolution grid in meters.  Largly matters when running in parallel with Dask,\n'
                                               'this is the size of the chunk of the survey that a single worker will handle.')
        self.hlayout_variabletile_one.addWidget(self.variabletile_tile_size)
        self.variabletile_resolution_lbl = QtWidgets.QLabel('Resolution (meters): ')
        self.hlayout_variabletile_one.addStretch()
        self.hlayout_variabletile_one.addWidget(self.variabletile_resolution_lbl)
        self.variabletile_resolution = QtWidgets.QComboBox()
        self.variabletile_resolution.addItems(['AUTO'])
        self.variabletile_resolution.setCurrentText('AUTO')
        self.single_rez_resolution.setToolTip('The resolution of the variable resolution subtile.  Auto will follow the NOAA specifications guidance,\n' +
                                              'using the depth to resolution lookup table.  This is currently the only option.')
        self.hlayout_variabletile_one.addWidget(self.variabletile_resolution)
        self.surf_layout.addLayout(self.hlayout_variabletile_one)

        self.hlayout_variabletile_two = QtWidgets.QHBoxLayout()
        self.variabletile_subtile_size_lbl = QtWidgets.QLabel('Subtile Size (meters): ')
        self.hlayout_variabletile_two.addWidget(self.variabletile_subtile_size_lbl)
        self.variabletile_subtile_size = QtWidgets.QComboBox()
        self.variabletile_subtile_size.addItems(['512', '256', '128', '64'])
        self.variabletile_subtile_size.setCurrentText('128')
        self.variabletile_subtile_size.setToolTip('The size of the subtile in the variable resolution grid in meters.  The subtile is all the same resolution, so this is the\n' +
                                                  'smallest unit of resolution change.  With a value of 128 meters, each 128x128 tile can be a different resolution.  Make this\n' +
                                                  'larger if you want less change in resolution.  Careful making this too small for deep areas, this size cannot be greater than\n' +
                                                  'your resolution.')
        self.hlayout_variabletile_two.addWidget(self.variabletile_subtile_size)
        self.hlayout_variabletile_two.addStretch()
        self.surf_layout.addLayout(self.hlayout_variabletile_two)

        self.use_dask_checkbox = QtWidgets.QCheckBox('Process in Parallel')
        self.use_dask_checkbox.setToolTip('With this checked, gridding will be done in parallel using the Dask Client.  Assuming you have multiple\n' +
                                          'tiles, this should improve performance significantly.  You may experience some instability, although this\n' +
                                          'current implementation has not shown any during testing.')
        self.surf_layout.addWidget(self.use_dask_checkbox)

        # self.output_msg = QtWidgets.QLabel('Select the output path for the surface')
        # self.surf_layout.addWidget(self.output_msg)

        # self.hlayout_one_two = QtWidgets.QHBoxLayout()
        # self.fil_text = QtWidgets.QLineEdit('', self)
        # self.fil_text.setMinimumWidth(400)
        # self.fil_text.setReadOnly(True)
        # self.hlayout_one_two.addWidget(self.fil_text)
        # self.browse_button = QtWidgets.QPushButton("Browse", self)
        # self.hlayout_one_two.addWidget(self.browse_button)
        # self.surf_layout.addLayout(self.hlayout_one_two)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")

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
        layout.addLayout(self.surf_layout)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False
        self.output_pth = None

        self.grid_type.currentTextChanged.connect(self._event_update_status)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        # self.browse_button.clicked.connect(self.file_browse)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

        self._event_update_status(None)

    def _event_update_status(self, e):
        curr_opts = self.grid_type.currentText()
        if curr_opts == 'Single Resolution':
            self.single_rez_resolution_lbl.show()
            self.single_rez_resolution.show()
            self.single_rez_tile_size.show()
            self.single_rez_tile_size_lbl.show()
            self.variabletile_resolution.hide()
            self.variabletile_resolution_lbl.hide()
            self.variabletile_subtile_size.hide()
            self.variabletile_subtile_size_lbl.hide()
            self.variabletile_tile_size.hide()
            self.variabletile_tile_size_lbl.hide()
        elif curr_opts == 'Variable Resolution Tile':
            self.single_rez_resolution_lbl.hide()
            self.single_rez_resolution.hide()
            self.single_rez_tile_size.hide()
            self.single_rez_tile_size_lbl.hide()
            self.variabletile_resolution.show()
            self.variabletile_resolution_lbl.show()
            self.variabletile_subtile_size.show()
            self.variabletile_subtile_size_lbl.show()
            self.variabletile_tile_size.show()
            self.variabletile_tile_size_lbl.show()

    def _event_update_fqpr_instances(self):
        self.update_fqpr_instances()

    def update_fqpr_instances(self, addtl_files=None):
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]
        if self.fqpr_inst:
            self.output_pth = os.path.dirname(self.fqpr_inst[0])

    def file_browse(self):
        msg, self.output_pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                                     Title='Select output surface path',
                                                                     AppName='kluster', bMulti=False,
                                                                     bSave=True, fFilter='numpy npz (*.npz)')
        if self.output_pth is not None:
            self.fil_text.setText(self.output_pth)

    def return_processing_options(self):
        if not self.canceled:
            curr_opts = self.grid_type.currentText()
            if curr_opts == 'Single Resolution':
                outpth = os.path.join(self.output_pth, 'srgrid_{}_{}'.format(self.surf_method.currentText(),
                                                                             self.single_rez_resolution.currentText()).lower())
                if self.single_rez_resolution.currentText() == 'AUTO':
                    rez = None
                else:
                    rez = float(self.single_rez_resolution.currentText())
                opts = {'fqpr_inst': self.fqpr_inst, 'grid_type': 'single_resolution',
                        'tile_size': float(self.single_rez_tile_size.currentText()),
                        'gridding_algorithm': self.surf_method.currentText().lower(),
                        'resolution': rez, 'output_path': outpth, 'use_dask': self.use_dask_checkbox.isChecked()}
            elif curr_opts == 'Variable Resolution Tile':
                outpth = os.path.join(self.output_pth, 'vrtilegrid_{}'.format(self.surf_method.currentText()).lower())
                if self.variabletile_resolution.currentText() == 'AUTO':
                    rez = None
                else:
                    rez = float(self.variabletile_resolution.currentText())
                opts = {'fqpr_inst': self.fqpr_inst, 'grid_type': 'variable_resolution_tile',
                        'tile_size': float(self.variabletile_tile_size.currentText()),
                        'subtile_size': float(self.variabletile_subtile_size.currentText()),
                        'gridding_algorithm': self.surf_method.currentText().lower(),
                        'resolution': rez, 'output_path': outpth, 'use_dask': self.use_dask_checkbox.isChecked()}
            else:
                raise ValueError('dialog_surface: unexpected grid type {}'.format(curr_opts))
        else:
            opts = None
        return opts

    def start_processing(self):
        if self.output_pth is None:
            self.status_msg.setText('Error: You must insert a surface path to continue')
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
    dlog = SurfaceDialog()
    dlog.show()
    if dlog.exec_():
        print(dlog.return_processing_options())
