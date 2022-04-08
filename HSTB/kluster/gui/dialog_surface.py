import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget, SaveStateDialog
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables
from bathygrid.grid_variables import depth_resolution_lookup

depth_lookup_formatted = 'MAX DEPTH: RESOLUTION\n'
for dval, rval in depth_resolution_lookup.items():
    depth_lookup_formatted += '{}: {}\n'.format(dval, rval)


class SurfaceDialog(SaveStateDialog):
    """
    Dialog for selecting surfacing options that we want to use to generate a new surface.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='surface')

        self.setWindowTitle('Generate New Surface')
        layout = QtWidgets.QVBoxLayout()

        self.basic_surface_group = QtWidgets.QGroupBox('Run surface generation on the following datasets:')
        self.basic_surface_group.setCheckable(True)
        self.basic_surface_group.setChecked(True)
        self.hlayout_zero = QtWidgets.QHBoxLayout()

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.input_fqpr.setMinimumWidth(500)
        self.hlayout_zero.addWidget(self.input_fqpr)
        self.basic_surface_group.setLayout(self.hlayout_zero)

        self.line_surface_checkbox = QtWidgets.QCheckBox('Only selected lines')
        self.line_surface_checkbox.setChecked(False)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.surf_layout = QtWidgets.QVBoxLayout()
        self.surf_msg = QtWidgets.QLabel('Select from the following options:')
        self.surf_layout.addWidget(self.surf_msg)

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.surf_method_lbl = QtWidgets.QLabel('Method: ')
        self.hlayout_one_one.addWidget(self.surf_method_lbl)
        self.surf_method = QtWidgets.QComboBox()
        self.surf_method.addItems(['Mean', 'Shoalest', 'CUBE'])
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
                                  'grids (tile size) that can contain grids of any resolution (that is a power of two).\n' +
                                  "Setting the resolution to auto will allow each tile to determine it's own resolution.")
        self.hlayout_one_one.addWidget(self.grid_type)
        self.surf_layout.addLayout(self.hlayout_one_one)

        self.hlayout_singlerez_one = QtWidgets.QHBoxLayout()
        self.single_rez_tile_size_lbl = QtWidgets.QLabel('Tile Size (meters): ')
        self.hlayout_singlerez_one.addWidget(self.single_rez_tile_size_lbl)
        self.single_rez_tile_size = QtWidgets.QComboBox()
        self.single_rez_tile_size.addItems(['2048', '1024', '512', '256', '128'])
        self.single_rez_tile_size.setCurrentText('128')
        self.single_rez_tile_size.setToolTip('The size of the single resolution tile in meters.  The default size of 1024 meters is a\n' +
                                             'good size for maximizing performance and minimizing memory usage.  Changing this value\n' +
                                             'could result in a much slower computation.  For larger grids, a larger tile size may\n' +
                                             'improve performance, but will require more memory to support the processing.')
        self.hlayout_singlerez_one.addWidget(self.single_rez_tile_size)
        self.hlayout_singlerez_one.addStretch()
        self.single_rez_resolution_lbl = QtWidgets.QLabel('Resolution (meters): ')
        self.hlayout_singlerez_one.addWidget(self.single_rez_resolution_lbl)
        self.single_rez_resolution = QtWidgets.QComboBox()
        self.single_rez_resolution.addItems(['AUTO_depth', 'AUTO_density', '0.25', '0.50', '1.0', '2.0', '4.0', '8.0', '16.0', '32.0', '64.0', '128.0'])
        self.single_rez_resolution.setCurrentText('AUTO_depth')
        self.single_rez_resolution.setToolTip('The resolution of the grid within each single resolution tile in meters.  Higher resolution values allow for a more detailed grid,\n' +
                                              'but will produce holes in the grid if there is not enough data.\n\n' +
                                              'AUTO_depth will follow the NOAA specifications guidance, using the depth in the resolution lookup table:\n\n'
                                              '{}\n'.format(depth_lookup_formatted) +
                                              'AUTO_density will base the resolution on the density/area of each tile using the following formula:\n\n' +
                                              'resolution_estimate=squareroot(2 * minimum_points_per_cell * 1.75 / cell_point_density)')
        self.hlayout_singlerez_one.addWidget(self.single_rez_resolution)
        self.surf_layout.addLayout(self.hlayout_singlerez_one)

        self.hlayout_variabletile_one = QtWidgets.QHBoxLayout()
        self.variabletile_tile_size_lbl = QtWidgets.QLabel('Tile Size (meters): ')
        self.hlayout_variabletile_one.addWidget(self.variabletile_tile_size_lbl)
        self.variabletile_tile_size = QtWidgets.QComboBox()
        self.variabletile_tile_size.addItems(['2048', '1024', '512', '256', '128', '64', '32'])
        self.variabletile_tile_size.setCurrentText('1024')
        self.variabletile_tile_size.setToolTip('The size of the tile in the variable resolution grid in meters.  The tile is all the same resolution, so this is the\n' +
                                               'smallest unit of resolution change.  With a value of 128 meters, each 128x128 tile can be a different resolution.  Make this\n' +
                                               'larger if you want better performance.  The minimum resolution is the tile size.')
        self.hlayout_variabletile_one.addWidget(self.variabletile_tile_size)
        self.variabletile_resolution_lbl = QtWidgets.QLabel('Resolution (meters): ')
        self.hlayout_variabletile_one.addStretch()
        self.hlayout_variabletile_one.addWidget(self.variabletile_resolution_lbl)
        self.variabletile_resolution = QtWidgets.QComboBox()
        self.variabletile_resolution.addItems(['AUTO_depth', 'AUTO_density'])
        self.variabletile_resolution.setCurrentText('AUTO_depth')
        self.variabletile_resolution.setToolTip('The resolution of the grid within each variable resolution tile.\n\n'
                                                'AUTO_depth will follow the NOAA specifications guidance, using the depth in the resolution lookup table:\n\n'
                                                '{}\n'.format(depth_lookup_formatted) +
                                                'AUTO_density will base the resolution on the density/area of each tile using the following formula:\n\n' +
                                                'resolution_estimate=squareroot(2 * minimum_points_per_cell * 1.75 / cell_point_density)')
        self.hlayout_variabletile_one.addWidget(self.variabletile_resolution)
        self.surf_layout.addLayout(self.hlayout_variabletile_one)

        self.hlayout_cube_one = QtWidgets.QHBoxLayout()
        self.cube_method_label = QtWidgets.QLabel('Method: ')
        self.hlayout_cube_one.addWidget(self.cube_method_label)
        self.cube_method_dropdown = QtWidgets.QComboBox()
        self.cube_method_dropdown.addItems(['local', 'posterior', 'prior'])
        self.cube_method_dropdown.setCurrentText('local')
        self.cube_method_dropdown.setToolTip("Method to use in determining the appropriate hypothesis value.\n"
                                             "'local' to use the local spatial context to find the closest node with a single hypothesis and use\n"
                                             "    that hypothesis depth to find the nearest hypothesis in terms of depth in the current node.\n"
                                             "'prior' to use the hypothesis with the most points associated with it.\n"
                                             "'posterior' to combine both prior and local methods to form an approximate Bayesian posterior distribution.")
        self.hlayout_cube_one.addWidget(self.cube_method_dropdown)
        self.hlayout_cube_one.addStretch()

        self.cube_ihoorder_label = QtWidgets.QLabel('IHO Order: ')
        self.hlayout_cube_one.addWidget(self.cube_ihoorder_label)
        self.cube_ihoorder_dropdown = QtWidgets.QComboBox()
        self.cube_ihoorder_dropdown.addItems(['exclusive', 'special', 'order1a', 'order1b', 'order2'])
        self.cube_ihoorder_dropdown.setCurrentText('order1a')
        self.cube_ihoorder_dropdown.setToolTip("Sets the fixed and variable Total Vertical Uncertainty components using the different IHO order categories.\n"
                                               "See S-44 Table 1 to learn more about the Minimum Bathymetry Standards for Safety of Navigation")
        self.hlayout_cube_one.addWidget(self.cube_ihoorder_dropdown)
        self.surf_layout.addLayout(self.hlayout_cube_one)

        self.hlayout_cube_two = QtWidgets.QHBoxLayout()
        self.cube_variance_label = QtWidgets.QLabel('Uncertainty: ')
        self.hlayout_cube_two.addWidget(self.cube_variance_label)
        self.cube_variance_dropdown = QtWidgets.QComboBox()
        self.cube_variance_dropdown.addItems(['CUBE', 'input', 'max'])
        self.cube_variance_dropdown.setCurrentText('CUBE')
        self.cube_variance_dropdown.setToolTip("Controls the reported uncertainty.\n"
                                               "'CUBE' to use CUBE's posterior uncertainty estimate\n"
                                               "'input' to track and use input uncertainty\n"
                                               "'max' to report the greater of the two.")
        self.hlayout_cube_two.addWidget(self.cube_variance_dropdown)
        self.hlayout_cube_two.addStretch()
        self.surf_layout.addLayout(self.hlayout_cube_two)

        self.output_msg = QtWidgets.QLabel('Save to:')

        self.hlayout_output = QtWidgets.QHBoxLayout()
        self.output_text = QtWidgets.QLineEdit('', self)
        self.output_text.setMinimumWidth(400)
        self.output_text.setReadOnly(False)
        self.hlayout_output.addWidget(self.output_text)
        self.output_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_output.addWidget(self.output_button)

        # self.use_dask_checkbox = QtWidgets.QCheckBox('Process in Parallel')
        # self.use_dask_checkbox.setToolTip('With this checked, gridding will be done in parallel using the Dask Client.  Assuming you have multiple\n' +
        #                                   'tiles, this should improve performance significantly.  You may experience some instability, although this\n' +
        #                                   'current implementation has not shown any during testing.')
        # self.surf_layout.addWidget(self.use_dask_checkbox)

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

        layout.addWidget(self.basic_surface_group)
        layout.addWidget(self.line_surface_checkbox)
        layout.addWidget(QtWidgets.QLabel(' '))
        layout.addLayout(self.surf_layout)
        layout.addStretch()
        layout.addWidget(self.output_msg)
        layout.addLayout(self.hlayout_output)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False
        self.output_pth = None

        self.basic_surface_group.toggled.connect(self._handle_basic_checked)
        self.line_surface_checkbox.toggled.connect(self._handle_line_checked)
        self.grid_type.currentTextChanged.connect(self._event_update_status)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.surf_method.currentTextChanged.connect(self._handle_method_changed)
        self.output_button.clicked.connect(self.file_browse)
        self.output_text.textChanged.connect(self._update_output_pth)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

        self.text_controls = [['method', self.surf_method], ['gridtype', self.grid_type], ['cube_method_dropdown', self.cube_method_dropdown],
                              ['cube_variance_dropdown', self.cube_variance_dropdown], ['cube_ihoorder_dropdown', self.cube_ihoorder_dropdown],
                              ['singlerez_tilesize', self.single_rez_tile_size], ['single_rez_resolution', self.single_rez_resolution],
                              ['variabletile_tile_size', self.variabletile_tile_size], ['variabletile_resolution', self.variabletile_resolution]]
        # self.checkbox_controls = [['use_dask_checkbox', self.use_dask_checkbox]]

        self.read_settings()
        self._event_update_status(None)
        self._handle_method_changed(None)

    def _handle_basic_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_surface_checkbox.setChecked(False)

    def _handle_line_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.basic_surface_group.setChecked(False)

    def _event_update_status(self, e):
        curr_opts = self.grid_type.currentText()
        if curr_opts == 'Single Resolution':
            self.single_rez_resolution_lbl.show()
            self.single_rez_resolution.show()
            self.single_rez_tile_size.show()
            self.single_rez_tile_size_lbl.show()
            self.variabletile_resolution.hide()
            self.variabletile_resolution_lbl.hide()
            # self.variabletile_subtile_size.hide()
            # self.variabletile_subtile_size_lbl.hide()
            self.variabletile_tile_size.hide()
            self.variabletile_tile_size_lbl.hide()
        elif curr_opts == 'Variable Resolution Tile':
            self.single_rez_resolution_lbl.hide()
            self.single_rez_resolution.hide()
            self.single_rez_tile_size.hide()
            self.single_rez_tile_size_lbl.hide()
            self.variabletile_resolution.show()
            self.variabletile_resolution_lbl.show()
            # self.variabletile_subtile_size.show()
            # self.variabletile_subtile_size_lbl.show()
            self.variabletile_tile_size.show()
            self.variabletile_tile_size_lbl.show()

    def _handle_method_changed(self, e):
        surf_method = self.surf_method.currentText()
        if surf_method == 'CUBE':
            self.cube_method_dropdown.show()
            self.cube_variance_dropdown.show()
            self.cube_ihoorder_dropdown.show()
            self.cube_method_label.show()
            self.cube_ihoorder_label.show()
            self.cube_variance_label.show()
        else:
            self.cube_method_dropdown.hide()
            self.cube_variance_dropdown.hide()
            self.cube_ihoorder_dropdown.hide()
            self.cube_method_label.hide()
            self.cube_ihoorder_label.hide()
            self.cube_variance_label.hide()

    def _event_update_fqpr_instances(self):
        self.update_fqpr_instances()

    def update_fqpr_instances(self, addtl_files=None):
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]
        if self.fqpr_inst:
            if not self.output_text.text():
                self.output_pth = os.path.dirname(self.fqpr_inst[0])
                curr_opts = self.grid_type.currentText()
                if curr_opts == 'Single Resolution':
                    outpth = os.path.join(self.output_pth, 'srgrid_{}_{}'.format(self.surf_method.currentText(),
                                                                                 self.single_rez_resolution.currentText()).lower())
                elif curr_opts == 'Variable Resolution Tile':
                    outpth = os.path.join(self.output_pth, 'vrtilegrid_{}'.format(self.surf_method.currentText()).lower())
                else:
                    raise NotImplementedError(f'dialog_surface: Unable to autobuild output path from grid type {curr_opts}')
                self.output_pth = outpth
                self.output_text.setText(outpth)

    def file_browse(self):
        msg, output_pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                                Title='Select output surface path',
                                                                AppName='kluster', bMulti=False,
                                                                bSave=True, fFilter='numpy npz (*.npz)')
        if output_pth is not None:
            self.output_text.setText(self.output_pth)
            self.output_pth = output_pth

    def _update_output_pth(self):
        self.output_pth = self.output_text.text()

    def return_processing_options(self):
        if not self.canceled:
            curr_opts = self.grid_type.currentText()
            grid_parameters = {'variance_selection': self.cube_variance_dropdown.currentText().lower(),
                               'iho_order': self.cube_ihoorder_dropdown.currentText().lower(),
                               'method': self.cube_method_dropdown.currentText().lower()}
            if curr_opts == 'Single Resolution':
                if self.single_rez_resolution.currentText() == 'AUTO_depth':
                    rez = None
                    automode = 'depth'
                elif self.single_rez_resolution.currentText() == 'AUTO_density':
                    rez = None
                    automode = 'density'
                else:
                    rez = float(self.single_rez_resolution.currentText())
                    automode = 'depth'
                opts = {'fqpr_inst': self.fqpr_inst, 'grid_type': 'single_resolution',
                        'tile_size': float(self.single_rez_tile_size.currentText()),
                        'gridding_algorithm': self.surf_method.currentText().lower(),
                        'auto_resolution_mode': automode, 'grid_parameters': grid_parameters,
                        'resolution': rez, 'output_path': self.output_pth, 'use_dask': False}
            elif curr_opts == 'Variable Resolution Tile':
                if self.variabletile_resolution.currentText() == 'AUTO_depth':
                    rez = None
                    automode = 'depth'
                elif self.variabletile_resolution.currentText() == 'AUTO_density':
                    rez = None
                    automode = 'density'
                else:
                    raise ValueError('Should not get here, variable rez is only an auto resolution mode operation')

                opts = {'fqpr_inst': self.fqpr_inst, 'grid_type': 'variable_resolution_tile',
                        'tile_size': float(self.variabletile_tile_size.currentText()),
                        # 'subtile_size': float(self.variabletile_subtile_size.currentText()),
                        'subtile_size': float(self.variabletile_tile_size.currentText()),
                        'gridding_algorithm': self.surf_method.currentText().lower(),
                        'auto_resolution_mode': automode, 'grid_parameters': grid_parameters,
                        'resolution': rez, 'output_path': self.output_pth, 'use_dask': False}
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
    dlog = SurfaceDialog()
    dlog.show()
    if dlog.exec_():
        print(dlog.return_processing_options())
