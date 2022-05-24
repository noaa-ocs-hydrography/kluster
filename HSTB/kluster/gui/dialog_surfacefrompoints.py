from pyproj import CRS

from HSTB.kluster.gui.dialog_surface import *


class SurfaceFromPointsDialog(SurfaceDialog):
    """
    Dialog for selecting surfacing options that we want to use to generate a new surface.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent=parent, title=title, settings=settings)

        self.setWindowTitle('Generate Surface From Points')
        self.input_fqpr.setup(mode='file', registry_key='kluster', app_name='pointfilebrowse',
                              supported_file_extension=['.txt', '.csv', '.las', '.laz'], multiselect=True,
                              filebrowse_title='Select points files to import',
                              filebrowse_filter='LAS files,CSV files (*.las;*.laz;*.csv;*.txt)')
        self.basic_surface_group.setTitle('Run surface generation on the following files:')
        self.line_surface_checkbox.hide()

        self.export_options = QtWidgets.QGroupBox('File Options:')
        self.export_options.setCheckable(False)
        self.export_options_top = QtWidgets.QVBoxLayout()

        self.basic_export_layout = QtWidgets.QHBoxLayout()
        self.epsg_label = QtWidgets.QLabel('Input EPSG: ')
        self.basic_export_layout.addWidget(self.epsg_label)
        self.inepsg_val = QtWidgets.QLineEdit('', self)
        self.inepsg_val.setToolTip('The integer EPSG code for these files, expects a UTM projected 2d or 2d Geographic coordinate system.')
        self.basic_export_layout.addWidget(self.inepsg_val)
        self.vertref_label = QtWidgets.QLabel('Vertical Reference: ')
        self.basic_export_layout.addWidget(self.vertref_label)
        self.invertref_val = QtWidgets.QLineEdit('', self)
        self.invertref_val.setToolTip('The vertical reference for the files, ex: "Ellipse" or "MLLW"')
        self.basic_export_layout.addWidget(self.invertref_val)
        self.basic_export_layout.addStretch()
        self.export_options_top.addLayout(self.basic_export_layout)

        self.csv_label = QtWidgets.QLabel('Set the Column Numbers for the CSV File (Starting with "1", XYZ is mandatory)')
        self.export_options_top.addWidget(self.csv_label)

        self.csv_column_layout = QtWidgets.QGridLayout()
        self.x_include = QtWidgets.QCheckBox('X (Eastings/Longitude)')
        self.x_include.setChecked(True)
        self.x_include.setDisabled(True)
        self.csv_column_layout.addWidget(self.x_include, 0, 0)
        self.x_lbl = QtWidgets.QLabel(' Column Number ')
        self.csv_column_layout.addWidget(self.x_lbl, 0, 1)
        self.x_entry = QtWidgets.QSpinBox()
        self.x_entry.setRange(1, 99)
        self.x_entry.setValue(1)
        self.csv_column_layout.addWidget(self.x_entry, 0, 2)
        self.xspacer = QtWidgets.QLabel('')
        self.csv_column_layout.addWidget(self.xspacer, 0, 3)

        self.y_include = QtWidgets.QCheckBox('Y (Northings/Latitude)')
        self.y_include.setChecked(True)
        self.y_include.setDisabled(True)
        self.csv_column_layout.addWidget(self.y_include, 1, 0)
        self.y_lbl = QtWidgets.QLabel(' Column Number ')
        self.csv_column_layout.addWidget(self.y_lbl, 1, 1)
        self.y_entry = QtWidgets.QSpinBox()
        self.y_entry.setRange(1, 99)
        self.y_entry.setValue(2)
        self.csv_column_layout.addWidget(self.y_entry, 1, 2)
        self.yspacer = QtWidgets.QLabel('')
        self.csv_column_layout.addWidget(self.yspacer, 1, 3)

        self.z_include = QtWidgets.QCheckBox('Z (Depth)')
        self.z_include.setChecked(True)
        self.z_include.setDisabled(True)
        self.csv_column_layout.addWidget(self.z_include, 2, 0)
        self.z_lbl = QtWidgets.QLabel(' Column Number ')
        self.csv_column_layout.addWidget(self.z_lbl, 2, 1)
        self.z_entry = QtWidgets.QSpinBox()
        self.z_entry.setRange(1, 99)
        self.z_entry.setValue(3)
        self.csv_column_layout.addWidget(self.z_entry, 2, 2)
        self.zspacer = QtWidgets.QLabel('')
        self.csv_column_layout.addWidget(self.zspacer, 2, 3)

        self.thu_include = QtWidgets.QCheckBox('THU (Horizontal Uncertainty)')
        self.thu_include.setChecked(False)
        self.thu_include.setDisabled(False)
        self.csv_column_layout.addWidget(self.thu_include, 3, 0)
        self.thu_lbl = QtWidgets.QLabel(' Column Number ')
        self.csv_column_layout.addWidget(self.thu_lbl, 3, 1)
        self.thu_entry = QtWidgets.QSpinBox()
        self.thu_entry.setRange(1, 99)
        self.thu_entry.setValue(4)
        self.csv_column_layout.addWidget(self.thu_entry, 3, 2)
        self.thuspacer = QtWidgets.QLabel('')
        self.csv_column_layout.addWidget(self.thuspacer, 3, 3)

        self.tvu_include = QtWidgets.QCheckBox('TVU (Vertical Uncertainty)')
        self.tvu_include.setChecked(False)
        self.tvu_include.setDisabled(False)
        self.csv_column_layout.addWidget(self.tvu_include, 4, 0)
        self.tvu_lbl = QtWidgets.QLabel(' Column Number ')
        self.csv_column_layout.addWidget(self.tvu_lbl, 4, 1)
        self.tvu_entry = QtWidgets.QSpinBox()
        self.tvu_entry.setRange(1, 99)
        self.tvu_entry.setValue(5)
        self.csv_column_layout.addWidget(self.tvu_entry, 4, 2)
        self.tvuspacer = QtWidgets.QLabel('')
        self.csv_column_layout.addWidget(self.tvuspacer, 4, 3)

        self.export_options_top.addLayout(self.csv_column_layout)

        self.export_options.setLayout(self.export_options_top)

        self.toplayout.insertWidget(1, self.export_options)

        self.new_grid_checkbox = QtWidgets.QCheckBox('Create new surface')
        self.new_grid_checkbox.setChecked(True)
        self.toplayout.insertWidget(4, self.new_grid_checkbox)

        self.append_grid_checkbox = QtWidgets.QCheckBox('Add to existing surface')
        self.append_grid_checkbox.setChecked(False)
        self.toplayout.insertWidget(7, self.append_grid_checkbox)

        self.append_options = QtWidgets.QGroupBox('Select from the following options:')
        self.append_options.setCheckable(False)
        self.append_layout = QtWidgets.QVBoxLayout()

        self.append_output_msg = QtWidgets.QLabel('Add to:')
        self.append_layout.addWidget(self.append_output_msg)

        self.hlayout_append = QtWidgets.QHBoxLayout()
        self.append_output_text = QtWidgets.QLineEdit('', self)
        self.append_output_text.setMinimumWidth(400)
        self.append_output_text.setReadOnly(False)
        self.hlayout_append.addWidget(self.append_output_text)
        self.append_output_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_append.addWidget(self.append_output_button)
        self.append_layout.addLayout(self.hlayout_append)

        self.append_options.setLayout(self.append_layout)

        self.toplayout.insertWidget(8, self.append_options)

        self.append_pth = None
        self.append_path_edited = False

        self.append_grid_checkbox.toggled.connect(self.handle_append_checked)
        self.new_grid_checkbox.toggled.connect(self.handle_create_checked)

        self.append_output_button.clicked.connect(self.append_browse)
        self.append_output_text.textChanged.connect(self._update_append_pth)

        self._filetype = ''
        self.text_controls += [['inepsg_val', self.inepsg_val], ['invertref_val', self.invertref_val],
                               ['x_entry', self.x_entry], ['y_entry', self.y_entry], ['z_entry', self.z_entry],
                               ['thu_entry', self.thu_entry], ['tvu_entry', self.tvu_entry], ['append_output_text', self.append_output_text]]
        self.checkbox_controls += [['thu_include', self.thu_include], ['tvu_include', self.tvu_include],
                                   ['append_grid_checkbox', self.append_grid_checkbox], ['new_grid_checkbox', self.new_grid_checkbox]]
        self.read_settings()
        self.handle_append_checked(self.append_grid_checkbox.isChecked())
        self.handle_create_checked(self.new_grid_checkbox.isChecked())

    @property
    def filetype(self):
        return self._filetype

    @filetype.setter
    def filetype(self, newtype: str):
        self._filetype = newtype
        if newtype in ['.csv', '.txt']:
            self.csv_label.show()
            self.x_include.show()
            self.x_lbl.show()
            self.x_entry.show()
            self.xspacer.show()
            self.y_include.show()
            self.y_lbl.show()
            self.y_entry.show()
            self.yspacer.show()
            self.z_include.show()
            self.z_lbl.show()
            self.z_entry.show()
            self.zspacer.show()
            self.thu_include.show()
            self.thu_lbl.show()
            self.thu_entry.show()
            self.thuspacer.show()
            self.tvu_include.show()
            self.tvu_lbl.show()
            self.tvu_entry.show()
            self.tvuspacer.show()
        else:
            self.csv_label.hide()
            self.x_include.hide()
            self.x_lbl.hide()
            self.x_entry.hide()
            self.xspacer.hide()
            self.y_include.hide()
            self.y_lbl.hide()
            self.y_entry.hide()
            self.yspacer.hide()
            self.z_include.hide()
            self.z_lbl.hide()
            self.z_entry.hide()
            self.zspacer.hide()
            self.thu_include.hide()
            self.thu_lbl.hide()
            self.thu_entry.hide()
            self.thuspacer.hide()
            self.tvu_include.hide()
            self.tvu_lbl.hide()
            self.tvu_entry.hide()
            self.tvuspacer.hide()

    def append_browse(self):
        msg, output_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster', Title='Select output surface path',
                                                           AppName='kluster')
        if output_pth is not None:
            self.append_pth = output_pth
            self.append_output_text.setText(self.append_pth)
            self.append_path_edited = True

    def _update_append_pth(self):
        self.append_pth = self.append_output_text.text()
        self.append_path_edited = True

    def handle_create_checked(self, e):
        if e:
            self.inepsg_val.setDisabled(False)
            self.invertref_val.setDisabled(False)
            self.epsg_label.setDisabled(False)
            self.vertref_label.setDisabled(False)
            self.surf_options.setDisabled(False)
            self.append_options.setDisabled(True)
            self.append_grid_checkbox.setChecked(False)

    def handle_append_checked(self, e):
        if e:
            self.inepsg_val.setDisabled(True)
            self.invertref_val.setDisabled(True)
            self.epsg_label.setDisabled(True)
            self.vertref_label.setDisabled(True)
            self.surf_options.setDisabled(True)
            self.append_options.setDisabled(False)
            self.new_grid_checkbox.setChecked(False)

    def _event_update_fqpr_instances(self):
        self.update_fqpr_instances()
        if self.fqpr_inst:
            self.filetype = os.path.splitext(self.fqpr_inst[0])[1].lower()

    def return_processing_options(self):
        opts = super().return_processing_options()
        if opts is not None:
            opts['allow_append'] = self.append_grid_checkbox.isChecked()
            if opts['allow_append']:
                opts['output_path'] = self.append_pth
            opts['horizontal_epsg'] = int(self.inepsg_val.text())
            opts['vertical_reference'] = str(self.invertref_val.text())

            csv_columns = []
            cvals = [self.x_entry.value(), self.y_entry.value(), self.z_entry.value(), self.thu_entry.value(), self.tvu_entry.value()]
            clbls = ['x', 'y', 'z', 'thu', 'tvu']
            cenabled = [self.x_include.isChecked(), self.y_include.isChecked(), self.z_include.isChecked(), self.thu_include.isChecked(),
                        self.tvu_include.isChecked()]
            cvals = [cv for cnt, cv in enumerate(cvals) if cenabled[cnt]]
            clbls = [cv for cnt, cv in enumerate(clbls) if cenabled[cnt]]
            for i in range(max(cvals)):
                try:
                    varindex = cvals.index(i + 1)
                    csv_columns.append(clbls[varindex])
                except ValueError:
                    csv_columns.append('')
            opts['csv_columns'] = tuple(csv_columns)
        return opts

    def validate_epsg(self):
        epsg = ''
        try:
            epsg = int(self.inepsg_val.text())
            ecrs = CRS.from_epsg(epsg)
            return True
        except:
            self.status_msg.setText(f'Unknown Error: Unable to generate new CRS from Input EPSG:{epsg}')
            return False

    def start_processing(self):
        if (self.output_pth is None and self.new_grid_checkbox.isChecked()) or (self.append_pth is None and self.append_grid_checkbox.isChecked()):
            self.status_msg.setText('Error: You must insert an output path to continue')
        elif not self.fqpr_inst:
            self.status_msg.setText('Error: You must provide at least one file in the box above')
        elif not self.append_grid_checkbox.isChecked() and not self.new_grid_checkbox.isChecked():
            self.status_msg.setText('Error: You must select either Create New Surface or Add to Existing Surface to proceed.')
        elif not self.validate_epsg():
            pass
        elif not self.invertref_val.text():
            self.status_msg.setText('Error: You must provide a vertical reference string for these files')
        elif self.surf_method.currentText() == 'CUBE' and (self.filetype in ['.csv', '.txt'] and (not self.tvu_include.isChecked() or not self.thu_include.isChecked())) or (self.filetype in ['.las', '.laz']):
            self.status_msg.setText('Error: Uncertainty must be provided to run the CUBE algorithm (not supported for las data)')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = SurfaceFromPointsDialog()
    dlog.show()
    if dlog.exec_() and not dlog.canceled:
        print(dlog.return_processing_options())
