from pyproj import CRS

from HSTB.kluster.gui.dialog_surface import *


class MosaicDialog(SurfaceDialog):
    """
    Dialog for selecting surfacing options that we want to use to generate a new surface.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent=parent, title=title, settings=settings)

        # first hide the non-mosaic related options
        self.setWindowTitle('Generate New Mosaic')
        self.basic_surface_group.setTitle('Run mosaic generation on the following files:')
        self.surf_method.clear()
        self.surf_method.addItems(['Mean'])
        self.grid_type.hide()
        self.grid_type_lbl.hide()
        self.single_rez_resolution.clear()
        self.single_rez_resolution.addItems(['0.25', '0.50', '1.0', '2.0', '4.0', '8.0', '16.0', '32.0', '64.0', '128.0'])

        self.surf_method.setToolTip('The algorithm used when gridding, will use this to determine the intensity value of the cell')
        self.single_rez_resolution.setToolTip('The resolution of the grid within each single resolution tile in meters.  Higher resolution values allow for a more detailed grid,\n' +
                                              'but will produce holes in the grid if there is not enough data.')

        self.surf_options.setTitle('Create Mosaic')
        self.surf_options.setCheckable(True)

        self.backscatter_groupbox = QtWidgets.QGroupBox('Process Backscatter')
        self.backscatter_groupbox.setCheckable(True)
        self.backscatter_layout = QtWidgets.QVBoxLayout()

        self.bscatter_hbox1 = QtWidgets.QHBoxLayout()
        self.fgcorrect = QtWidgets.QCheckBox('Remove Fixed Gain')
        self.fgcorrect.setChecked(True)
        self.bscatter_hbox1.addWidget(self.fgcorrect)
        self.tvgcorrect = QtWidgets.QCheckBox('Remove TVG')
        self.tvgcorrect.setChecked(True)
        self.bscatter_hbox1.addWidget(self.tvgcorrect)
        self.backscatter_layout.addLayout(self.bscatter_hbox1)

        self.bscatter_hbox2 = QtWidgets.QHBoxLayout()
        self.tlcorrect = QtWidgets.QCheckBox('Transmission Loss Correct')
        self.tlcorrect.setChecked(True)
        self.bscatter_hbox2.addWidget(self.tlcorrect)
        self.areacorrect = QtWidgets.QCheckBox('Area Correct')
        self.areacorrect.setChecked(True)
        self.bscatter_hbox2.addWidget(self.areacorrect)
        self.backscatter_layout.addLayout(self.bscatter_hbox2)

        self.backscatter_groupbox.setLayout(self.backscatter_layout)
        self.toplayout.insertWidget(2, self.backscatter_groupbox)
        self.toplayout.insertWidget(2, QtWidgets.QLabel(''))

        self.avg_groupbox = QtWidgets.QGroupBox('AVG Corrector')
        self.avg_groupbox.setCheckable(True)
        self.avg_layout = QtWidgets.QVBoxLayout()

        self.avg_useexist = QtWidgets.QCheckBox('Use Existing')
        self.avg_layout.addWidget(self.avg_useexist)

        validator = QtGui.QDoubleValidator(0, 90, 3)
        self.avg_horiz_one = QtWidgets.QHBoxLayout()
        self.binsize_lbl = QtWidgets.QLabel('Bin Size (deg)')
        self.avg_horiz_one.addWidget(self.binsize_lbl)
        self.binsize = QtWidgets.QLineEdit('1.0')
        self.binsize.setValidator(validator)
        self.avg_horiz_one.addWidget(self.binsize)
        self.refangle_lbl = QtWidgets.QLabel('Reference Angle (deg)')
        self.avg_horiz_one.addWidget(self.refangle_lbl)
        self.refangle = QtWidgets.QLineEdit('45.0')
        self.refangle.setValidator(validator)
        self.avg_horiz_one.addWidget(self.refangle)
        self.avg_layout.addLayout(self.avg_horiz_one)

        self.avg_groupbox.setLayout(self.avg_layout)
        self.toplayout.insertWidget(4, self.avg_groupbox)
        self.toplayout.insertWidget(4, QtWidgets.QLabel(''))

        self.basesrgrid_name = 'mosaic'

        self.avg_useexist.toggled.connect(self.handle_avg_use_existing)

        self.text_controls += [['binsize', self.binsize], ['refangle', self.refangle]]
        self.checkbox_controls += [['backscatter_groupbox', self.backscatter_groupbox], ['fgcorrect', self.fgcorrect],
                                   ['tvgcorrect', self.tvgcorrect], ['tlcorrect', self.tlcorrect], ['areacorrect', self.areacorrect],
                                   ['avg_groupbox', self.avg_groupbox], ['avg_useexist', self.avg_useexist], ['surf_options', self.surf_options]]

        self.read_settings()

        self.handle_avg_use_existing(self.avg_useexist.isChecked())

    def handle_avg_use_existing(self, e):
        if e:
            self.binsize_lbl.setDisabled(True)
            self.binsize.setDisabled(True)
            self.refangle_lbl.setDisabled(True)
            self.refangle.setDisabled(True)
        else:
            self.binsize_lbl.setDisabled(False)
            self.binsize.setDisabled(False)
            self.refangle_lbl.setDisabled(False)
            self.refangle.setDisabled(False)

    def return_processing_options(self):
        if not self.canceled:
            bsize = float(self.binsize.text())
            refangle = float(self.refangle.text())
            rez = float(self.single_rez_resolution.currentText())
            opts = {'fqpr_inst': self.fqpr_inst, 'tile_size': float(self.single_rez_tile_size.currentText()),
                    'gridding_algorithm': self.surf_method.currentText().lower(), 'resolution': rez,
                    'process_backscatter': self.backscatter_groupbox.isChecked(), 'create_mosaic': self.surf_options.isChecked(),
                    'angle_varying_gain': self.avg_groupbox.isChecked(), 'avg_angle': refangle, 'avg_line': None,
                    'avg_bin_size': bsize, 'overwrite_existing_avg': not self.avg_useexist.isChecked(),
                    'process_backscatter_fixed_gain_corrected': self.fgcorrect.isChecked(),
                    'process_backscatter_tvg_corrected': self.tvgcorrect.isChecked(),
                    'process_backscatter_transmission_loss_corrected': self.tlcorrect.isChecked(),
                    'process_backscatter_area_corrected': self.areacorrect.isChecked(), 'output_path': self.output_pth,
                    'use_dask': False}
        else:
            opts = None
        return opts

    def start_processing(self):
        if self.avg_groupbox.isChecked():
            try:
                tst = float(self.binsize.text())
                assert 0 < tst <= 90
                bsizechk = True
            except:
                bsizechk = False
            try:
                tst = float(self.refangle.text())
                assert 0 <= tst <= 90
                refanglechk = True
            except:
                refanglechk = False
        else:
            bsizechk = True
            refanglechk = True

        if self.output_pth is None:
            self.status_msg.setText('Error: You must insert a mosaic path to continue')
        elif not self.line_surface_checkbox.isChecked() and not self.basic_surface_group.isChecked():
            self.status_msg.setText('Error: You must either check "Run Mosaic Generation..." or "Only Selected Lines"')
        elif not self.backscatter_groupbox.isChecked() and not self.avg_groupbox.isChecked() and not self.surf_options.isChecked():
            self.status_msg.setText('Error: You must check one of "Process Backscatter", "AVG Corrector", "Create Mosaic"')
        elif not bsizechk:
            self.status_msg.setText('Error: "Bin Size" must be a valid number between 0.0 and 90.0')
        elif not refanglechk:
            self.status_msg.setText('Error: "Reference Angle" must be a valid number between 0.0 and 90.0')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = MosaicDialog()
    dlog.show()
    if dlog.exec_() and not dlog.canceled:
        print(dlog.return_processing_options())
