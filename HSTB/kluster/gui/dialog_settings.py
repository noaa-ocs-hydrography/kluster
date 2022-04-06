import os
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled
if qgis_enabled:
    os.environ['PYDRO_GUI_FORCE_PYQT'] = 'True'
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables
from HSTB.kluster.modules.georeference import set_vyperdatum_vdatum_path, clear_vdatum_path


class SettingsDialog(SaveStateDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='settings')

        self.setWindowTitle('Settings')
        layout = QtWidgets.QVBoxLayout()
        self.tabwidget = QtWidgets.QTabWidget()

        self.general_tab = QtWidgets.QWidget()
        self.general_layout = QtWidgets.QVBoxLayout()

        self.parallel_write = QtWidgets.QCheckBox('Enable Parallel Writes')
        self.parallel_write.setChecked(True)
        self.parallel_write.setToolTip('If checked, Kluster will write to the hard drive in parallel, disabling this ' +
                                       'is a useful step in troubleshooting PermissionErrors.')

        self.keep_waterline_changes = QtWidgets.QCheckBox('Retain Waterline Changes')
        self.keep_waterline_changes.setChecked(True)
        self.keep_waterline_changes.setToolTip('If checked (only applicable if you are using a Vessel File), Kluster will save all ' +
                                               'waterline changes in later multibeam files to the vessel file.  \nUncheck this if you ' +
                                               'do not want changes in waterline to be new entries in the vessel file.')

        self.force_coordinate_match = QtWidgets.QCheckBox('Force all days to have the same Coordinate System')
        self.force_coordinate_match.setChecked(True)
        self.force_coordinate_match.setToolTip('By default, Kluster will assign an automatic UTM zone number to each day of data.  If you ' +
                                               'have data that crosses UTM zones, you might find that a project \ncontains data with ' +
                                               'different coordinate systems.  Check this box if you want to force all days in a project ' +
                                               '\nto have the same coordinate system by using the most prevalent coordinate system in the Project Tree list.  use_epsg in project ' +
                                               'settings will ignore this.')

        self.gen_hlayout_one_one = QtWidgets.QHBoxLayout()
        self.auto_processing_mode_label = QtWidgets.QLabel('Process Mode: ')
        self.gen_hlayout_one_one.addWidget(self.auto_processing_mode_label)
        self.auto_processing_mode = QtWidgets.QComboBox()
        autooptions = ['normal', 'convert_only', 'concatenate']
        self.auto_processing_mode.addItems(autooptions)
        self.auto_processing_mode.setToolTip('Controls the processing actions that appear when new data is added or settings are changed.\n' +
                                             'See the following mode explanations for the currently available options\n\n' +
                                             'normal = data is converted and processed as it comes in, where each line added would reprocess the whole day\n' +
                                             'convert only = data is only converted, data is never automatically processed\n' +
                                             'concatenate = data is converted as lines are added and each line is processed individually.  Similar to normal\n' +
                                             '  mode but more efficient if you are adding lines as they are acquired, normal mode would do a full reprocess of\n' +
                                             '  the day after each new line is added')
        self.gen_hlayout_one_one.addWidget(self.auto_processing_mode)
        self.gen_hlayout_one_one.addStretch()

        self.gen_hlayout_one = QtWidgets.QHBoxLayout()
        self.vdatum_label = QtWidgets.QLabel('VDatum Directory')
        self.gen_hlayout_one.addWidget(self.vdatum_label)
        self.vdatum_text = QtWidgets.QLineEdit('', self)
        self.vdatum_text.setToolTip('Optional, this is required if you are using the "NOAA MLLW" or "NOAA MHW" vertical reference options.')
        self.gen_hlayout_one.addWidget(self.vdatum_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.gen_hlayout_one.addWidget(self.browse_button)

        self.gen_hlayout_two = QtWidgets.QHBoxLayout()
        self.filter_label = QtWidgets.QLabel('External Filter Directory')
        self.gen_hlayout_two.addWidget(self.filter_label)
        self.filter_text = QtWidgets.QLineEdit('', self)
        self.filter_text.setToolTip('Optional, set if you have a directory of custom Kluster filter .py files that you would like to include.')
        self.gen_hlayout_two.addWidget(self.filter_text)
        self.browse_filter_button = QtWidgets.QPushButton("Browse", self)
        self.gen_hlayout_two.addWidget(self.browse_filter_button)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.hlayout_five.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_five.addWidget(self.ok_button)
        self.hlayout_five.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_five.addWidget(self.cancel_button)
        self.hlayout_five.addStretch(1)
        self.default_button = QtWidgets.QPushButton('Reset Tab', self)
        self.hlayout_five.addWidget(self.default_button)
        self.hlayout_five.addStretch(1)

        self.general_layout.addWidget(self.parallel_write)
        self.general_layout.addWidget(self.keep_waterline_changes)
        self.general_layout.addWidget(self.force_coordinate_match)
        self.general_layout.addLayout(self.gen_hlayout_one_one)
        self.general_layout.addLayout(self.gen_hlayout_one)
        self.general_layout.addLayout(self.gen_hlayout_two)
        self.general_layout.addWidget(self.status_msg)
        self.general_layout.addStretch()

        self.general_tab.setLayout(self.general_layout)
        self.tabwidget.addTab(self.general_tab, 'General')

        self.display_tab = QtWidgets.QWidget()
        self.display_layout = QtWidgets.QVBoxLayout()

        # yes I know about color pickers, yes I know this is a dumb way to do this, get off my back already
        possible_colors = ['black', 'white', 'red', 'magenta', 'purple', 'blue', 'cyan', 'pink', 'salmon', 'peru',
                           'orange', 'yellow', 'light green', 'green', 'teal']
        colorone = QtWidgets.QHBoxLayout()
        self.kvar_pass_color_lbl = QtWidgets.QLabel('Pass Color')
        self.kvar_pass_color = QtWidgets.QComboBox()
        self.kvar_pass_color.addItems(possible_colors)
        self.kvar_pass_color.setCurrentText(kluster_variables.pass_color)
        self.kvar_pass_color.setToolTip('Color of the graphical labels and text where a test passes')

        self.kvar_error_color_lbl = QtWidgets.QLabel('Error Color')
        self.kvar_error_color = QtWidgets.QComboBox()
        self.kvar_error_color.addItems(possible_colors)
        self.kvar_error_color.setCurrentText(kluster_variables.error_color)
        self.kvar_error_color.setToolTip('Color of the graphical labels and text where a test fails')

        self.kvar_warning_color_lbl = QtWidgets.QLabel('Warning Color')
        self.kvar_warning_color = QtWidgets.QComboBox()
        self.kvar_warning_color.addItems(possible_colors)
        self.kvar_warning_color.setCurrentText(kluster_variables.warning_color)
        self.kvar_warning_color.setToolTip('Color of the graphical labels and text where a warning is raised')

        colorone.addWidget(self.kvar_pass_color_lbl)
        colorone.addWidget(self.kvar_pass_color)
        colorone.addWidget(self.kvar_error_color_lbl)
        colorone.addWidget(self.kvar_error_color)
        colorone.addWidget(self.kvar_warning_color_lbl)
        colorone.addWidget(self.kvar_warning_color)

        colortwo = QtWidgets.QHBoxLayout()
        self.kvar_amplitude_color_lbl = QtWidgets.QLabel('Amplitude Color')
        self.kvar_amplitude_color = QtWidgets.QComboBox()
        self.kvar_amplitude_color.addItems(possible_colors)
        self.kvar_amplitude_color.setCurrentText(kluster_variables.amplitude_color)
        self.kvar_amplitude_color.setToolTip('Color of the Amplitude status points in Points View')

        self.kvar_phase_color_lbl = QtWidgets.QLabel('Phase Color')
        self.kvar_phase_color = QtWidgets.QComboBox()
        self.kvar_phase_color.addItems(possible_colors)
        self.kvar_phase_color.setCurrentText(kluster_variables.phase_color)
        self.kvar_phase_color.setToolTip('Color of the Phase status points in Points View')

        self.kvar_reject_color_lbl = QtWidgets.QLabel('Reject Color')
        self.kvar_reject_color = QtWidgets.QComboBox()
        self.kvar_reject_color.addItems(possible_colors)
        self.kvar_reject_color.setCurrentText(kluster_variables.reject_color)
        self.kvar_reject_color.setToolTip('Color of the Reject status points in Points View')

        self.kvar_reaccept_color_lbl = QtWidgets.QLabel('Reaccept Color')
        self.kvar_reaccept_color = QtWidgets.QComboBox()
        self.kvar_reaccept_color.addItems(possible_colors)
        self.kvar_reaccept_color.setCurrentText(kluster_variables.reaccept_color)
        self.kvar_reaccept_color.setToolTip('Color of the Reaccept status points in Points View')

        colortwo.addWidget(self.kvar_amplitude_color_lbl)
        colortwo.addWidget(self.kvar_amplitude_color)
        colortwo.addWidget(self.kvar_phase_color_lbl)
        colortwo.addWidget(self.kvar_phase_color)
        colortwo.addWidget(self.kvar_reject_color_lbl)
        colortwo.addWidget(self.kvar_reject_color)
        colortwo.addWidget(self.kvar_reaccept_color_lbl)
        colortwo.addWidget(self.kvar_reaccept_color)

        self.display_layout.addLayout(colorone)
        self.display_layout.addLayout(colortwo)
        self.display_layout.addStretch()

        self.display_tab.setLayout(self.display_layout)
        self.tabwidget.addTab(self.display_tab, 'Display')

        self.processing_tab = QtWidgets.QWidget()
        self.processing_layout = QtWidgets.QVBoxLayout()

        processingone = QtWidgets.QHBoxLayout()
        self.kvar_convfiles_label = QtWidgets.QLabel('Files converted at once')
        self.kvar_convfiles = QtWidgets.QSpinBox()
        self.kvar_convfiles.setRange(1, 999)
        self.kvar_convfiles.setValue(kluster_variables.converted_files_at_once)
        self.kvar_convfiles.setToolTip('Conversion will convert this many files at once, raising this value can create memory issues in Kluster')
        processingone.addWidget(self.kvar_convfiles_label)
        processingone.addWidget(self.kvar_convfiles)

        processingtwo = QtWidgets.QHBoxLayout()
        self.kvar_pingslas_label = QtWidgets.QLabel('Pings per LAS File')
        self.kvar_pingslas = QtWidgets.QSpinBox()
        self.kvar_pingslas.setRange(1, 500000)
        self.kvar_pingslas.setValue(kluster_variables.pings_per_las)
        self.kvar_pingslas.setToolTip('LAS export will put this many pings in one file before starting a new file, raising this value can create overly large files')
        processingtwo.addWidget(self.kvar_pingslas_label)
        processingtwo.addWidget(self.kvar_pingslas)

        processingthree = QtWidgets.QHBoxLayout()
        self.kvar_pingscsv_label = QtWidgets.QLabel('Pings per CSV File')
        self.kvar_pingscsv = QtWidgets.QSpinBox()
        self.kvar_pingscsv.setRange(1, 500000)
        self.kvar_pingscsv.setValue(kluster_variables.pings_per_csv)
        self.kvar_pingscsv.setToolTip('CSV export will put this many pings in one file before starting a new file, raising this value can create overly large files')
        processingthree.addWidget(self.kvar_pingscsv_label)
        processingthree.addWidget(self.kvar_pingscsv)

        self.processing_layout.addLayout(processingone)
        self.processing_layout.addLayout(processingtwo)
        self.processing_layout.addLayout(processingthree)
        self.processing_layout.addStretch()

        self.processing_tab.setLayout(self.processing_layout)
        self.tabwidget.addTab(self.processing_tab, 'Processing')

        self.uncertainty_tab = QtWidgets.QWidget()
        self.uncertainty_layout = QtWidgets.QVBoxLayout()
        validator = QtGui.QDoubleValidator(-999, 999, 3)

        uncertaintyone = QtWidgets.QHBoxLayout()
        self.kvar_heaveerror_label = QtWidgets.QLabel('Default Heave Error (meters)')
        self.kvar_heaveerror = QtWidgets.QLineEdit('')
        self.kvar_heaveerror.setValidator(validator)
        self.kvar_heaveerror.setText(str(kluster_variables.default_heave_error))
        self.kvar_heaveerror.editingFinished.connect(self.validate_numctrl)
        self.kvar_heaveerror.setToolTip('Default 1 sigma standard deviation in the heave sensor, generally found in manufacturer specifications.')
        self.kvar_rollerror_label = QtWidgets.QLabel('Default Roll Sensor Error (meters)')
        self.kvar_rollerror = QtWidgets.QLineEdit('')
        self.kvar_rollerror.setValidator(validator)
        self.kvar_rollerror.setText(str(kluster_variables.default_roll_sensor_error))
        self.kvar_rollerror.editingFinished.connect(self.validate_numctrl)
        self.kvar_rollerror.setToolTip('Default 1 sigma standard deviation in the roll sensor, generally found in manufacturer specifications.')
        uncertaintyone.addWidget(self.kvar_heaveerror_label)
        uncertaintyone.addWidget(self.kvar_heaveerror)
        uncertaintyone.addWidget(self.kvar_rollerror_label)
        uncertaintyone.addWidget(self.kvar_rollerror)

        uncertaintytwo = QtWidgets.QHBoxLayout()
        self.kvar_pitcherror_label = QtWidgets.QLabel('Default Pitch Sensor Error (meters)')
        self.kvar_pitcherror = QtWidgets.QLineEdit('')
        self.kvar_pitcherror.setValidator(validator)
        self.kvar_pitcherror.setText(str(kluster_variables.default_pitch_sensor_error))
        self.kvar_pitcherror.editingFinished.connect(self.validate_numctrl)
        self.kvar_pitcherror.setToolTip('Default 1 sigma standard deviation in the pitch sensor, generally found in manufacturer specifications')
        self.kvar_yawerror_label = QtWidgets.QLabel('Default Yaw Sensor Error (meters)')
        self.kvar_yawerror = QtWidgets.QLineEdit('')
        self.kvar_yawerror.setValidator(validator)
        self.kvar_yawerror.setText(str(kluster_variables.default_heading_sensor_error))
        self.kvar_yawerror.editingFinished.connect(self.validate_numctrl)
        self.kvar_yawerror.setToolTip('Default 1 sigma standard deviation in the heading sensor, generally found in manufacturer specifications')
        uncertaintytwo.addWidget(self.kvar_pitcherror_label)
        uncertaintytwo.addWidget(self.kvar_pitcherror)
        uncertaintytwo.addWidget(self.kvar_yawerror_label)
        uncertaintytwo.addWidget(self.kvar_yawerror)

        uncertaintythree = QtWidgets.QHBoxLayout()
        self.kvar_beamangle_label = QtWidgets.QLabel('Default Beam Opening Angle (degrees)')
        self.kvar_beamangle = QtWidgets.QLineEdit('')
        self.kvar_beamangle.setValidator(validator)
        self.kvar_beamangle.setText(str(kluster_variables.default_beam_opening_angle))
        self.kvar_beamangle.editingFinished.connect(self.validate_numctrl)
        self.kvar_beamangle.setToolTip('Default Receiver beam opening angle, should auto populate from the multibeam data, this value is used otherwise')
        self.kvar_sverror_label = QtWidgets.QLabel('Default Surface SV Error (meters/second)')
        self.kvar_sverror = QtWidgets.QLineEdit('')
        self.kvar_sverror.setValidator(validator)
        self.kvar_sverror.setText(str(kluster_variables.default_surface_sv_error))
        self.kvar_sverror.editingFinished.connect(self.validate_numctrl)
        self.kvar_sverror.setToolTip('Default 1 sigma standard deviation in surface sv sensor, generally found in manufacturer specifications')
        uncertaintythree.addWidget(self.kvar_beamangle_label)
        uncertaintythree.addWidget(self.kvar_beamangle)
        uncertaintythree.addWidget(self.kvar_sverror_label)
        uncertaintythree.addWidget(self.kvar_sverror)

        uncertaintyfour = QtWidgets.QHBoxLayout()
        self.kvar_rollpatch_label = QtWidgets.QLabel('Default Roll Patch Error (degrees)')
        self.kvar_rollpatch = QtWidgets.QLineEdit('')
        self.kvar_rollpatch.setValidator(validator)
        self.kvar_rollpatch.setText(str(kluster_variables.default_roll_patch_error))
        self.kvar_rollpatch.editingFinished.connect(self.validate_numctrl)
        self.kvar_rollpatch.setToolTip('Default 1 sigma standard deviation in your roll angle patch test procedure')
        self.kvar_waterline_label = QtWidgets.QLabel('Default Waterline (meters)')
        self.kvar_waterline = QtWidgets.QLineEdit('')
        self.kvar_waterline.setValidator(validator)
        self.kvar_waterline.setText(str(kluster_variables.default_waterline_error))
        self.kvar_waterline.editingFinished.connect(self.validate_numctrl)
        self.kvar_waterline.setToolTip('Default 1 sigma standard deviation of the waterline measurement, only used for waterline vertical reference')
        uncertaintyfour.addWidget(self.kvar_rollpatch_label)
        uncertaintyfour.addWidget(self.kvar_rollpatch)
        uncertaintyfour.addWidget(self.kvar_waterline_label)
        uncertaintyfour.addWidget(self.kvar_waterline)

        uncertaintyfive = QtWidgets.QHBoxLayout()
        self.kvar_horizontalerror_label = QtWidgets.QLabel('Default Horizontal Positioning Error (meters)')
        self.kvar_horizontalerror = QtWidgets.QLineEdit('')
        self.kvar_horizontalerror.setValidator(validator)
        self.kvar_horizontalerror.setText(str(kluster_variables.default_horizontal_positioning_error))
        self.kvar_horizontalerror.editingFinished.connect(self.validate_numctrl)
        self.kvar_horizontalerror.setToolTip('Default 1 sigma standard deviation of the horizontal positioning system, only used if SBET is not provided')
        self.kvar_verticalerror_label = QtWidgets.QLabel('Default Vertical Positioning Error (meters)')
        self.kvar_verticalerror = QtWidgets.QLineEdit('')
        self.kvar_verticalerror.setValidator(validator)
        self.kvar_verticalerror.setText(str(kluster_variables.default_vertical_positioning_error))
        self.kvar_verticalerror.editingFinished.connect(self.validate_numctrl)
        self.kvar_verticalerror.setToolTip('Default 1 sigma standard deviation of the vertical positioning system, only used if SBET is not provided')
        uncertaintyfive.addWidget(self.kvar_horizontalerror_label)
        uncertaintyfive.addWidget(self.kvar_horizontalerror)
        uncertaintyfive.addWidget(self.kvar_verticalerror_label)
        uncertaintyfive.addWidget(self.kvar_verticalerror)

        self.uncertainty_layout.addLayout(uncertaintyone)
        self.uncertainty_layout.addLayout(uncertaintytwo)
        self.uncertainty_layout.addLayout(uncertaintythree)
        self.uncertainty_layout.addLayout(uncertaintyfour)
        self.uncertainty_layout.addLayout(uncertaintyfive)
        self.uncertainty_layout.addStretch()

        self.uncertainty_tab.setLayout(self.uncertainty_layout)
        self.tabwidget.addTab(self.uncertainty_tab, 'Uncertainty')

        layout.addWidget(self.tabwidget)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.vdatum_pth = None
        self.filter_pth = None

        self.canceled = False

        self.browse_button.clicked.connect(self.vdatum_browse)
        self.vdatum_text.textChanged.connect(self.vdatum_changed)
        self.browse_filter_button.clicked.connect(self.filter_browse)
        self.filter_text.textChanged.connect(self.filter_changed)
        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)
        self.default_button.clicked.connect(self.set_to_default)

        self.text_controls = [['vdatum_directory', self.vdatum_text], ['auto_processing_mode', self.auto_processing_mode],
                              ['filter_text', self.filter_text]]
        self.checkbox_controls = [['enable_parallel_writes', self.parallel_write], ['keep_waterline_changes', self.keep_waterline_changes],
                                  ['force_coordinate_match', self.force_coordinate_match]]

        self.read_settings()
        self.resize(600, 150)

    def validate_numctrl(self):
        """
        validation function tied to whenever you lose focus on one of the number controls, automatically formats the
        number to something like '1.123'
        """
        sender = self.sender()
        sender.setText(format(float(sender.text()), '.3f'))

    def set_vyperdatum_path(self):
        if self.vdatum_pth:
            err, status = set_vyperdatum_vdatum_path(self.vdatum_pth)
            if err:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            else:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText(status)
        elif self.vdatum_pth == '':  # special case where the user is wanting to clear the vdatum directory path
            clear_vdatum_path()
            self.status_msg.setText('')
        else:
            self.status_msg.setText('')

    def return_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            opts = {'write_parallel': self.parallel_write.isChecked(),
                    'keep_waterline_changes': self.keep_waterline_changes.isChecked(),
                    'force_coordinate_match': self.force_coordinate_match.isChecked(),
                    'vdatum_directory': self.vdatum_pth,
                    'filter_directory': self.filter_pth,
                    'autoprocessing_mode': self.auto_processing_mode.currentText()}
            self.set_vyperdatum_path()
        else:
            opts = None
        return opts

    def return_kvars(self):
        """
        Return the values that will be used to overwrite the kluster_variables values and be saved to the kluster ini file
        """
        if not self.canceled:
            opts = {'pass_color': self.kvar_pass_color.currentText(), 'error_color': self.kvar_error_color.currentText(),
                    'warning_color': self.kvar_warning_color.currentText(), 'amplitude_color': self.kvar_amplitude_color.currentText(),
                    'phase_color': self.kvar_phase_color.currentText(), 'reject_color': self.kvar_reject_color.currentText(),
                    'reaccept_color': self.kvar_reaccept_color.currentText(), 'converted_files_at_once': self.kvar_convfiles.text(),
                    'pings_per_las': self.kvar_pingslas.text(), 'pings_per_csv': self.kvar_pingscsv.text(),
                    'default_heave_error': self.kvar_heaveerror.text(), 'default_roll_sensor_error': self.kvar_rollerror.text(),
                    'default_pitch_sensor_error': self.kvar_pitcherror.text(), 'default_heading_sensor_error': self.kvar_yawerror.text(),
                    'default_beam_opening_angle': self.kvar_beamangle.text(), 'default_surface_sv_error': self.kvar_sverror.text(),
                    'default_roll_patch_error': self.kvar_rollpatch.text(), 'default_waterline_error': self.kvar_waterline.text(),
                    'default_horizontal_positioning_error': self.kvar_horizontalerror.text(),
                    'default_vertical_positioning_error': self.kvar_verticalerror.text()}
        else:
            opts = None
        return opts

    def vdatum_browse(self):
        # dirpath will be None or a string
        msg, vdatum_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                           Title='Select Vdatum directory', AppName='\\reghelp')
        if vdatum_pth:
            self.vdatum_text.setText(vdatum_pth)

    def vdatum_changed(self):
        self.vdatum_pth = self.vdatum_text.text()
        self.set_vyperdatum_path()

    def filter_browse(self):
        # dirpath will be None or a string
        msg, filter_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                           Title='Select filter directory', AppName='\\reghelp')
        if filter_pth:
            self.filter_text.setText(filter_pth)

    def filter_changed(self):
        self.filter_pth = self.filter_text.text()

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        print('General settings saved')
        self.canceled = False
        self.save_settings()
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()

    def set_to_default(self):
        curidx = self.tabwidget.currentIndex()
        if curidx == 0:
            self.parallel_write.setChecked(True)
            self.force_coordinate_match.setChecked(True)
            self.keep_waterline_changes.setChecked(True)
            self.auto_processing_mode.setCurrentText('normal')
            self.vdatum_text.setText('')
            self.set_vyperdatum_path()
        elif curidx == 1:
            self.kvar_pass_color.setCurrentText(kluster_variables.kvar_initial_state['pass_color'])
            self.kvar_error_color.setCurrentText(kluster_variables.kvar_initial_state['error_color'])
            self.kvar_warning_color.setCurrentText(kluster_variables.kvar_initial_state['warning_color'])
            self.kvar_amplitude_color.setCurrentText(kluster_variables.kvar_initial_state['amplitude_color'])
            self.kvar_phase_color.setCurrentText(kluster_variables.kvar_initial_state['phase_color'])
            self.kvar_reject_color.setCurrentText(kluster_variables.kvar_initial_state['reject_color'])
            self.kvar_reaccept_color.setCurrentText(kluster_variables.kvar_initial_state['reaccept_color'])
        elif curidx == 2:
            self.kvar_convfiles.setValue(int(kluster_variables.kvar_initial_state['converted_files_at_once']))
            self.kvar_pingslas.setValue(int(kluster_variables.kvar_initial_state['pings_per_las']))
            self.kvar_pingscsv.setValue(int(kluster_variables.kvar_initial_state['pings_per_csv']))
        elif curidx == 3:
            self.kvar_beamangle.setText(str(kluster_variables.kvar_initial_state['default_beam_opening_angle']))
            self.kvar_heaveerror.setText(str(kluster_variables.kvar_initial_state['default_heave_error']))
            self.kvar_rollerror.setText(str(kluster_variables.kvar_initial_state['default_roll_sensor_error']))
            self.kvar_pitcherror.setText(str(kluster_variables.kvar_initial_state['default_pitch_sensor_error']))
            self.kvar_yawerror.setText(str(kluster_variables.kvar_initial_state['default_heading_sensor_error']))
            self.kvar_sverror.setText(str(kluster_variables.kvar_initial_state['default_surface_sv_error']))
            self.kvar_rollpatch.setText(str(kluster_variables.kvar_initial_state['default_roll_patch_error']))
            self.kvar_waterline.setText(str(kluster_variables.kvar_initial_state['default_waterline_error']))
            self.kvar_horizontalerror.setText(str(kluster_variables.kvar_initial_state['default_horizontal_positioning_error']))
            self.kvar_verticalerror.setText(str(kluster_variables.kvar_initial_state['default_vertical_positioning_error']))
        else:
            raise NotImplementedError('Found a tab index above 3 in settings dialog, should only be four tabs.')

    def read_settings(self):
        super().read_settings()
        self.vdatum_pth = self.vdatum_text.text()
        self.filter_pth = self.filter_text.text()
        self.set_vyperdatum_path()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = SettingsDialog()
    dlog.show()
    if dlog.exec_():
        print(dlog.return_options())
        print(dlog.return_kvars())
