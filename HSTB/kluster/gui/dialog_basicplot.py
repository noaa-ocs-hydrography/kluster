from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime

from HSTB.shared import RegistryHelpers

from HSTB.kluster.gui import common_widgets


class BasicPlotDialog(QtWidgets.QDialog):
    """
    Using the PlotDataHandler, allow the user to provide Kluster converted data and plot a variable across the whole
    time range or a subset of time (see PlotDataHandler for subsetting time)

    BasicPlot holds the calls that are just generic xarray.Dataset.plot calls.  If you need something fancy, it should
    be put in AdvancedPlot, as there are no controls in BasicPlot for additional files/settings.
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.fqpr = None
        self.datasets = None
        self.variables = None
        self.recent_plot = None

        self.variable_translator = {'acrosstrack': 'SoundVelocity_AcrossTrack', 'alongtrack': 'SoundVelocity_AlongTrack',
                                    'beampointingangle': 'Uncorrected_Beam_Angle', 'corr_altitude': 'Corrected_Altitude',
                                    'corr_heave': 'Corrected_Heave', 'corr_pointing_angle': 'Corrected_Beam_Angle',
                                    'counter': 'Ping_Counter', 'delay': 'Beam_Delay', 'depthoffset': 'SoundVelocity_Depth',
                                    'detectioninfo': 'Beam_Filter', 'frequency': 'Beam_Frequency', 'mode': 'Ping_Mode',
                                    'modetwo': 'Ping_Mode_Two', 'processing_status': 'Processing_Status',
                                    'qualityfactor': 'Beam_Uncertainty', 'rel_azimuth': 'Relative_Azimuth',
                                    'soundspeed': 'Surface_Sound_Velocity', 'thu': 'Beam_Total_Horizontal_Uncertainty',
                                    'tiltangle': 'Ping_Tilt_Angle', 'traveltime': 'Beam_Travel_Time',
                                    'tvu': 'Beam_Total_Vertical_Uncertainty', 'txsector_beam': 'Beam_Sector_Number',
                                    'x': 'Georeferenced_Easting', 'y': 'Georeferenced_Northing',
                                    'yawpitchstab': 'Yaw_Pitch_Stabilization', 'z': 'Georeferenced_Depth',
                                    'datum_uncertainty': 'Vertical_Datum_Uncertainty'}
        self.variable_reverse_lookup = {'SoundVelocity_AcrossTrack': 'acrosstrack', 'SoundVelocity_AlongTrack': 'alongtrack',
                                        'Uncorrected_Beam_Angle': 'beampointingangle', 'Corrected_Altitude': 'corr_altitude',
                                        'Corrected_Heave': 'corr_heave', 'Corrected_Beam_Angle': 'corr_pointing_angle',
                                        'Ping_Counter': 'counter', 'Beam_Delay': 'delay',
                                        'SoundVelocity_Depth': 'depthoffset', 'Beam_Filter': 'detectioninfo',
                                        'Beam_Frequency': 'frequency', 'Ping_Mode': 'mode', 'Ping_Mode_Two': 'modetwo',
                                        'Processing_Status': 'processing_status', 'Beam_Uncertainty': 'qualityfactor',
                                        'Relative_Azimuth': 'rel_azimuth', 'Surface_Sound_Velocity': 'soundspeed',
                                        'Beam_Total_Horizontal_Uncertainty': 'thu', 'Ping_Tilt_Angle': 'tiltangle',
                                        'Beam_Travel_Time': 'traveltime', 'Beam_Total_Vertical_Uncertainty': 'tvu',
                                        'Beam_Sector_Number': 'txsector_beam', 'Georeferenced_Easting': 'x',
                                        'Georeferenced_Northing': 'y', 'Yaw_Pitch_Stabilization': 'yawpitchstab',
                                        'Georeferenced_Depth': 'z', 'Vertical_Datum_Uncertainty': 'datum_uncertainty'}
        self.plot_lookup = {2: ['Histogram', 'Image', 'Contour', 'Line - Mean', 'Line - Nadir',
                                'Line - Port Outer Beam', 'Line - Starboard Outer Beam'],
                            1: ['Line', 'Histogram', 'Scatter']}
        self.custom_plot_lookup = {'sound_velocity_profiles': ['Plot Profiles', 'Profile Map'],
                                   'sound_velocity_correct': ['2d scatter, color by depth', '2d scatter, color by sector',
                                                              '3d scatter, color by depth', '3d scatter, color by sector'],
                                   'georeferenced': ['2d scatter, color by depth', '2d scatter, color by sector',
                                                     '3d scatter, color by depth', '3d scatter, color by sector'],
                                   'animations': ['Uncorrected Beam Vectors', 'Corrected Beam Vectors', 'Vessel Orientation']}

        self.setWindowTitle('Basic Plots')
        layout = QtWidgets.QVBoxLayout()

        self.data_widget = common_widgets.PlotDataHandler()

        self.hline = QtWidgets.QFrame()
        self.hline.setFrameShape(QtWidgets.QFrame.HLine)
        self.hline.setFrameShadow(QtWidgets.QFrame.Sunken)

        self.hlayout_main = QtWidgets.QHBoxLayout()
        self.vlayout_left = QtWidgets.QVBoxLayout()
        self.vlayout_right = QtWidgets.QVBoxLayout()

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.dataset_label = QtWidgets.QLabel('Source', self)
        self.dataset_label.setMinimumWidth(60)
        self.hlayout_one.addWidget(self.dataset_label)
        self.dataset_dropdown = QtWidgets.QComboBox(self)
        self.dataset_dropdown.setMinimumWidth(180)
        self.hlayout_one.addWidget(self.dataset_dropdown)
        self.hlayout_one.addStretch()

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.variable_label = QtWidgets.QLabel('Variable', self)
        self.variable_label.setMinimumWidth(60)
        self.hlayout_two.addWidget(self.variable_label)
        self.variable_dropdown = QtWidgets.QComboBox(self)
        self.variable_dropdown.setMinimumWidth(230)
        self.hlayout_two.addWidget(self.variable_dropdown)
        self.variable_dim_label = QtWidgets.QLabel('      Dimensions', self)
        self.hlayout_two.addWidget(self.variable_dim_label)
        self.variable_dimone = QtWidgets.QLineEdit('time', self)
        self.variable_dimone.setMaximumWidth(50)
        self.hlayout_two.addWidget(self.variable_dimone)
        self.variable_dimtwo = QtWidgets.QLineEdit('beam', self)
        self.variable_dimtwo.setMaximumWidth(50)
        self.hlayout_two.addWidget(self.variable_dimtwo)
        self.hlayout_two.addStretch()

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.plottype_label = QtWidgets.QLabel('Plot Type', self)
        self.plottype_label.setMinimumWidth(60)
        self.hlayout_three.addWidget(self.plottype_label)
        self.plottype_dropdown = QtWidgets.QComboBox(self)
        self.plottype_dropdown.setMinimumWidth(250)
        self.hlayout_three.addWidget(self.plottype_dropdown)
        self.bincount_label = QtWidgets.QLabel('Bins')
        self.bincount_label.hide()
        self.hlayout_three.addWidget(self.bincount_label)
        self.bincount = QtWidgets.QLineEdit('100', self)
        self.bincount.hide()
        self.hlayout_three.addWidget(self.bincount)
        self.hlayout_three.addStretch()

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.add_to_current_plot = QtWidgets.QCheckBox('Add to current plot', self)
        self.add_to_current_plot.setChecked(False)
        self.hlayout_five.addWidget(self.add_to_current_plot)
        self.zero_center_plot = QtWidgets.QCheckBox('Center on zero', self)
        self.zero_center_plot.setChecked(False)
        self.zero_center_plot.hide()
        self.hlayout_five.addWidget(self.zero_center_plot)

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.hlayout_four.addStretch()
        self.plot_button = QtWidgets.QPushButton('Plot', self)
        self.plot_button.setMaximumWidth(70)
        self.plot_button.setDisabled(True)
        self.hlayout_four.addWidget(self.plot_button)
        self.exportvar_button = QtWidgets.QPushButton(' Export Variable ', self)
        self.exportvar_button.setDisabled(True)
        self.hlayout_four.addWidget(self.exportvar_button)
        self.exportsource_button = QtWidgets.QPushButton(' Export Source ', self)
        self.exportsource_button.setDisabled(True)
        self.hlayout_four.addWidget(self.exportsource_button)
        self.hlayout_four.addStretch()

        self.hlayout_six = QtWidgets.QHBoxLayout()
        self.explanation = QtWidgets.QTextEdit('', self)
        self.hlayout_six.addWidget(self.explanation)

        layout.addWidget(self.data_widget)
        layout.addWidget(self.hline)

        self.vlayout_left.addLayout(self.hlayout_one)
        self.vlayout_left.addLayout(self.hlayout_two)
        self.vlayout_left.addLayout(self.hlayout_three)
        self.vlayout_left.addLayout(self.hlayout_five)
        self.vlayout_right.addLayout(self.hlayout_six)
        self.hlayout_main.addLayout(self.vlayout_left)
        self.hlayout_main.addLayout(self.vlayout_right)
        layout.addLayout(self.hlayout_main)

        layout.addLayout(self.hlayout_four)
        self.setLayout(layout)

        self.data_widget.fqpr_loaded.connect(self.new_fqpr_loaded)
        self.data_widget.ping_count_changed.connect(self.new_ping_count)
        self.dataset_dropdown.currentTextChanged.connect(self.load_variables)
        self.variable_dropdown.currentTextChanged.connect(self.load_plot_types)
        self.plottype_dropdown.currentTextChanged.connect(self.plot_type_selected)
        self.plot_button.clicked.connect(self.plot)
        self.exportvar_button.clicked.connect(self.export_variable)
        self.exportsource_button.clicked.connect(self.export_source)

    def new_fqpr_loaded(self, loaded: bool):
        """
        If a new fqpr is loaded (fqpr = converted Kluster data store) load the datasets

        Parameters
        ----------
        loaded
            if True, self.fqpr is valid
        """

        if loaded:
            self.fqpr = self.data_widget.fqpr
            self.load_datasets()
        else:
            self.fqpr = None
            self.datasets = {}
            self.dataset_dropdown.clear()
            self.variable_dropdown.clear()
            self.variable_dimone.setText('')
            self.variable_dimtwo.setText('')
            self.plottype_dropdown.clear()
            self.plot_button.setDisabled(True)
            self.exportsource_button.setDisabled(True)
            self.exportvar_button.setDisabled(True)

    def load_datasets(self):
        """
        Build the lookup for the various kluster datasets and populate the dataset dropdown with the keys
        """
        if self.fqpr is not None:
            self.datasets = {}
            if self.fqpr.multibeam.raw_ping:
                self.datasets['multibeam'] = self.fqpr.multibeam.raw_ping
                self.datasets['raw navigation'] = self.fqpr.multibeam.return_raw_navigation()
            else:
                print('No multibeam dataset(s) found in {}'.format(self.fqpr.multibeam.converted_pth))

            if self.fqpr.multibeam.raw_att:
                self.datasets['attitude'] = self.fqpr.multibeam.raw_att
            else:
                print('No attitude dataset found in {}'.format(self.fqpr.multibeam.converted_pth))

            if self.fqpr.navigation:
                self.datasets['processed navigation'] = self.fqpr.navigation
            else:
                print('No processed navigation dataset found in {}'.format(self.fqpr.multibeam.converted_pth))

            if 'alongtrack' in self.fqpr.multibeam.raw_ping[0] or 'x' in self.fqpr.multibeam.raw_ping[0]:
                self.datasets['custom'] = self.fqpr.plot
            else:
                print('No svcorrected/georeferenced variables found in {}'.format(self.fqpr.multibeam.converted_pth))

            combo_items = list(self.datasets.keys())
            self.dataset_dropdown.clear()
            self.dataset_dropdown.addItems(combo_items)
            self.dataset_dropdown.setCurrentIndex(0)

    def reload_datasets(self):
        """
        Triggered when self.fqpr is changed, we reload the datasets without the debug messaging just to keep it clean
        """

        if self.fqpr is not None:
            self.datasets = {}
            if self.fqpr.multibeam.raw_ping:
                self.datasets['multibeam'] = self.fqpr.multibeam.raw_ping
                self.datasets['raw navigation'] = self.fqpr.multibeam.return_raw_navigation()
            if self.fqpr.multibeam.raw_att:
                self.datasets['attitude'] = self.fqpr.multibeam.raw_att
            if self.fqpr.navigation:
                self.datasets['processed navigation'] = self.fqpr.navigation
            if 'alongtrack' in self.fqpr.multibeam.raw_ping[0] or 'x' in self.fqpr.multibeam.raw_ping[0]:
                self.datasets['custom'] = self.fqpr

    def load_variables(self):
        """
        Get the list of variables that are within the provided dataset
        """

        if self.datasets:
            ky = self.dataset_dropdown.currentText()
            if ky:
                dset = self.datasets[ky]
                if ky == 'multibeam':
                    dset = dset[0]  # grab the first multibeam dataset (dual head will have two) for the lookup
                    variable_names = [self.variable_translator[nm] for nm in list(dset.variables.keys()) if nm in self.variable_translator]
                elif ky == 'custom':
                    variable_names = ['sound_velocity_profiles', 'sound_velocity_correct', 'georeferenced', 'animations']
                else:
                    variable_names = [nm for nm in list(dset.variables.keys()) if nm not in ['time', 'beam', 'xyz']]

                self.variable_dropdown.clear()
                self.variable_dropdown.addItems(sorted(variable_names))
                self.variable_dropdown.setCurrentIndex(0)

    def load_plot_types(self):
        """
        Get the list of available plots for the provided variable/dataset
        """
        if self.datasets:
            self.data_widget.warning_message.setText('')
            self.plottype_dropdown.clear()
            ky = self.dataset_dropdown.currentText()
            dset = self.datasets[ky]
            vari = self.variable_dropdown.currentText()
            if vari:
                if ky == 'multibeam':
                    dset = dset[0]  # grab the first multibeam dataset (dual head will have two) for the lookup
                    dset_var = self.variable_reverse_lookup[vari]
                else:
                    dset_var = vari
                if dset_var:
                    if ky == 'custom':
                        self.variable_dimone.setText('time')
                        self.variable_dimtwo.setText('beam')
                        plottypes = self.custom_plot_lookup[vari]
                        self.plottype_dropdown.addItems(plottypes)
                        self.plottype_dropdown.setCurrentIndex(0)
                        self.plot_button.setEnabled(True)
                        self.exportvar_button.setEnabled(False)
                        self.exportsource_button.setEnabled(False)
                    elif dset[dset_var].ndim == 2:
                        self.variable_dimone.setText(dset[dset_var].dims[0])
                        self.variable_dimtwo.setText(dset[dset_var].dims[1])
                        plottypes = self.plot_lookup[2]
                        self.plottype_dropdown.addItems(plottypes)
                        self.plottype_dropdown.setCurrentIndex(0)
                        self.plot_button.setEnabled(True)
                        self.exportvar_button.setEnabled(True)
                        self.exportsource_button.setEnabled(True)
                    elif dset[dset_var].ndim == 1:
                        self.variable_dimone.setText(dset[dset_var].dims[0])
                        self.variable_dimtwo.setText('None')
                        plottypes = self.plot_lookup[1].copy()
                        if dset_var in ['mode', 'modetwo', 'yawpitchstab']:
                            plottypes.remove('Line')
                        self.plottype_dropdown.addItems(plottypes)
                        self.plottype_dropdown.setCurrentIndex(0)
                        self.plot_button.setEnabled(True)
                        self.exportvar_button.setEnabled(True)
                        self.exportsource_button.setEnabled(True)
                    else:
                        self.variable_dimone.setText('None')
                        self.variable_dimtwo.setText('None')
                        self.data_widget.warning_message.setText('ERROR: only 2d and 1d vars allowed, found {}:{}d'.format(self.variable_dropdown.currentText(),
                                                                                                                           dset[dset_var].ndim))

    def plot_type_selected(self):
        """
        We have some checks against ping count for some plot types.  Go ahead and get the current ping count and run
        those checks.
        """
        if self.datasets:
            plottype = self.plottype_dropdown.currentText()
            if plottype:
                ping_count = int(self.data_widget.ping_count.text())
                self.new_ping_count(ping_count)
                self.refresh_explanation()
                if plottype == 'Histogram':
                    self.bincount_label.show()
                    self.bincount.show()
                else:
                    self.bincount_label.hide()
                    self.bincount.hide()
                if plottype[0:4] == 'Line':
                    self.zero_center_plot.show()
                else:
                    self.zero_center_plot.hide()

    def new_ping_count(self, ping_count: int):
        """
        We check some plottypes to ensure the number of pings provided makes sense

        Parameters
        ----------
        ping_count
            total number of pings that we plan on plotting
        """

        if self.datasets:
            plottype = self.plottype_dropdown.currentText()
            if plottype:
                if plottype in ['3d scatter, color by depth', '3d scatter, color by sector'] and ping_count > 100:
                    self.data_widget.warning_message.setText('Warning: 3d scatter will be very slow with greater than 100 pings')
                elif plottype in ['2d scatter, color by depth', '2d scatter, color by sector'] and ping_count > 500:
                    self.data_widget.warning_message.setText('Warning: 2d scatter will be very slow with greater than 500 pings')
                else:
                    self.data_widget.warning_message.setText('')

    def _plot_variable(self, dataset, dataset_name, variable, plottype, sonartype, serialnum):
        """
        Method for plotting the various variables.

        Parameters
        ----------
        dataset
            the base object for plotting, either an xarray Dataset or a Fqpr object (for dataset_name == 'custom')
        dataset_name
            the base name, 'multibeam', 'attitude', etc.
        variable
            the variable name, 'beampointingangle', etc.
        plottype
            the plot type, 'Line', 'Histogram', etc.
        sonartype
            the sonar type, 'em2040', etc.
        serialnum
            the serial number of this sonar, '389', etc.
        """

        try:
            translated_var = self.variable_translator[variable]
        except:
            translated_var = variable

        if not self.add_to_current_plot.isChecked() and dataset_name != 'custom':
            if isinstance(dataset, list) and len(dataset) > 1:
                fig, self.recent_plot = plt.subplots(ncols=2)
                fig.suptitle('{}: {} Plot of {}'.format(sonartype[0], plottype, translated_var))
            else:
                fig = plt.figure()
                self.recent_plot = [plt.subplot()]

        if not isinstance(dataset, list):
            dataset = [dataset]
        custom_vartype = None
        for cnt, dset in enumerate(dataset):
            if variable == 'corr_pointing_angle':
                data = np.rad2deg(dset[variable])
            elif dataset_name == 'custom':
                data = dset
                if variable == 'georeferenced':
                    custom_vartype = 'georef'
                else:
                    custom_vartype = 'svcorr'
            else:
                data = dset[variable]

            identifier = '{} {}'.format(dataset_name, translated_var)
            if plottype == 'Line':
                if self.zero_center_plot.isChecked():
                    (data - data.mean()).plot.line(ax=self.recent_plot[cnt], label=identifier)
                else:
                    data.plot.line(ax=self.recent_plot[cnt], label=identifier)
            elif plottype == 'Line - Mean':
                newdata = data.mean(axis=1)
                if self.zero_center_plot.isChecked():
                    (newdata - newdata.mean()).plot.line(ax=self.recent_plot[cnt], label=identifier + ' (mean)')
                else:
                    newdata.plot.line(ax=self.recent_plot[cnt], label=identifier + ' (mean)')
            elif plottype == 'Line - Nadir':
                nadir_beam_num = int((data.beam.shape[0] / 2) - 1)
                newdata = data.isel(beam=nadir_beam_num)
                if self.zero_center_plot.isChecked():
                    (newdata - newdata.mean()).plot.line(ax=self.recent_plot[cnt], label=identifier + ' (nadir)')
                else:
                    newdata.plot.line(ax=self.recent_plot[cnt], label=identifier + ' (nadir)')
            elif plottype == 'Line - Port Outer Beam':
                newdata = data.isel(beam=0)
                if self.zero_center_plot.isChecked():
                    (newdata - newdata.mean()).plot.line(ax=self.recent_plot[cnt], label=identifier + ' (port outer)')
                else:
                    newdata.plot.line(ax=self.recent_plot[cnt], label=identifier + ' (port outer)')
            elif plottype == 'Line - Starboard Outer Beam':
                last_beam_num = int((data.beam.shape[0]) - 1)
                newdata = data.isel(beam=last_beam_num)
                if self.zero_center_plot.isChecked():
                    (newdata - newdata.mean()).plot.line(ax=self.recent_plot[cnt], label=identifier + ' (stbd outer)')
                else:
                    newdata.plot.line(ax=self.recent_plot[cnt], label=identifier + ' (stbd outer)')
            elif plottype == 'Histogram':
                bincount = int(self.bincount.text())
                data.plot.hist(ax=self.recent_plot[cnt], bins=bincount, label=identifier)
            elif plottype == 'Scatter':
                dset.plot.scatter('time', variable, ax=self.recent_plot[cnt], label=identifier)
            elif plottype == 'Image':
                data.plot.imshow(ax=self.recent_plot[cnt], label=identifier)
            elif plottype == 'Contour':
                data.plot.contourf(ax=self.recent_plot[cnt])

            elif plottype == '2d scatter, color by depth':
                data.plot.soundings_plot_2d(custom_vartype, color_by='depth')
            elif plottype == '2d scatter, color by sector':
                data.plot.soundings_plot_2d(custom_vartype, color_by='sector')
            elif plottype == '3d scatter, color by depth':
                data.plot.soundings_plot_3d(custom_vartype, color_by='depth')
            elif plottype == '3d scatter, color by sector':
                data.plot.soundings_plot_3d(custom_vartype, color_by='sector')
            elif plottype == 'Uncorrected Beam Vectors':
                data.plot.visualize_beam_pointing_vectors(False)
            elif plottype == 'Corrected Beam Vectors':
                data.plot.visualize_beam_pointing_vectors(True)
            elif plottype == 'Vessel Orientation':
                data.plot.visualize_orientation_vector()
            elif plottype == 'Profile Map':
                data.plot.plot_sound_velocity_map()
            elif plottype == 'Plot Profiles':
                data.plot.plot_sound_velocity_profiles()

            if dataset_name != 'custom' and plottype not in ['Image', 'Contour']:
                self.recent_plot[cnt].legend()

        if not self.add_to_current_plot.isChecked():
            if dataset_name != 'custom' and len(self.recent_plot) == 2:
                self.recent_plot[0].set_title('Port-{}'.format(serialnum[0]))
                self.recent_plot[1].set_title('Starboard-{}'.format(serialnum[1]))
            else:
                plt.title('{} (SN{}): {} Plot of {}'.format(sonartype, serialnum, plottype, translated_var))

    def plot(self):
        """
        Build out the data that we plan to plot
        """
        if self.datasets:
            min_max = self.data_widget.return_trim_times()
            if min_max:
                self.fqpr.subset_by_time(min_max[0], min_max[1])
            self.reload_datasets()
            ky = self.dataset_dropdown.currentText()
            plottype = self.plottype_dropdown.currentText()
            dsets = self.datasets[ky]
            dset_name = ky
            if plottype == 'Histogram':
                try:
                    bincount = int(self.bincount.text())
                except:
                    self.data_widget.warning_message.setText('Bins must be an integer, user entered "{}"'.format(self.bincount.text()))
                    return

            if ky == 'multibeam':
                dset_var = self.variable_reverse_lookup[self.variable_dropdown.currentText()]
                if len(dsets) > 1:
                    sonartype = [d.sonartype for d in dsets]
                    serialnum = [d.system_identifier for d in dsets]
                else:
                    sonartype = dsets[0].sonartype
                    serialnum = dsets[0].system_identifier
                self._plot_variable(dsets, dset_name, dset_var, plottype, sonartype, serialnum)
            else:
                dset_var = self.variable_dropdown.currentText()
                sonartype = self.datasets['multibeam'][0].sonartype
                serialnum = self.datasets['multibeam'][0].system_identifier
                self._plot_variable(dsets, dset_name, dset_var, plottype, sonartype, serialnum)
            if min_max:
                self.fqpr.restore_subset()

    def export_variable(self):
        """
        Export the currently selected dataset/variable to csv
        """

        if self.datasets:
            ky = self.dataset_dropdown.currentText()
            plottype = self.plottype_dropdown.currentText()
            reduce_method = None
            zero_centered = False

            try:
                dvar = self.variable_reverse_lookup[self.variable_dropdown.currentText()]
            except:
                dvar = self.variable_dropdown.currentText()
            defvalue = os.path.join(self.fqpr.output_folder, 'export_{}_{}.csv'.format(ky, dvar))
            if plottype[0:4] == 'Line':
                zero_centered = self.zero_center_plot.isChecked()
                if plottype.find('Mean') != -1:
                    reduce_method = 'mean'
                    defvalue = os.path.splitext(defvalue)[0] + '_mean.csv'
                elif plottype.find('Nadir') != -1:
                    reduce_method = 'nadir'
                    defvalue = os.path.splitext(defvalue)[0] + '_nadir.csv'
                elif plottype.find('Port') != -1:
                    reduce_method = 'port_outer_beam'
                    defvalue = os.path.splitext(defvalue)[0] + '_portbeam.csv'
                elif plottype.find('Starboard') != -1:
                    reduce_method = 'starboard_outer_beam'
                    defvalue = os.path.splitext(defvalue)[0] + '_stbdbeam.csv'
            if zero_centered:
                defvalue = os.path.splitext(defvalue)[0] + '_zerocentered.csv'

            msg, output_pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster', DefaultVal=self.fqpr.output_folder, fFilter="csv files|*.csv",
                                                                    DefaultFile=defvalue, Title='Output dataset path for csv export', AppName='\\reghelp')
            if output_pth:
                min_max = self.data_widget.return_trim_times()
                if min_max:
                    self.fqpr.subset_by_time(min_max[0], min_max[1])
                self.reload_datasets()
                self.fqpr.export_variable(ky, dvar, output_pth, reduce_method=reduce_method, zero_centered=zero_centered)
                if min_max:
                    self.fqpr.restore_subset()

    def export_source(self):
        """
        Export the currently selected dataset to csv
        """
        if self.datasets:
            ky = self.dataset_dropdown.currentText()
            defvalue = os.path.join(self.fqpr.output_folder, 'export_{}.csv'.format(ky))
            msg, output_pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                                    DefaultVal=self.fqpr.output_folder,
                                                                    fFilter="csv files|*.csv",
                                                                    DefaultFile=defvalue,
                                                                    Title='Output dataset path for csv export',
                                                                    AppName='\\reghelp')
            if output_pth:
                min_max = self.data_widget.return_trim_times()
                if min_max:
                    self.fqpr.subset_by_time(min_max[0], min_max[1])
                self.reload_datasets()
                self.fqpr.export_dataset(ky, output_pth)
                if min_max:
                    self.fqpr.restore_subset()

    def refresh_explanation(self):
        """
        Update the help text control (self.explanation) with an explanation of what the plot is, based on the
        set source/variable/plottype control.
        """
        source = self.dataset_dropdown.currentText()
        variable = self.variable_dropdown.currentText()
        plottype = self.plottype_dropdown.currentText()
        source_expl = ''
        variable_expl = ''
        plot_expl = ''

        if source == 'multibeam':
            source_expl = 'Source = From the raw multibeam data, or from the Kluster processed intermediate variables.'
            if variable == 'Beam_Delay':
                variable_expl = 'Variable = The time delay applied to each sector, expanded to the beam dimension.  Comes from the multibeam raw data.  Generally fairly small, or zero.'
            elif variable == 'Beam_Filter':
                variable_expl = 'Variable = The accepted/rejected state of each beam.  2 = rejected, 1 = phase detection, 0 = amplitude detection.  See Kongsberg "detectioninfo".'
            elif variable == 'Beam_Frequency':
                variable_expl = 'Variable = The frequency of each beam in Hz.'
            elif variable == 'Beam_Sector_Number':
                variable_expl = 'Variable = The sector number of each beam.'
            elif variable == 'Beam_Total_Horizontal_Uncertainty':
                variable_expl = 'Variable = The Hare-Godin-Mayer TPU model - horizontal component.  In meters.'
            elif variable == 'Beam_Total_Vertical_Uncertainty':
                variable_expl = 'Variable = The Hare-Godin-Mayer TPU model - vertical component.  In meters.'
            elif variable == 'Beam_Travel_Time':
                variable_expl = 'Variable = The two way travel time of each beam in seconds.'
            elif variable == 'Beam_Uncertainty':
                variable_expl = 'Variable = The raw uncertainty record that comes from the multibeam.  Corresponds to the Kongsberg detectioninfo (.all) detectiontype (.kmall).  See datagram description for more information.'
            elif variable == 'Corrected_Beam_Angle':
                variable_expl = 'Variable = The result of running Compute Beam Vectors in Kluster.  This is the raw beam angles corrected for attitude and mounting angles, relative to nadir (straight down from sonar).'
            elif variable == 'Corrected_Altitude':
                variable_expl = 'Variable = If this dataset is processed to the waterline this will be zero.  Otherwise, the altitude correction is the attitude rotated lever arm between the reference point of the altitude and the transmitter, if non-zero.  This will be the original altitude plus this correction.'
            elif variable == 'Corrected_Heave':
                variable_expl = 'Variable = If this dataset is processed to the ellipse this will be zero.  Otherwise, the heave correction is the attitude rotated lever arm between the reference point of the heave and the transmitter, if non-zero. This will be the original heave plus this correction.'
            elif variable == 'Georeferenced_Depth':
                variable_expl = 'Variable = The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Depth is in meters from the vertical reference you chose.'
            elif variable == 'Georeferenced_Easting':
                variable_expl = 'Variable = The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Easting is in meters.'
            elif variable == 'Georeferenced_Northing':
                variable_expl = 'Variable = The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Northing is in meters.'
            elif variable == 'Ping_Counter':
                variable_expl = 'Variable = The identification number assigned to each ping.  For Kongsberg .all, this is a 16bit number, so you will see it reset at 65536.'
            elif variable == 'Ping_Mode':
                variable_expl = 'Variable = The first mode value. \n(if TX Pulse Form) CW for continuous waveform, FM for frequency modulated, MIX for mix between FM and CW. \n(if Ping mode) VS for Very Shallow, SH for Shallow, ME for Medium, DE for Deep, VD for Very Deep, ED for Extra Deep'
            elif variable == 'Ping_Mode_Two':
                variable_expl = 'Variable = The second mode value. \n(if Pulse Length) vsCW = very short continuous waveform, shCW = short cw, meCW = medium cw, loCW = long cw, vlCW = very long cw, elCW = extra long cw, shFM = short frequency modulated, loFM = long fm. \n(if Depth Mode) VS = Very Shallow, SH = Shallow, ME = Medium, DE = Deep, DR = Deeper, VD = Very Deep, ED = Extra deep, XD = Extreme Deep, if followed by "m" system is in manual mode'
            elif variable == 'Ping_Tilt_Angle':
                variable_expl = 'Variable = Steering angle of the sector transmit beam, in degrees'
            elif variable == 'Processing_Status':
                variable_expl = 'Variable = The Kluster processing status of each beam, the highest state of the beam.  EX: If 3, sounding is only processed up to sound velocity correction. 0 = converted, 1 = orientation, 2 = beamvector, 3 = soundvelocity, 4 = georeference, 5 = tpu'
            elif variable == 'Relative_Azimuth':
                variable_expl = 'Variable = The result of running Compute Beam Vectors in Kluster.  This is the direction to the beam footprint on the seafloor from the sonar in radians.'
            elif variable == 'SoundVelocity_AcrossTrack':
                variable_expl = 'Variable = The result of running Sound Velocity Correct in Kluster.  This is the acrosstrack (perpendicular to vessel movement) distance to the beam footprint on the seafloor from the vessel reference point in meters'
            elif variable == 'SoundVelocity_AlongTrack':
                variable_expl = 'Variable = The result of running Sound Velocity Correct in Kluster.  This is the alongtrack (vessel direction) distance to the beam footprint on the seafloor from the vessel reference point in meters'
            elif variable == 'SoundVelocity_Depth':
                variable_expl = 'Variable = The result of running Sound Velocity Correct in Kluster.  This is the down (positive down) distance to the beam footprint on the seafloor from the transmitter in meters.  Not from the vessel reference point to align with Kongsberg svcorrect convention.  We apply the z lever arm in georeferencing.'
            elif variable == 'Surface_Sound_Velocity':
                variable_expl = 'Variable = The surface sound velocimeter data, in meters per second'
            elif variable == 'Uncorrected_Beam_Angle':
                variable_expl = 'Variable = The raw beam angle that comes from the multibeam data.  Angle in degrees from the receiver to the beam footprint on the seafloor, does not take attitude or mounting angles into account.'
            elif variable == 'Yaw_Pitch_Stabilization':
                variable_expl = 'Variable = Tells you whether yaw/pitch stabilization was enabled on the sonar\nY = Only yaw stab, P = Only pitch stab, PY = Pitch and yaw stab, N = Neither.'
            elif variable == 'Vertical_Datum_Uncertainty':
                variable_expl = 'Variable = Included when VDatum is used for vertical transformation to NOAA Chart Datums, is the uncertainty of that transform.  Will be all zeros if NOAA_MLLW/NOAA_MHW is not selected.'
        elif source == 'attitude':
            source_expl = 'Source = Attitude from the raw multibeam file.'
            if variable == 'heading':
                variable_expl = 'Variable = From the raw multibeam data, the logged heading data from the attitude system in degrees.  Relative to true north in navigation reference frame'
            elif variable == 'heave':
                variable_expl = 'Variable = From the raw multibeam data, the logged heave data from the attitude system in meters.  Short period vertical motion of vessel'
            elif variable == 'pitch':
                variable_expl = 'Variable = From the raw multibeam data, the logged pitch data from the attitude system in degrees.  Rotation of vessel (up and down) about the Y axis (transverse axis)'
            elif variable == 'roll':
                variable_expl = 'Variable = From the raw multibeam data, the logged roll data from the attitude system in degrees.  Rotation of vessel (left to right) about the X axis (longitudinal axis)'
        elif source == 'raw navigation':
            source_expl = 'Source = Navigation from the raw multibeam file.'
            if variable == 'altitude':
                variable_expl = 'Variable = From the raw multibeam data, the logged altitude data from the navigation system in meters.  Relative to the ellipsoid chosen in the navigation system setup.'
            elif variable == 'latitude':
                variable_expl = 'Variable = From the raw multibeam data, the logged latitude data from the navigation system in degrees.'
            elif variable == 'longitude':
                variable_expl = 'Variable = From the raw multibeam data, the logged longitude data from the navigation system in degrees.'
        elif source == 'processed navigation':
            source_expl = 'Source = Navigation from imported post-processed navigation'
            if variable == 'altitude':
                variable_expl = 'Variable = From the imported post processed navigation, the exported altitude data in meters.  Relative to the ellipsoid chosen in the post processing software.'
            elif variable == 'latitude':
                variable_expl = 'Variable = From the imported post processed navigation, the logged latitude data from the navigation system in degrees.'
            elif variable == 'longitude':
                variable_expl = 'Variable = From the imported post processed navigation, the logged longitude data from the navigation system in degrees.'
            elif variable == 'down_position_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged down position error from the navigation system in meters.'
            elif variable == 'east_position_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged east position error data from the navigation system in meters.'
            elif variable == 'north_position_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged north position error data from the navigation system in meters.'
            elif variable == 'roll_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged roll error data from the navigation system in degrees.'
            elif variable == 'pitch_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged pitch error data from the navigation system in degrees.'
            elif variable == 'heading_error':
                variable_expl = 'Variable = From the imported post processed navigation, the logged heading error data from the navigation system in degrees.'
        elif source == 'custom':
            source_expl = 'Source = Custom Kluster plots from all converted and processed data'
            if variable == 'georeferenced':
                variable_expl = 'Variable = From the Kluster processed georeferenced northing/easting/depth'
            elif variable == 'sound_velocity_correct':
                variable_expl = 'Variable = From the Kluster processed sound velocity corrected alongtrack offset/acrosstrack offset/depth offset'
            elif variable == 'sound_velocity_profiles':
                variable_expl = 'Variable = Build plots from the imported sound velocity profiles (casts), including those in the raw multibeam data.'
            elif variable == 'animations':
                variable_expl = 'Variable = Build custom animations from the Kluster processed data'

        if plottype == 'Line':
            plot_expl = 'Plot = Line plot connecting the points in the variable, will connect points across gaps in data.  Use scatter to see the gaps.'
        elif plottype == 'Line - Mean':
            plot_expl = 'Plot = Line plot of the average value for this variable for each ping'
        elif plottype == 'Line - Nadir':
            plot_expl = 'Plot = Line plot of the Nadir beam (center beam) for this variable'
        elif plottype == 'Line - Port Outer Beam':
            plot_expl = 'Plot = Line plot of the first beam (port outer beam) for this variable'
        elif plottype == 'Line - Starboard Outer Beam':
            plot_expl = 'Plot = Line plot of the last beam (starboard outer beam) for this variable'
        elif plottype == 'Histogram':
            plot_expl = 'Plot = Bar plot grouping the data into bins, the number of which is set by the user.'
        elif plottype == 'Scatter':
            plot_expl = 'Plot = Plot all the points in the variable.'
        elif plottype == 'Image':
            plot_expl = 'Plot = Build a linearly interpolated image of the 2d variable using matplotlib imshow, provides a fast view of the data'
        elif plottype == 'Contour':
            plot_expl = 'Plot = Plot the filled in contours of the data'
        elif plottype == '2d scatter, color by depth':
            plot_expl = 'Plot = Overhead view of the variable points, colored by depth'
        elif plottype == '2d scatter, color by sector':
            plot_expl = 'Plot = Overhead view of the variable points, colored by the sector each beam belongs to'
        elif plottype == '3d scatter, color by depth':
            plot_expl = 'Plot = 3d scatter plot of variable, colored by depth'
        elif plottype == '3d scatter, color by sector':
            plot_expl = 'Plot = 3d scatter plot of variable, colored by the sector each beam belongs to'
        elif plottype == 'Uncorrected Beam Vectors':
            plot_expl = 'Plot = Animation of uncorrected beam angles versus traveltime, will show the effects of attitude and mounting angles.'
        elif plottype == 'Corrected Beam Vectors':
            plot_expl = 'Plot = Animation of corrected beam angles versus traveltime, corrected for attitude and mounting angles.'
        elif plottype == 'Vessel Orientation':
            plot_expl = 'Plot = Animation of Vessel Orientation, corrected for attitude and mounting angles.  TX vector represents the transmitter, RX vector represents the receiver.'
        elif plottype == 'Plot Profiles':
            plot_expl = 'Plot = Plot of depth versus sound velocity values in each sound velocity profile.  All profiles from Kongsberg multibeam have been extended to 12000 meters.  Zoom in to see the shallow values.  Shows all casts regardless of specified time range'
        elif plottype == 'Profile Map':
            plot_expl = 'Plot = Plot all lines within the specified time range and all sound velocity profiles.  Casts from multibeam have a position equal to the position of the vessel at the time of the cast.'

        if plot_expl and variable_expl and source_expl:
            self.explanation.setText('{}\n\n{}\n\n{}'.format(source_expl, variable_expl, plot_expl))
        else:
            self.explanation.setText('')


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = BasicPlotDialog()
    dlog.show()
    app.exec_()
