from PySide2 import QtWidgets, QtCore, QtGui
import numpy as np

from HSTB.kluster.gui import common_widgets
from HSTB.kluster.modules import wobble, sat


class AdvancedPlotDialog(QtWidgets.QDialog):
    """
    Using the PlotDataHandler, allow the user to provide Kluster converted data and plot a variable across the whole
    time range or a subset of time (see PlotDataHandler for subsetting time)
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.fqpr = None
        self.datasets = None
        self.variables = None
        self.recent_plot = None
        self.current_ping_count = 0

        self.wobble = None
        self.extinction = None
        self.period = None
        self.needs_rebuilding = False

        #self.plottypes = ['Wobble Test', 'Accuracy Test', 'Extinction Test', 'Data Density Test', 'Ping Period Test']
        self.plottypes = ['Wobble Test', 'Extinction Test', 'Ping Period Test']
        self.modetypes = {'Wobble Test': ['Dashboard', 'Allowable Percent Deviation', 'Attitude Scaling One', 'Attitude Scaling Two',
                                          'Attitude Latency', 'Yaw Alignment', 'X (Forward) Sonar Offset', 'Y (Starboard) Sonar Offset',
                                          'Heave Sound Speed One', 'Heave Sound Speed Two'],
                          'Accuracy Test': ['Use most prevalent mode'],
                          'Extinction Test': ['Plot Extinction by Frequency', 'Plot Extinction by Mode',
                                              'Plot Extinction by Sub Mode'],
                          'Data Density Test': ['By Frequency', 'By Mode'],
                          'Ping Period Test': ['Plot Period by Frequency', 'Plot Period by Mode',
                                               'Plot Period by Sub Mode']}

        self.setWindowTitle('Advanced Plots')
        layout = QtWidgets.QVBoxLayout()

        self.data_widget = common_widgets.PlotDataHandler()

        self.hline = QtWidgets.QFrame()
        self.hline.setFrameShape(QtWidgets.QFrame.HLine)
        self.hline.setFrameShadow(QtWidgets.QFrame.Sunken)

        self.hlayout_main = QtWidgets.QHBoxLayout()
        self.vlayout_left = QtWidgets.QVBoxLayout()
        self.vlayout_right = QtWidgets.QVBoxLayout()

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.plot_type_label = QtWidgets.QLabel('Plot Type ', self)
        self.plot_type_label.setMinimumWidth(60)
        self.hlayout_one.addWidget(self.plot_type_label)
        self.plot_type_dropdown = QtWidgets.QComboBox(self)
        self.plot_type_dropdown.setMinimumWidth(180)
        self.hlayout_one.addWidget(self.plot_type_dropdown)
        self.hlayout_one.addStretch()

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.mode_label = QtWidgets.QLabel('Mode ', self)
        self.mode_label.setMinimumWidth(60)
        self.hlayout_two.addWidget(self.mode_label)
        self.mode_dropdown = QtWidgets.QComboBox(self)
        self.mode_dropdown.setMinimumWidth(230)
        self.hlayout_two.addWidget(self.mode_dropdown)
        self.hlayout_two.addStretch()

        self.hlayout_extinction = QtWidgets.QHBoxLayout()
        self.roundedfreq = QtWidgets.QCheckBox('Round Frequency')
        self.roundedfreq.setChecked(True)
        self.roundedfreq.hide()
        self.hlayout_extinction.addWidget(self.roundedfreq)
        self.extinction_onlycomplete = QtWidgets.QCheckBox('Only Complete Swaths')
        self.extinction_onlycomplete.setChecked(True)
        self.extinction_onlycomplete.hide()
        self.hlayout_extinction.addWidget(self.extinction_onlycomplete)
        self.extinction_binsizelabel = QtWidgets.QLabel('Depth Bin Size (m): ')
        self.extinction_binsizelabel.hide()
        self.hlayout_extinction.addWidget(self.extinction_binsizelabel)
        self.extinction_binsize = QtWidgets.QLineEdit('1', self)
        self.extinction_binsize.hide()
        self.hlayout_extinction.addWidget(self.extinction_binsize)
        self.hlayout_extinction.addStretch()

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.hlayout_four.addStretch()
        self.plot_button = QtWidgets.QPushButton('Plot', self)
        self.plot_button.setMaximumWidth(70)
        self.plot_button.setDisabled(True)
        self.hlayout_four.addWidget(self.plot_button)
        self.hlayout_four.addStretch()

        self.hlayout_six = QtWidgets.QHBoxLayout()
        self.explanation = QtWidgets.QTextEdit('', self)
        self.explanation.setMinimumWidth(500)
        self.hlayout_six.addWidget(self.explanation)

        layout.addWidget(self.data_widget)
        layout.addWidget(self.hline)

        self.vlayout_left.addLayout(self.hlayout_one)
        self.vlayout_left.addLayout(self.hlayout_two)
        self.vlayout_left.addLayout(self.hlayout_extinction)

        self.vlayout_right.addLayout(self.hlayout_six)

        self.hlayout_main.addLayout(self.vlayout_left)
        self.hlayout_main.addLayout(self.vlayout_right)
        layout.addLayout(self.hlayout_main)

        layout.addLayout(self.hlayout_four)
        self.setLayout(layout)

        self.data_widget.fqpr_loaded.connect(self.new_fqpr_loaded)
        self.data_widget.ping_count_changed.connect(self.new_ping_count)
        self.plot_type_dropdown.currentTextChanged.connect(self.plottype_changed)
        self.mode_dropdown.currentTextChanged.connect(self.mode_changed)
        self.plot_button.clicked.connect(self.plot)
        self.roundedfreq.clicked.connect(self._clear_alldata)

    def _clear_alldata(self):
        """
        Clear all the datasets stored for each test
        """

        self.extinction = None
        self.wobble = None
        self.period = None

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
            self.plot_type_dropdown.clear()
            self.mode_dropdown.clear()
            self.plot_button.setDisabled(True)

    def load_datasets(self):
        """
        Build the lookup for the various kluster datasets and populate the dataset dropdown with the keys
        """
        if self.fqpr is not None:
            self.datasets = {}
            if self.fqpr.multibeam.raw_ping:
                self.datasets['multibeam'] = self.fqpr.multibeam.raw_ping
            else:
                print('No multibeam dataset(s) found in {}'.format(self.fqpr.multibeam.converted_pth))

            if self.fqpr.multibeam.raw_att:
                self.datasets['attitude'] = self.fqpr.multibeam.raw_att
            else:
                print('No attitude dataset found in {}'.format(self.fqpr.multibeam.converted_pth))

            if self.fqpr.multibeam.raw_nav:
                self.datasets['raw navigation'] = self.fqpr.multibeam.raw_nav
            else:
                print('No raw navigation dataset found in {}'.format(self.fqpr.multibeam.converted_pth))

            if self.fqpr.navigation:
                self.datasets['processed navigation'] = self.fqpr.navigation
            else:
                print('No processed navigation dataset found in {}'.format(self.fqpr.multibeam.converted_pth))

            self.plot_type_dropdown.clear()
            self.plot_type_dropdown.addItems(self.plottypes)
            self.plot_type_dropdown.setCurrentIndex(0)
            self._clear_alldata()

    def reload_datasets(self):
        """
        Triggered when self.fqpr is changed, we reload the datasets without the debug messaging just to keep it clean
        """

        if self.fqpr is not None:
            self.datasets = {}
            if self.fqpr.multibeam.raw_ping:
                self.datasets['multibeam'] = self.fqpr.multibeam.raw_ping
            if self.fqpr.multibeam.raw_att:
                self.datasets['attitude'] = self.fqpr.multibeam.raw_att
            if self.fqpr.multibeam.raw_nav:
                self.datasets['raw navigation'] = self.fqpr.multibeam.raw_nav
            if self.fqpr.navigation:
                self.datasets['processed navigation'] = self.fqpr.navigation

    def plottype_changed(self):
        """
        Triggered when changing the plottype dropdown
        """

        if self.datasets:
            self._clear_alldata()
            ky = self.plot_type_dropdown.currentText()
            if ky:
                modetypes = self.modetypes[ky]
                self.mode_dropdown.clear()
                self.mode_dropdown.addItems(modetypes)
                self.mode_dropdown.setCurrentIndex(0)
                data_is_there = False
                if ky == 'Wobble Test':
                    data_is_there = np.all([x in self.fqpr.multibeam.raw_ping[0] for x in ['depthoffset', 'corr_pointing_angle', 'corr_heave', 'corr_altitude']])
                elif ky == 'Extinction Test':
                    data_is_there = np.all([x in self.fqpr.multibeam.raw_ping[0] for x in ['acrosstrack', 'depthoffset', 'frequency', 'mode', 'modetwo']])
                elif ky == 'Ping Period Test':
                    data_is_there = np.all([x in self.fqpr.multibeam.raw_ping[0] for x in ['depthoffset', 'frequency', 'mode', 'modetwo']])

                if data_is_there:
                    self.plot_button.setEnabled(True)
                else:
                    self.data_widget.warning_message.setText('{}: Unable to find the necessary data to produce this plot')

    def mode_changed(self):
        """
        Triggered when changing the mode dropdown
        """

        if self.datasets:
            plottype = self.plot_type_dropdown.currentText()
            mode = self.mode_dropdown.currentText()
            if plottype in ['Extinction Test', 'Ping Period Test']:
                if mode in ['Plot Extinction by Frequency', 'Plot Period by Frequency']:
                    self.roundedfreq.show()
                else:
                    self.roundedfreq.hide()
                self.extinction_binsize.show()
                self.extinction_binsizelabel.show()
                if plottype == 'Extinction Test':
                    self.extinction_onlycomplete.show()
                else:
                    self.extinction_onlycomplete.hide()
            else:
                self.extinction_binsize.hide()
                self.extinction_binsizelabel.hide()
                self.extinction_onlycomplete.hide()
                self.roundedfreq.hide()

        self.load_helptext()
        self.update_ping_warnings()

    def load_helptext(self):
        """
        Get the list of available plots for the provided variable/dataset
        """
        if self.datasets:
            plottype = self.plot_type_dropdown.currentText()
            mode = self.mode_dropdown.currentText()
            plottype_expl = ''
            mode_expl = ''

            if plottype == 'Wobble Test':
                plottype_expl = "Implementation of 'Dynamic Motion Residuals in Swath Sonar Data: Ironing out the Creases' using Kluster processed"
                plottype_expl += " multibeam data.\nhttp://www.omg.unb.ca/omg/papers/Lect_26_paper_ihr03.pdf\n\nWobbleTest will generate the high"
                plottype_expl += " pass filtered mean depth and ping-wise slope, and build the correlation plots as described in the paper."
                plottype_expl += "\nRecommend the user start with 'Allowable Percent Deviation' to find the suitable time range."
                if mode == 'Dashboard':
                    mode_expl = 'Plot the full suite of plots.  See the allowable percent deviation to ensure that your dataset is flat enough for this to work.'
                elif mode == 'Allowable Percent Deviation':
                    mode_expl = 'Plot the correlation plot between ping time and percent deviation in the ping slope linear regression.  Percent'
                    mode_expl += ' deviation here is related to the standard error of the y in the regression.  Include bounds for invalid data'
                    mode_expl += ' in the plot as a filled in red area.  According to source paper, greater than 5% should be rejected.'
                elif mode == 'Attitude Scaling One':
                    mode_expl = "Correlation between filtered ping slope and roll, should signify sensor scaling issues, really shouldn't be present with modern survey systems."
                    mode_expl += "\nIf scaling_one and scaling_two have your artifact, its probably a scaling issue. Otherwise, if the plots are different, it"
                    mode_expl += " is most likely sound speed.  Inner swath and outer swath will differ as the swath is curved"
                elif mode == 'Attitude Scaling Two':
                    mode_expl = "Correlation between trimmed ping slope and roll, can signify possible surface sound speed issues.  When the soundspeed"
                    mode_expl += " at the face is incorrect, roll angles will introduce steering angle error, so your beampointingangle will be off."
                    mode_expl += "  As the roll changes, the error will change, making this a dynamic error that is correlated with roll."
                elif mode == 'Attitude Latency':
                    mode_expl = "Plot to determine the attitude latency either in the attitude system initial processing or the transmission to the sonar."
                    mode_expl += " We use roll just because it is the most sensitive, most easy to notice.  It's a linear tilt we are looking for,"
                    mode_expl += " so the timing latency would be equal to the slope of the regression of roll rate vs ping slope."
                elif mode == 'Yaw Alignment':
                    mode_expl = "Plot to determine the misalignment between roll/pitch and heading.  For us, the attitude/navigation system is a tightly "
                    mode_expl += "coupled system that provides these three data streams, so there really shouldn't be any yaw misalignment with roll/pitch."
                elif mode == 'X (Forward) Sonar Offset':
                    mode_expl = "Plot to find the x lever arm error, which is determined by looking at the correlation between filtered depth"
                    mode_expl += " and pitch.  X lever arm error affects the induced heave by the following equation:\nInduced Heave Error = -x_error * sin(pitch)"
                elif mode == 'Y (Starboard) Sonar Offset':
                    mode_expl = "Plot to find the y lever arm error, which is determined by looking at the correlation between filtered depth"
                    mode_expl += " and roll.  Y lever arm error affects the induced heave by the following equation:\nInduced Heave Error (y) = y_error * sin(roll) * cos(pitch)"
                elif mode == 'Heave Sound Speed One':
                    mode_expl = "Plot to find error associated with heaving through sound speed layers.  For flat face sonar that are mostly"
                    mode_expl += " level while receiving, this affect should be minimal.  If I'm understanding this correctly, it's because the"
                    mode_expl += " system is actively steering the beams using the surface sv sensor.  For barrel arrays, there is no active"
                    mode_expl += " beam steering so there will be an error in the beam angles."
                elif mode == 'Heave Sound Speed Two':
                    mode_expl = "See Heave Sound Speed One.  There are two plots for the port/starboard swaths.  You need two as the"
                    mode_expl += " swath artifact is a smile/frown, so the two plots should be mirror images if the artifact exists.  A full"
                    mode_expl += " swath analysis would not show this."
            elif plottype == 'Extinction Test':
                plottype_expl = "Plot the outermost sound velocity corrected alongtrack/depth offsets to give a sense of the maximum swath"
                plottype_expl += " coverage versus depth.  Useful for operational planning where you can think to yourself, 'At 50 meters"
                plottype_expl += " depth, I can expect about 4 x 50 meters coverage (4x water depth)'"
                if mode == 'Plot Extinction by Frequency':
                    mode_expl = 'Group the plot by frequency, check the rounded frequency option to plot to the nearest kHz'
                elif mode == 'Plot Extinction by Mode':
                    mode_expl = "Group the plot by primary mode, if you don't see the groupings you expect, try the sub mode option"
                elif mode == 'Plot Extinction by Sub Mode':
                    mode_expl = "Group the plot by secondary mode, if you don't see the groupings you expect, try the mode option"
            elif plottype == 'Ping Period Test':
                plottype_expl = "Plot the period of the pings binned by depth.  Illustrates the increase in ping period as depth increases."
                plottype_expl += " Gets some odd results with dual swath/dual head sonar.  We try to plot the rolling mean of the ping period"
                plottype_expl += " in these cases."
                if mode == 'Plot Period by Frequency':
                    mode_expl = 'Group the plot by frequency, check the rounded frequency option to plot to the nearest Hz'
                elif mode == 'Plot Period by Mode':
                    mode_expl = "Group the plot by primary mode, if you don't see the groupings you expect, try the sub mode option"
                elif mode == 'Plot Period by Sub Mode':
                    mode_expl = "Group the plot by secondary mode, if you don't see the groupings you expect, try the mode option"

            if plottype_expl and mode_expl:
                self.explanation.setText('{}\n\n{}'.format(plottype_expl, mode_expl))
            else:
                self.explanation.setText('')

    def plot(self):
        """
        Build out the data that we plan to plot
        """
        if self.datasets:
            min_max = self.data_widget.return_trim_times()
            if min_max:
                self.fqpr.subset_by_time(min_max[0], min_max[1])
            self.reload_datasets()
            plottype = self.plot_type_dropdown.currentText()
            mode = self.mode_dropdown.currentText()

            if plottype == 'Wobble Test':
                if self.needs_rebuilding or self.wobble is None:
                    self.wobble = wobble.WobbleTest(self.fqpr)
                    self.wobble.generate_starting_data()
                    self.needs_rebuilding = False
                if mode == 'Dashboard':
                    self.wobble.plot_correlation_table()
                elif mode == 'Allowable Percent Deviation':
                    self.wobble.plot_allowable_percent_deviation()
                elif mode == 'Attitude Scaling One':
                    self.wobble.plot_attitude_scaling_one()
                elif mode == 'Attitude Scaling Two':
                    self.wobble.plot_attitude_scaling_two()
                elif mode == 'Attitude Latency':
                    self.wobble.plot_attitude_latency()
                elif mode == 'Yaw Alignment':
                    self.wobble.plot_yaw_alignment()
                elif mode == 'X (Forward) Sonar Offset':
                    self.wobble.plot_x_lever_arm_error()
                elif mode == 'Y (Starboard) Sonar Offset':
                    self.wobble.plot_y_lever_arm_error()
                elif mode == 'Heave Sound Speed One':
                    self.wobble.plot_heave_sound_speed_one()
                elif mode == 'Heave Sound Speed Two':
                    self.wobble.plot_heave_sound_speed_two()
            elif plottype == 'Extinction Test':
                try:
                    binsize = float(self.extinction_binsize.text())
                except:
                    self.data_widget.warning_message.setText('ERROR: Bin Size must be a number, found {}'.format(self.extinction_binsize.text()))
                    return
                if self.needs_rebuilding or self.extinction is None:
                    self.extinction = sat.ExtinctionTest(self.fqpr, round_frequency=self.roundedfreq.isChecked())
                    self.needs_rebuilding = False
                if mode == 'Plot Extinction by Frequency':
                    self.extinction.plot(mode='frequency', depth_bin_size=binsize, filter_incomplete_swaths=self.extinction_onlycomplete.isChecked())
                elif mode == 'Plot Extinction by Mode':
                    self.extinction.plot(mode='mode', depth_bin_size=binsize, filter_incomplete_swaths=self.extinction_onlycomplete.isChecked())
                elif mode == 'Plot Extinction by Sub Mode':
                    self.extinction.plot(mode='modetwo', depth_bin_size=binsize, filter_incomplete_swaths=self.extinction_onlycomplete.isChecked())
            elif plottype == 'Ping Period Test':
                try:
                    binsize = float(self.extinction_binsize.text())
                except:
                    self.data_widget.warning_message.setText('ERROR: Bin Size must be a number, found {}'.format(self.extinction_binsize.text()))
                    return
                if self.needs_rebuilding or self.period is None:
                    self.period = sat.PingPeriodTest(self.fqpr, round_frequency=self.roundedfreq.isChecked())
                    self.needs_rebuilding = False
                if mode == 'Plot Period by Frequency':
                    self.period.plot(mode='frequency', depth_bin_size=binsize)
                elif mode == 'Plot Period by Mode':
                    self.period.plot(mode='mode', depth_bin_size=binsize)
                elif mode == 'Plot Period by Sub Mode':
                    self.period.plot(mode='modetwo', depth_bin_size=binsize)

            if min_max:
                self.fqpr.restore_subset()

    def new_ping_count(self, ping_count: int):
        """
        We check some plottypes to ensure the number of pings provided makes sense

        Parameters
        ----------
        ping_count
            total number of pings that we plan on plotting
        """

        self.current_ping_count = ping_count
        if self.datasets:
            self.needs_rebuilding = True
            self.update_ping_warnings()

    def update_ping_warnings(self):
        """
        On changing the plot type or the current ping count, we update the warnings that are based on ping count for that plot
        """

        ping_count = self.current_ping_count
        if self.datasets:
            plottype = self.plot_type_dropdown.currentText()
            if plottype == 'Wobble Test' and ping_count > 3000:
                self.data_widget.warning_message.setText('Warning: WobbleTest will be very slow with greater than 3000 pings')
            elif plottype == 'Wobble Test' and self.fqpr.multibeam.is_dual_head():
                self.data_widget.warning_message.setText('Warning: Dual Head - Each head will be treated as the port/starboard swath, provides some odd results')
            else:
                self.data_widget.warning_message.setText('')


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = AdvancedPlotDialog()
    dlog.show()
    app.exec_()
