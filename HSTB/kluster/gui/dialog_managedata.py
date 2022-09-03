import os
from datetime import datetime, timezone
import logging

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import ManageDialog
from matplotlib.backends.backend_qt5agg import (FigureCanvasQTAgg, NavigationToolbar2QT as NavigationToolbar)
import matplotlib.pyplot as plt


class ManageDataDialog(ManageDialog):
    """
    Dialog contains a summary of the Fqpr data and some options for altering the data contained within.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    refresh_fqpr = Signal(object, object)

    def __init__(self, parent=None, settings=None):
        super().__init__(parent, 'Manage Surface', 'ManageSurfaceDialog', settings)

        self.calcdropdown.addItems(['distance, meters', 'distance, lnm'])
        self.rundropdown.addItems(['SVP Editor', 'SVP Map', 'Remove SBET'])
        self.fqpr = None
        self.svpdialog = None

    def populate(self, fqpr):
        self.fqpr = fqpr
        self.managelabel.setText('Manage: {}'.format(os.path.split(fqpr.output_folder)[1]))
        self.basicdata.setText(fqpr.__repr__())

    def remove_sbet(self, e):
        if self.fqpr is not None:
            if 'nav_files' in self.fqpr.multibeam.raw_ping[0].attrs:
                self.set_below()
                newstate = RemoveSBETDialog(list(self.fqpr.multibeam.raw_ping[0].nav_files.keys())).run()
                if newstate:
                    self.fqpr.remove_post_processed_navigation()
                    self.refresh_fqpr.emit(self.fqpr, self)
                self.set_on_top()
            else:
                self.print('No SBET files found', logging.ERROR)
        else:
            self.print('No data found', logging.ERROR)

    def remove_svp(self, profilename):
        if self.svpdialog is not None:
            if self.svpdialog.number_of_profiles > 1:
                self.set_below()
                newstate = RemoveSVPDialog(profilename).run()
                if newstate:
                    self.fqpr.remove_profile(profilename)
                    self.svpdialog.remove_data_at_index(profilename)
                    self.refresh_fqpr.emit(self.fqpr, self)
                self.set_on_top()
            else:
                self.print('Unable to remove the last profile of a dataset', logging.ERROR)
        else:
            self.print('WARNING: Unable to find svp data dialog', logging.ERROR)

    def manage_svp(self, e):
        if self.fqpr is not None:
            profnames, casts, cast_times, castlocations = self.fqpr.return_all_profiles()
            if profnames is not None:
                self.svpdialog = None
                self.svpdialog = ManageSVPDialog(profnames, casts, cast_times, castlocations)
                self.svpdialog.remove_cast_sig.connect(self.remove_svp)
                self.svpdialog.exec_()
            else:
                print('No profiles found!')

    def svp_map_display(self, e):
        if self.fqpr is not None:
            self.fqpr.plot.plot_sound_velocity_map()
            # set always on top
            plt.gcf().canvas.manager.window.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
            plt.gcf().canvas.manager.window.show()

    def plot_tooltip_config(self, e):
        pass

    def calc_tooltip_config(self, e):
        calcname = self.calcdropdown.currentText()
        if calcname == 'distance, meters':
            self.calcdropdown.setToolTip('Calculate the total distance of all lines in the container in meters')
        elif calcname == 'distance, lnm':
            self.calcdropdown.setToolTip('Calculate the total distance of all lines in the container in linear nautical miles')

    def run_tooltip_config(self, e):
        runname = self.rundropdown.currentText()
        if runname == 'SVP Editor':
            self.rundropdown.setToolTip('Run the SVP Editor on all casts in this container.  Removing a cast may create a new processing action.')
        elif runname == 'SVP Map':
            self.rundropdown.setToolTip('Build an image of all lines and sound velocity cast locations')
        elif runname == 'Remove SBET':
            self.rundropdown.setToolTip('Remove the SBET(s) currently contained in this container.  May create a new processing action.')

    def calculate_statistic(self, e):
        if self.fqpr is not None:
            stat = self.calcdropdown.currentText()
            if stat in ['distance, meters', 'distance, lnm']:
                dist = self.fqpr.total_distance_meters
            if stat == 'distance, meters':
                self.calcanswer.setText(str(round(dist, 3)))
            elif stat == 'distance, lnm':
                self.calcanswer.setText(str(round(dist * 0.000539957, 3)))
            else:
                raise ValueError(f'Unrecognized input for calculating statistic: {stat}')

    def generate_plot(self, e):
        pass

    def run_function(self, e):
        if self.rundropdown.currentText() == 'SVP Editor':
            self.manage_svp(e)
        elif self.rundropdown.currentText() == 'SVP Map':
            self.svp_map_display(e)
        elif self.rundropdown.currentText() == 'Remove SBET':
            self.remove_sbet(e)


class RemoveSBETDialog(QtWidgets.QMessageBox):
    def __init__(self, navfiles: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Remove SBET')
        self.setText('Remove all SBETs and associated error data from this converted instance?\n\nCurrently includes:\n{}'.format('\n'.join(navfiles)) +
                     '\n\nWARNING: Removing SBETs will generate a new georeferencing action to reprocess with multibeam navigation.')
        self.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        self.setDefaultButton(QtWidgets.QMessageBox.Yes)
        self.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)

    def run(self):
        result = self.exec_()
        if result == QtWidgets.QMessageBox.No:
            return False
        else:
            return True


class RemoveSVPDialog(QtWidgets.QMessageBox):
    def __init__(self, profile_name: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Remove Sound Velocity Profile')
        self.setText('Remove the following sound velocity profile?\n\n{}'.format(profile_name) +
                     '\n\nWARNING: Removing profiles will generate a new sound velocity action to reprocess with the remaining casts.')
        self.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        self.setDefaultButton(QtWidgets.QMessageBox.Yes)
        self.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)

    def run(self):
        result = self.exec_()
        if result == QtWidgets.QMessageBox.No:
            return False
        else:
            return True


class MplCanvas(FigureCanvasQTAgg):

    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig = plt.Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super(MplCanvas, self).__init__(self.fig)
        self.setParent(parent)
        self.parent = parent

    def plot(self, svvalues, depthvalues, profname, title):
        self.axes.clear()
        self.axes.plot(svvalues, depthvalues, label=profname)
        self.axes.set_title('Sound Velocity Profile, ' + title)
        self.axes.set_xlabel('Sound Velocity (meters/second)')
        self.axes.set_ylabel('Depth (meters)')
        self.axes.invert_yaxis()
        self.axes.legend()
        self.draw()


class ManageSVPDialog(QtWidgets.QDialog):
    """
    Dialog for managing the sound velocity profiles currently within this kluster converted data instance
    """
    remove_cast_sig = Signal(str)

    def __init__(self, profnames, casts, cast_times, castlocations, parent=None):
        super().__init__(parent)
        self.profnames = profnames  # list of profile names
        self.casts = casts  # list of [depth values, sv values] for each profile
        self.cast_times = cast_times  # list of times in utc seconds for each profile
        self.cast_locations = castlocations  # list of [latitude, longitude] for each profile

        self.setWindowTitle('Manage Sound Velocity Profiles')
        self.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)

        layout = QtWidgets.QVBoxLayout()

        self.sc = MplCanvas(self, width=7, height=5, dpi=100)
        toolbar = NavigationToolbar(self.sc, self)

        utclayout = QtWidgets.QHBoxLayout()
        utctimelbl = QtWidgets.QLabel('UTC Timestamp: ')
        utclayout.addWidget(utctimelbl)
        self.utc_time = QtWidgets.QLineEdit()
        self.utc_time.setReadOnly(True)
        utclayout.addWidget(self.utc_time)
        utclayout.addStretch()

        utcdatelayout = QtWidgets.QHBoxLayout()
        utcdatelbl = QtWidgets.QLabel('UTC Date: ')
        utcdatelayout.addWidget(utcdatelbl)
        self.utc_date = QtWidgets.QLineEdit()
        self.utc_date.setReadOnly(True)
        utcdatelayout.addWidget(self.utc_date)
        utcdatelayout.addStretch()

        localdatelayout = QtWidgets.QHBoxLayout()
        localdatelbl = QtWidgets.QLabel('Local Date: ')
        localdatelayout.addWidget(localdatelbl)
        self.local_date = QtWidgets.QLineEdit()
        self.local_date.setReadOnly(True)
        localdatelayout.addWidget(self.local_date)
        localdatelayout.addStretch()

        castlocationlayout = QtWidgets.QHBoxLayout()
        castloclabel = QtWidgets.QLabel('Cast Location (latitude, longitude): ')
        castlocationlayout.addWidget(castloclabel)
        self.cast_location_lat = QtWidgets.QLineEdit()
        self.cast_location_lat.setReadOnly(True)
        castlocationlayout.addWidget(self.cast_location_lat)
        self.cast_location_lon = QtWidgets.QLineEdit()
        self.cast_location_lon.setReadOnly(True)
        castlocationlayout.addWidget(self.cast_location_lon)
        castlocationlayout.addStretch()

        buttonlayout = QtWidgets.QHBoxLayout()
        buttonlayout.addStretch()
        self.previousbutton = QtWidgets.QPushButton('Previous')
        buttonlayout.addWidget(self.previousbutton)
        buttonlayout.addStretch()
        self.removebutton = QtWidgets.QPushButton('Delete')
        buttonlayout.addWidget(self.removebutton)
        buttonlayout.addStretch()
        self.nextbutton = QtWidgets.QPushButton('Next')
        buttonlayout.addWidget(self.nextbutton)
        buttonlayout.addStretch()

        layout.addWidget(toolbar)
        layout.addWidget(self.sc)
        layout.addLayout(utclayout)
        layout.addLayout(utcdatelayout)
        layout.addLayout(localdatelayout)
        layout.addLayout(castlocationlayout)
        layout.addLayout(buttonlayout)
        self.setLayout(layout)

        self.previousbutton.clicked.connect(self.previous_cast)
        self.nextbutton.clicked.connect(self.advance_cast)
        self.removebutton.clicked.connect(self.remove_cast)

        self.cur_index = 0
        self.plot_cast()

    @property
    def number_of_profiles(self):
        return len(self.profnames)

    def remove_data_at_index(self, profile_name):
        index = self.profnames.index(profile_name)
        self.profnames.pop(index)
        self.casts.pop(index)
        self.cast_times.pop(index)
        self.cast_locations.pop(index)
        if index == self.cur_index:
            self.advance_cast()

    def advance_cast(self):
        newindex = self.cur_index + 1
        if newindex >= len(self.profnames):
            self.cur_index = 0
        else:
            self.cur_index = newindex
        self.plot_cast()

    def remove_cast(self):
        self.remove_cast_sig.emit(str(self.profnames[self.cur_index]))

    def previous_cast(self):
        newindex = self.cur_index - 1
        if newindex == -1:
            self.cur_index = len(self.profnames) - 1
        else:
            self.cur_index = newindex
        self.plot_cast()

    def plot_cast(self):
        index = self.cur_index
        self.utc_time.setText(str(self.cast_times[index]))
        self.utc_date.setText(datetime.fromtimestamp(float(self.cast_times[index]), tz=timezone.utc).strftime('%c'))
        self.local_date.setText(datetime.fromtimestamp(float(self.cast_times[index]), tz=timezone.utc).astimezone(datetime.now().astimezone().tzinfo).strftime('%c'))
        self.cast_location_lat.setText(str(self.cast_locations[index][0]))
        self.cast_location_lon.setText(str(self.cast_locations[index][1]))
        self.sc.plot(self.casts[index][1], self.casts[index][0], self.profnames[index], 'Cast #{}'.format(index + 1))


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManageDataDialog()
    # from HSTB.kluster.fqpr_convenience import reload_data
    # dlog.populate(reload_data(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040", skip_dask=True))
    dlog.show()
    app.exec_()
