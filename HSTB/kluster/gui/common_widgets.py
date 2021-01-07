import sys
import os
import numpy as np
from PySide2 import QtWidgets, QtGui, QtCore
from datetime import datetime, timezone

from HSTB.shared import RegistryHelpers
from HSTB.kluster.fqpr_convenience import reload_data


# Current widgets that might be of interest:
#   BrowseListWidget - You need a list widget with browse buttons and removing of items built in?  Check this out


class PlotDataHandler(QtWidgets.QWidget):
    """
    Widget allowing the user to provide a directory of kluster converted data and specify a time range in a number of
    different ways.
    - specify time range by manually by sliding the rangeslider handles around
    - specify time by typing in the min time, max time
    - specify time by selecting the line you are interested in
    """
    fqpr_loaded = QtCore.Signal(bool)
    ping_count_changed = QtCore.Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.fqpr = None
        self.fqpr_path = None
        self.fqpr_mintime = 0
        self.fqpr_maxtime = 0
        self.fqpr_line_dict = None
        self.slider_mintime = 0
        self.slider_maxtime = 0
        self.translate_time = False

        self.setWindowTitle('Basic Plot')
        layout = QtWidgets.QVBoxLayout()

        self.start_msg = QtWidgets.QLabel('Select the converted data to plot (a converted folder):')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('', self)
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout_one.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one.addWidget(self.browse_button)

        self.trim_time_check = QtWidgets.QGroupBox('Trim by time')
        self.trim_time_check.setCheckable(True)
        self.trim_time_check.setChecked(False)
        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.trim_time_start_lbl = QtWidgets.QLabel('Start time (utc seconds)')
        self.hlayout_two.addWidget(self.trim_time_start_lbl)
        self.trim_time_start = QtWidgets.QLineEdit('', self)
        self.hlayout_two.addWidget(self.trim_time_start)
        self.trim_time_end_lbl = QtWidgets.QLabel('End time (utc seconds)')
        self.hlayout_two.addWidget(self.trim_time_end_lbl)
        self.trim_time_end = QtWidgets.QLineEdit('', self)
        self.hlayout_two.addWidget(self.trim_time_end)
        self.trim_time_datetime_start_lbl = QtWidgets.QLabel('Start time (utc)')
        self.trim_time_datetime_start_lbl.hide()
        self.hlayout_two.addWidget(self.trim_time_datetime_start_lbl)
        self.trim_time_datetime_start = QtWidgets.QDateTimeEdit(self)
        self.trim_time_datetime_start.setDisplayFormat("MM/dd/yyyy hh:mm:ss")
        self.trim_time_datetime_start.hide()
        self.hlayout_two.addWidget(self.trim_time_datetime_start)
        self.trim_time_datetime_end_lbl = QtWidgets.QLabel('End time (utc)')
        self.trim_time_datetime_end_lbl.hide()
        self.hlayout_two.addWidget(self.trim_time_datetime_end_lbl)
        self.trim_time_datetime_end = QtWidgets.QDateTimeEdit(self)
        self.trim_time_datetime_end.setDisplayFormat("MM/dd/yyyy hh:mm:ss")
        self.trim_time_datetime_end.hide()
        self.hlayout_two.addWidget(self.trim_time_datetime_end)
        self.hlayout_two.addStretch()
        self.trim_time_check.setLayout(self.hlayout_two)

        self.trim_line_check = QtWidgets.QGroupBox('Trim by line')
        self.trim_line_check.setCheckable(True)
        self.trim_line_check.setChecked(False)
        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.trim_lines_lbl = QtWidgets.QLabel('Line Name')
        self.hlayout_three.addWidget(self.trim_lines_lbl)
        self.trim_lines = QtWidgets.QComboBox(self)
        self.trim_lines.setMinimumWidth(350)
        self.hlayout_three.addWidget(self.trim_lines)
        self.trim_line_check.setLayout(self.hlayout_three)

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.ping_count_label = QtWidgets.QLabel('Ping count')
        self.hlayout_four.addWidget(self.ping_count_label)
        self.ping_count = QtWidgets.QLineEdit('', self)
        self.ping_count.setMinimumWidth(80)
        self.ping_count.setReadOnly(True)
        self.hlayout_four.addWidget(self.ping_count)
        self.time_as_label = QtWidgets.QLabel('Time as')
        self.hlayout_four.addWidget(self.time_as_label)
        self.time_as_dropdown = QtWidgets.QComboBox(self)
        self.time_as_dropdown.addItems(['utc seconds', 'utc datetime'])
        self.hlayout_four.addWidget(self.time_as_dropdown)
        self.hlayout_four.addStretch(2)

        self.hlayout_four_one = QtWidgets.QHBoxLayout()
        self.display_start_time = QtWidgets.QLabel('0.0', self)
        self.hlayout_four_one.addWidget(self.display_start_time)
        self.hlayout_four_one.addStretch()
        self.display_range = QtWidgets.QLabel('(0.0, 0.0)', self)
        self.hlayout_four_one.addWidget(self.display_range)
        self.hlayout_four_one.addStretch()
        self.display_end_time = QtWidgets.QLabel('0.0', self)
        self.hlayout_four_one.addWidget(self.display_end_time)

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.sliderbar = RangeSlider(self)
        self.sliderbar.setTickInterval(1000)
        self.sliderbar.setRangeLimit(0, 1000)
        self.sliderbar.setRange(20, 200)
        self.hlayout_five.addWidget(self.sliderbar)

        self.hlayout_six = QtWidgets.QHBoxLayout()
        self.warning_message = QtWidgets.QLabel('', self)
        self.warning_message.setStyleSheet("color: red;")
        self.hlayout_six.addWidget(self.warning_message)

        layout.addWidget(self.start_msg)
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.trim_time_check)
        layout.addWidget(self.trim_line_check)
        layout.addLayout(self.hlayout_four)
        layout.addLayout(self.hlayout_four_one)
        layout.addLayout(self.hlayout_five)
        layout.addLayout(self.hlayout_six)
        self.setLayout(layout)

        self.browse_button.clicked.connect(self.file_browse)
        self.sliderbar.mouse_move.connect(self.update_from_slider)
        self.trim_time_start.textChanged.connect(self.update_from_trim_time)
        self.trim_time_end.textChanged.connect(self.update_from_trim_time)
        self.trim_time_datetime_start.dateTimeChanged.connect(self.update_from_trim_datetime)
        self.trim_time_datetime_end.dateTimeChanged.connect(self.update_from_trim_datetime)
        self.trim_lines.currentTextChanged.connect(self.update_from_line)
        self.trim_time_check.toggled.connect(self.trim_time_toggled)
        self.trim_line_check.toggled.connect(self.trim_line_toggled)
        self.time_as_dropdown.currentTextChanged.connect(self.update_translate_mode)

    def file_browse(self):
        """
        Browse to a Kluster converted data folder.  Structure should look something like:

        C:\collab\dasktest\data_dir\kmall_test\mbes\converted
        C:\collab\dasktest\data_dir\kmall_test\mbes\converted\attitude.zarr
        C:\collab\dasktest\data_dir\kmall_test\mbes\converted\navigation.zarr
        C:\collab\dasktest\data_dir\kmall_test\mbes\converted\ping_53011.zarr

        You would point at the converted folder using this browse button.
        """
        # dirpath will be None or a string
        msg, fqpr_path = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                          Title='Select converted data directory',
                                                          AppName='\\reghelp')
        if fqpr_path:
            self.new_fqpr_path(fqpr_path)
            self.initialize_controls()

    def update_from_slider(self, first_pos, second_pos):
        """
        Using the slider, we update the printed time in the widget
        """
        if self.fqpr is not None:
            self.slider_mintime = self.fqpr_mintime + first_pos
            self.slider_maxtime = self.fqpr_mintime + second_pos
            totalpings = self.fqpr.return_total_pings(self.slider_mintime, self.slider_maxtime)
            self._set_display_range(self.slider_mintime, self.slider_maxtime)
            self.ping_count.setText(str(totalpings))
            if totalpings == 0:
                self.warning_message.setText('ERROR: Found 0 total pings for this time range')
            else:
                self.ping_count_changed.emit(totalpings)

    def update_from_trim_time(self, e):
        """
        User typed in a new time range
        """
        if self.fqpr is not None and self.trim_time_check.isChecked():
            try:
                set_mintime = int(float(self.trim_time_start.text()))
                if not self.fqpr_maxtime >= set_mintime >= self.fqpr_mintime:
                    self.warning_message.setText('Invalid start time, must be inbetween max and minimum time')
                    return
            except ValueError:
                self.warning_message.setText(
                    'Invalid start time, must be a number: {}'.format(self.trim_time_start.text()))
                return
            try:
                set_maxtime = int(float(self.trim_time_end.text()))
                if not self.fqpr_maxtime >= set_maxtime >= self.fqpr_mintime:
                    self.warning_message.setText('Invalid end time, must be inbetween max and minimum time')
                    return
            except ValueError:
                self.warning_message.setText('Invalid end time, must be a number: {}'.format(self.trim_time_end.text()))
                return

            self.warning_message.setText('')
            self._set_new_times(set_mintime, set_maxtime)

    def update_from_trim_datetime(self, e):
        if self.fqpr is not None and self.trim_time_check.isChecked():
            try:
                set_datetime = self.trim_time_datetime_start.dateTime().toPython()
                set_datetime = set_datetime.replace(tzinfo=timezone.utc)
                set_mintime = int(float(set_datetime.timestamp()))
                if not self.fqpr_maxtime >= set_mintime >= self.fqpr_mintime:
                    self.warning_message.setText('Invalid start time, must be inbetween max and minimum time')
                    return
            except ValueError:
                self.warning_message.setText(
                    'Invalid start time, must be a number: {}'.format(self.trim_time_start.text()))
                return
            try:
                set_datetime = self.trim_time_datetime_end.dateTime().toPython()
                set_datetime = set_datetime.replace(tzinfo=timezone.utc)
                set_maxtime = int(float(set_datetime.timestamp()))
                if not self.fqpr_maxtime >= set_maxtime >= self.fqpr_mintime:
                    self.warning_message.setText('Invalid end time, must be inbetween max and minimum time')
                    return
            except ValueError:
                self.warning_message.setText('Invalid end time, must be a number: {}'.format(self.trim_time_end.text()))
                return

            self.warning_message.setText('')
            self._set_new_times(set_mintime, set_maxtime)

    def trim_time_toggled(self, state):
        if state:
            self.trim_line_check.setChecked(False)
            starttme = self.slider_mintime
            endtme = self.slider_maxtime
            self.trim_time_start.setText(str(starttme))
            self.trim_time_end.setText(str(endtme))
            self.trim_time_datetime_start.setDateTime(QtCore.QDateTime.fromSecsSinceEpoch(int(starttme), QtCore.QTimeZone(0)))
            self.trim_time_datetime_end.setDateTime(QtCore.QDateTime.fromSecsSinceEpoch(int(endtme), QtCore.QTimeZone(0)))

    def trim_line_toggled(self, state):
        if state:
            self.trim_time_check.setChecked(False)
            self.update_from_line(None)

    def _set_new_times(self, starttime, endtime):
        """
        Set the slider range and the associated text controls
        """
        set_minslider_position = int(starttime - self.fqpr_mintime)
        set_maxslider_position = int(endtime - self.fqpr_mintime)

        self.sliderbar.setRange(set_minslider_position, set_maxslider_position)
        self.slider_mintime = starttime
        self.slider_maxtime = endtime
        self._set_display_range(self.slider_mintime, self.slider_maxtime)
        pingcount = int(self.fqpr.return_total_pings(self.slider_mintime, self.slider_maxtime))
        self.ping_count.setText(str(pingcount))
        self.ping_count_changed.emit(pingcount)

    def _set_display_range(self, mintime, maxtime):
        if self.translate_time:
            self.display_range.setText(str('({}, {})'.format(datetime.fromtimestamp(mintime, tz=timezone.utc).strftime('%c'),
                                                             datetime.fromtimestamp(maxtime, tz=timezone.utc).strftime('%c'))))
        else:
            self.display_range.setText(str('({}, {})'.format(mintime, maxtime)))

    def _set_display_minmax(self, mintime, maxtime):
        if self.translate_time:
            self.display_start_time.setText(datetime.fromtimestamp(mintime, tz=timezone.utc).strftime('%c'))
            self.display_end_time.setText(datetime.fromtimestamp(maxtime, tz=timezone.utc).strftime('%c'))
        else:
            self.display_start_time.setText(str(mintime))
            self.display_end_time.setText(str(maxtime))

    def update_from_line(self, e):
        """
        User selected a line to trim the times by
        """

        if self.fqpr is not None and self.trim_line_check.isChecked():
            linename = self.trim_lines.currentText()
            if self.fqpr_line_dict is not None and linename:
                linetimes = self.fqpr_line_dict[linename]
                self.warning_message.setText('')
                self._set_new_times(linetimes[0], linetimes[1])

    def update_translate_mode(self, mode):
        if mode == 'utc seconds':
            self.trim_time_datetime_start_lbl.hide()
            self.trim_time_start_lbl.show()
            self.trim_time_datetime_start.hide()
            self.trim_time_start.show()
            self.trim_time_datetime_end_lbl.hide()
            self.trim_time_end_lbl.show()
            self.trim_time_datetime_end.hide()
            self.trim_time_end.show()
            self.translate_time = False
        elif mode == 'utc datetime':
            self.trim_time_datetime_start_lbl.show()
            self.trim_time_start_lbl.hide()
            self.trim_time_datetime_start.show()
            self.trim_time_start.hide()
            self.trim_time_datetime_end_lbl.show()
            self.trim_time_end_lbl.hide()
            self.trim_time_datetime_end.show()
            self.trim_time_end.hide()
            self.translate_time = True

        if self.fqpr is not None:
            self._set_display_range(self.slider_mintime, self.slider_maxtime)
            self._set_display_minmax(self.fqpr_mintime, self.fqpr_maxtime)

    def new_fqpr_path(self, fqpr_path):
        """
        User selected a new fqpr instance (fqpr = the converted datastore, see file_browse)
        """
        try:
            self.fqpr = reload_data(fqpr_path, skip_dask=True)
            self.fil_text.setText(fqpr_path)

            if self.fqpr is not None:
                self.fqpr_path = fqpr_path
            else:
                self.fqpr_path = None
                self.warning_message.setText('ERROR: Invalid path to converted data store')
        except:
            return

    def initialize_controls(self):
        """
        On start up, we initialize all the controls (or clear all controls if the fqpr provided was invalid)
        """
        if self.fqpr is not None:
            self.fqpr_mintime = int(np.floor(np.min([rp.time.values[0] for rp in self.fqpr.multibeam.raw_ping])))
            self.fqpr_maxtime = int(np.ceil(np.max([rp.time.values[-1] for rp in self.fqpr.multibeam.raw_ping])))
            self.slider_mintime = self.fqpr_mintime
            self.slider_maxtime = self.fqpr_maxtime

            self.sliderbar.setTickInterval(int(self.fqpr_maxtime - self.fqpr_mintime))
            self.sliderbar.setRangeLimit(0, self.fqpr_maxtime - self.fqpr_mintime)
            self.sliderbar.setRange(0, self.fqpr_maxtime - self.fqpr_mintime)
            self._set_display_range(self.slider_mintime, self.slider_maxtime)
            self._set_display_minmax(self.fqpr_mintime, self.fqpr_maxtime)

            self.trim_time_start.setText(str(self.fqpr_mintime))
            self.trim_time_end.setText(str(self.fqpr_maxtime))

            self.fqpr_line_dict = self.fqpr.multibeam.raw_ping[0].multibeam_files
            self.fqpr_line_dict = {t: [int(np.max([self.fqpr_mintime, self.fqpr_line_dict[t][0]])),
                                       int(np.min([self.fqpr_maxtime, np.ceil(self.fqpr_line_dict[t][1])]))] for t in
                                   self.fqpr_line_dict}

            self.trim_lines.clear()
            self.trim_lines.addItems(sorted(list(self.fqpr_line_dict.keys())))

            self.ping_count.setText(str(self.fqpr.return_total_pings()))

            self.fqpr_loaded.emit(True)
        else:
            self.fqpr_mintime = 0
            self.fqpr_maxtime = 0
            self.sliderbar.setTickInterval(1000)
            self.sliderbar.setRangeLimit(0, 1000)
            self.sliderbar.setRange(20, 200)

            self.display_start_time.setText('0.0')
            self.display_end_time.setText('0.0')
            self.display_range.setText('(0.0, 0.0)')

            self.trim_time_start.setText('')
            self.trim_time_end.setText('')

            self.fqpr_line_dict = None
            self.trim_lines.clear()
            self.ping_count.setText('')

            self.fqpr_loaded.emit(False)

    def return_trim_times(self):
        """
        Return the time range specified by one of the 3 ways to specify range, or None if there is no valid range
        """

        if np.abs(self.slider_mintime - self.fqpr_mintime) >= 1:
            valid_min = self.slider_mintime
        else:
            valid_min = self.fqpr_mintime

        if np.abs(self.slider_maxtime - self.fqpr_maxtime) >= 1:
            valid_max = self.slider_maxtime
        else:
            valid_max = self.fqpr_maxtime

        if (valid_max != self.fqpr_maxtime) or (valid_min != self.fqpr_mintime):
            return valid_min, valid_max
        else:
            return None


class RangeSlider(QtWidgets.QWidget):
    """
    Build a custom slider with two handles, allowing you to specify a range.  Utilize the QStyleOptionSlider
    widget to do so.
    """
    mouse_move = QtCore.Signal(int, int)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.first_position = 1
        self.second_position = 8

        self.opt = QtWidgets.QStyleOptionSlider()
        self.opt.minimum = 0
        self.opt.maximum = 10

        self.setTickPosition(QtWidgets.QSlider.TicksAbove)
        self.setTickInterval(1)

        self.setSizePolicy(
            QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Slider)
        )

    def setRangeLimit(self, minimum: int, maximum: int):
        """
        Set the maximum range of the slider bar
        """
        self.opt.minimum = minimum
        self.opt.maximum = maximum

    def setRange(self, start: int, end: int):
        """
        Set the position of the two handles, the range of the selection
        """
        self.first_position = start
        self.second_position = end
        self.update()

    def getRange(self):
        """
        Get the positions of the handles
        """
        return self.first_position, self.second_position

    def setTickPosition(self, position: QtWidgets.QSlider.TickPosition):
        self.opt.tickPosition = position

    def setTickInterval(self, ti: int):
        self.opt.tickInterval = ti

    def paintEvent(self, event: QtGui.QPaintEvent):

        painter = QtGui.QPainter(self)

        # Draw rule
        self.opt.initFrom(self)
        self.opt.rect = self.rect()
        self.opt.sliderPosition = 0
        self.opt.subControls = QtWidgets.QStyle.SC_SliderGroove | QtWidgets.QStyle.SC_SliderTickmarks

        #   Draw GROOVE
        self.style().drawComplexControl(QtWidgets.QStyle.CC_Slider, self.opt, painter)

        #  Draw INTERVAL

        color = self.palette().color(QtGui.QPalette.Highlight)
        color.setAlpha(160)
        painter.setBrush(QtGui.QBrush(color))
        painter.setPen(QtCore.Qt.NoPen)

        self.opt.sliderPosition = self.first_position
        x_left_handle = (
            self.style()
            .subControlRect(QtWidgets.QStyle.CC_Slider, self.opt, QtWidgets.QStyle.SC_SliderHandle)
            .right()
        )

        self.opt.sliderPosition = self.second_position
        x_right_handle = (
            self.style()
            .subControlRect(QtWidgets.QStyle.CC_Slider, self.opt, QtWidgets.QStyle.SC_SliderHandle)
            .left()
        )

        groove_rect = self.style().subControlRect(
            QtWidgets.QStyle.CC_Slider, self.opt, QtWidgets.QStyle.SC_SliderGroove
        )

        selection = QtCore.QRect(x_left_handle, groove_rect.y(), x_right_handle - x_left_handle, groove_rect.height(),).adjusted(-1, 1, 1, -1)

        painter.drawRect(selection)

        # Draw first handle

        self.opt.subControls = QtWidgets.QStyle.SC_SliderHandle
        self.opt.sliderPosition = self.first_position
        self.style().drawComplexControl(QtWidgets.QStyle.CC_Slider, self.opt, painter)

        # Draw second handle
        self.opt.sliderPosition = self.second_position
        self.style().drawComplexControl(QtWidgets.QStyle.CC_Slider, self.opt, painter)

    def mousePressEvent(self, event: QtGui.QMouseEvent):

        self.opt.sliderPosition = self.first_position
        self._first_sc = self.style().hitTestComplexControl(
            QtWidgets.QStyle.CC_Slider, self.opt, event.pos(), self
        )

        self.opt.sliderPosition = self.second_position
        self._second_sc = self.style().hitTestComplexControl(
            QtWidgets.QStyle.CC_Slider, self.opt, event.pos(), self
        )

    def mouseMoveEvent(self, event: QtGui.QMouseEvent):

        distance = self.opt.maximum - self.opt.minimum

        pos = self.style().sliderValueFromPosition(
            0, distance, event.pos().x(), self.rect().width()
        )

        if self._first_sc == QtWidgets.QStyle.SC_SliderHandle:
            if pos <= self.second_position:
                self.first_position = pos
                self.update()
                self.mouse_move.emit(self.first_position, self.second_position)
                return

        if self._second_sc == QtWidgets.QStyle.SC_SliderHandle:
            if pos >= self.first_position:
                self.second_position = pos
                self.update()
                self.mouse_move.emit(self.first_position, self.second_position)

    def sizeHint(self):
        """ override """
        SliderLength = 84
        TickSpace = 5

        w = SliderLength
        h = self.style().pixelMetric(QtWidgets.QStyle.PM_SliderThickness, self.opt, self)

        if (
            self.opt.tickPosition & QtWidgets.QSlider.TicksAbove
            or self.opt.tickPosition & QtWidgets.QSlider.TicksBelow
        ):
            h += TickSpace

        return (
            self.style()
            .sizeFromContents(QtWidgets.QStyle.CT_Slider, self.opt, QtCore.QSize(w, h), self)
            .expandedTo(QtWidgets.QApplication.globalStrut())
        )


class DeletableListWidget(QtWidgets.QListWidget):
    """
    Inherit from the ListWidget and allow the user to press delete or backspace key to remove items
    """
    files_updated = QtCore.Signal(bool)

    def __init__(self, *args, **kwrds):
        super().__init__(*args, **kwrds)
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)

    def keyReleaseEvent(self, event):
        if event.matches(QtGui.QKeySequence.Delete) or event.matches(QtGui.QKeySequence.Back):
            for itm in self.selectedItems():
                self.takeItem(self.row(itm))
        self.files_updated.emit(True)


class BrowseListWidget(QtWidgets.QWidget):
    """
    List widget with insert/remove buttons to add or remove browsed file paths.  Will emit a signal on adding/removing
    items so you can connect it to other widgets.
    """
    files_updated = QtCore.Signal(bool)

    def __init__(self, parent):
        super().__init__(parent)

        self.layout = QtWidgets.QHBoxLayout()

        self.list_widget = DeletableListWidget(self)
        list_size_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Preferred)
        list_size_policy.setHorizontalStretch(2)
        self.list_widget.setSizePolicy(list_size_policy)
        self.layout.addWidget(self.list_widget)

        self.button_layout = QtWidgets.QVBoxLayout()
        self.button_layout.addStretch(1)
        self.insert_button = QtWidgets.QPushButton("Insert", self)
        self.button_layout.addWidget(self.insert_button)
        self.remove_button = QtWidgets.QPushButton("Remove", self)
        self.button_layout.addWidget(self.remove_button)
        self.button_layout.addStretch(1)
        self.layout.addLayout(self.button_layout)

        self.setLayout(self.layout)

        self.opts = {}
        self.setup()
        self.insert_button.clicked.connect(self.file_browse)
        self.remove_button.clicked.connect(self.remove_item)
        self.list_widget.files_updated.connect(self.files_changed)

    def setup(self, mode='file', registry_key='pydro', app_name='browselistwidget', supported_file_extension='*.*',
              multiselect=True, filebrowse_title='Select files', filebrowse_filter='all files (*.*)'):
        """
        keyword arguments for the widget.
        """
        self.opts = vars()

    def file_browse(self):
        """
        select a file and add it to the list.
        """
        fils = []
        if self.opts['mode'] == 'file':
            msg, fils = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey=self.opts['registry_key'],
                                                              Title=self.opts['filebrowse_title'],
                                                              AppName=self.opts['app_name'],
                                                              bMulti=self.opts['multiselect'], bSave=False,
                                                              fFilter=self.opts['filebrowse_filter'])
        elif self.opts['mode'] == 'directory':
            msg, fils = RegistryHelpers.GetDirFromUserQT(self, RegistryKey=self.opts['registry_key'],
                                                         Title=self.opts['filebrowse_title'],
                                                         AppName=self.opts['app_name'])
            fils = [fils]
        if fils:
            self.add_new_files(fils)
        self.files_changed()

    def add_new_files(self, files):
        """
        Add some new files to the widget, assuming they pass the supported extension option

        Parameters
        ----------
        files: list, list of string paths to files

        """
        files = sorted(files)
        supported_ext = self.opts['supported_file_extension']
        for f in files:
            if self.list_widget.findItems(f, QtCore.Qt.MatchExactly):  # no duplicates allowed
                continue

            if self.opts['mode'] == 'file':
                fil_extension = os.path.splitext(f)[1]
                if supported_ext == '*.*':
                    self.list_widget.addItem(f)
                elif type(supported_ext) is str and fil_extension == supported_ext:
                    self.list_widget.addItem(f)
                elif type(supported_ext) is list and fil_extension in supported_ext:
                    self.list_widget.addItem(f)
                else:
                    print('{} is not a supported file extension.  Must be a string or list of file extensions.'.format(
                        supported_ext))
                    return
            else:
                self.list_widget.addItem(f)

    def return_all_items(self):
        """
        Return all the items in the list widget

        Returns
        -------
        list
            list of strings for all items in the widget
        """
        items = [self.list_widget.item(i).text() for i in range(self.list_widget.count())]
        return items

    def remove_item(self):
        """
        remove a file from the list
        """
        for itm in self.list_widget.selectedItems():
            self.list_widget.takeItem(self.list_widget.row(itm))
        self.files_changed()

    def files_changed(self):
        self.files_updated.emit(True)


class CollapsibleWidget(QtWidgets.QWidget):
    """
    Transcribed to pyside from https://github.com/MichaelVoelkel/qt-collapsible-section/blob/master/Section.cpp
    """
    def __init__(self, parent: None, title: str, animation_duration: int, set_expanded_height: int = 0):
        super().__init__(parent=parent)

        self.parent = parent
        self.animation_duration = animation_duration
        self.title = title
        self.set_expanded_height = set_expanded_height

        self.toggle_button = QtWidgets.QToolButton()
        self.header_line = QtWidgets.QFrame()
        self.toggle_animation = QtCore.QParallelAnimationGroup()
        self.content_area = QtWidgets.QScrollArea()
        self.main_layout = QtWidgets.QGridLayout()

        self.toggle_button.setStyleSheet("QToolButton { border: none; }")
        self.toggle_button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.toggle_button.setArrowType(QtCore.Qt.RightArrow)
        self.toggle_button.setText(str(title))
        self.toggle_button.setCheckable(True)
        self.toggle_button.setChecked(False)

        self.header_line.setFrameShape(QtWidgets.QFrame.HLine)
        self.header_line.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.header_line.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Maximum)

        self.content_area.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.content_area.setMaximumHeight(0)
        self.content_area.setMinimumHeight(0)

        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self, b'minimumHeight'))
        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self, b'maximumHeight'))
        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self.content_area, b'maximumHeight'))

        self.main_layout.setVerticalSpacing(0)
        self.main_layout.setContentsMargins(0, 0, 0, 0)

        self.main_layout.addWidget(self.toggle_button, 0, 0, 1, 1, QtCore.Qt.AlignLeft)
        self.main_layout.addWidget(self.header_line, 0, 2, 1, 1)
        self.main_layout.addWidget(self.content_area, 1, 0, 1, 3)
        self.setLayout(self.main_layout)

        self.toggle_button.toggled.connect(self.toggle)

    def toggle(self, is_collapsed):
        if is_collapsed:
            self.toggle_button.setArrowType(QtCore.Qt.DownArrow)
            self.toggle_animation.setDirection(QtCore.QAbstractAnimation.Forward)
        else:
            self.toggle_button.setArrowType(QtCore.Qt.RightArrow)
            self.toggle_animation.setDirection(QtCore.QAbstractAnimation.Backward)
        self.toggle_animation.start()

    def setContentLayout(self, contentLayout):
        self.content_area.destroy()
        self.content_area.setLayout(contentLayout)
        collapsed_height = self.sizeHint().height() - self.content_area.maximumHeight()
        if not self.set_expanded_height:
            content_height = contentLayout.sizeHint().height()
        else:
            content_height = self.set_expanded_height
        for i in range(self.toggle_animation.animationCount() - 1):
            collapse_animation = self.toggle_animation.animationAt(i)
            collapse_animation.setDuration(self.animation_duration)
            collapse_animation.setStartValue(collapsed_height)
            collapse_animation.setEndValue(collapsed_height + content_height)

        content_animation = self.toggle_animation.animationAt(self.toggle_animation.animationCount() - 1)
        content_animation.setDuration(self.animation_duration)
        content_animation.setStartValue(0)
        content_animation.setEndValue(content_height)


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the widget for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Test Window')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        # self.widg = BrowseListWidget(self)
        # self.widg.files_updated.connect(self.print_out_files)
        # layout.addWidget(self.widg)

        self.collapse = CollapsibleWidget(self, 'collapse', 100)
        self.datalayout = QtWidgets.QVBoxLayout()
        self.datalayout.addWidget(QtWidgets.QLabel('Some text in Section', self.collapse))
        self.datalayout.addWidget(QtWidgets.QPushButton('Some text in Section', self.collapse))
        self.collapse.setContentLayout(self.datalayout)
        layout.addWidget(self.collapse)

        self.datalayout.layout()
        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()

    def print_out_files(self):
        print([self.widg.list_widget.item(i).text() for i in range(self.widg.list_widget.count())])


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())