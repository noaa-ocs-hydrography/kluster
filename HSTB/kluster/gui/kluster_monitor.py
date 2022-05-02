import logging
import sys, os
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.shared import RegistryHelpers
from HSTB.kluster import monitor


class MonitorPath(QtWidgets.QWidget):
    """
    Base widget for interacting with the fqpr_intelligence.DirectoryMonitor object.  Each instance of this class
    has a file browse button, status light, etc.
    """
    monitor_file_event = Signal(str, str)
    monitor_start = Signal(str)

    def __init__(self, parent: QtWidgets.QWidget = None):
        """
        initialize

        Parameters
        ----------
        parent
            MonitorDashboard
        """
        super().__init__(parent=parent)

        self.vlayout = QtWidgets.QVBoxLayout()

        self.hlayoutone = QtWidgets.QHBoxLayout()
        self.statuslight = QtWidgets.QCheckBox('')
        self.statuslight.setStyleSheet("QCheckBox::indicator {background-color : black;}")
        self.statuslight.setDisabled(True)
        self.hlayoutone.addWidget(self.statuslight)
        self.fil_text = QtWidgets.QLineEdit('')
        self.fil_text.setReadOnly(True)
        self.hlayoutone.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse")
        self.hlayoutone.addWidget(self.browse_button)

        self.hlayouttwo = QtWidgets.QHBoxLayout()
        self.start_button = QtWidgets.QPushButton('Start')
        self.hlayouttwo.addWidget(self.start_button)
        self.stop_button = QtWidgets.QPushButton('Stop')
        self.hlayouttwo.addWidget(self.stop_button)
        spcr = QtWidgets.QLabel('    ')
        self.hlayouttwo.addWidget(spcr)
        self.include_subdirectories = QtWidgets.QCheckBox('Include Subdirectories')
        self.hlayouttwo.addWidget(self.include_subdirectories)

        self.vlayout.addLayout(self.hlayoutone)
        self.vlayout.addLayout(self.hlayouttwo)
        self.setLayout(self.vlayout)

        self.browse_button.clicked.connect(self.dir_browse)
        self.start_button.clicked.connect(self.start_monitoring)
        self.stop_button.clicked.connect(self.stop_monitoring)

        self.monitor = None

    def print(self, msg: str, loglevel: int):
        if self.parent() is not None:
            self.parent().print(msg, loglevel)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int):
        if self.parent() is not None:
            self.parent().debug_print(msg, loglevel)
        else:
            print(msg)

    def dir_browse(self):
        """
        As long as you aren't currently running the monitoring, this will get the directory you want to monitor
        """

        if not self.is_running():
            # dirpath will be None or a string
            msg, pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='klusterintel',
                                                        Title='Select directory to monitor', AppName='klusterintel')
            if pth is not None:
                self.fil_text.setText(pth)
        else:
            self.print('You have to stop monitoring before you can change the path', logging.WARNING)

    def return_monitoring_path(self):
        """
        Return the path we are monitoring

        Returns
        -------
        str
            directory path we are monitoring
        """

        return self.fil_text.displayText()

    def is_recursive(self):
        """
        Return whether or not the include_subdirectories checkbox is checked

        Returns
        -------
        bool
            True if checked
        """

        return self.include_subdirectories.isChecked()

    def is_running(self):
        """
        Return whether or not the monitor is running

        Returns
        -------
        bool
            True if the monitor is running
        """

        if self.monitor is not None:
            if self.monitor.watchdog_observer.is_alive():
                return True
        return False

    def start_monitoring(self):
        """
        Start a new DirectoryMonitor.  A stopped DirectoryMonitor will have to be re-instantiated, you can't restart
        a watchdog observer.

        Also sets the status light to green
        """

        pth = self.return_monitoring_path()
        is_recursive = self.is_recursive()
        if os.path.exists(pth):
            # you can't restart a watchdog observer, have to create a new one
            self.stop_monitoring()
            self.monitor = monitor.DirectoryMonitor(pth, is_recursive)
            self.monitor.bind_to(self.emit_monitor_event)
            self.monitor.start()
            self.monitor_start.emit(pth)
            self.include_subdirectories.setEnabled(False)
            self.statuslight.setStyleSheet("QCheckBox::indicator {background-color : green;}")
            self.print('Monitoring {}'.format(pth), logging.INFO)
        else:
            self.print('MonitorPath: Path does not exist: {}'.format(pth), logging.ERROR)

    def stop_monitoring(self):
        """
        If the DirectoryMonitor object is running, stop it
        """

        if self.is_running():
            self.monitor.stop()
            self.include_subdirectories.setEnabled(True)
            self.statuslight.setStyleSheet("QCheckBox::indicator {background-color : black;}")
            self.print('No longer monitoring {}'.format(self.return_monitoring_path()), logging.INFO)

    def emit_monitor_event(self, newfile: str, file_event: str):
        """
        Triggered when self.monitor sees a newfile, passes in the file path and the event

        Parameters
        ----------
        newfile
            file path
        file_event
            one of 'created', 'deleted'
        """

        self.monitor_file_event.emit(newfile, file_event)


class KlusterMonitorWidget(QtWidgets.QWidget):
    """
    Widget for holding the folder path entered, the start stop buttons, etc. for the monitor tool.  Hook up to the
    two events to get the data from the controls.
    """

    monitor_file_event = Signal(str, str)
    monitor_start = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.monitor_layout = QtWidgets.QVBoxLayout()

        self.monitorone_layout = QtWidgets.QHBoxLayout()
        self.monitorone = MonitorPath(self)
        self.monitorone_layout.addWidget(self.monitorone)
        self.monitor_layout.addLayout(self.monitorone_layout)

        self.monitortwo_layout = QtWidgets.QHBoxLayout()
        self.monitortwo = MonitorPath(self)
        self.monitortwo_layout.addWidget(self.monitortwo)
        self.monitor_layout.addLayout(self.monitortwo_layout)

        self.monitorthree_layout = QtWidgets.QHBoxLayout()
        self.monitorthree = MonitorPath(self)
        self.monitorthree_layout.addWidget(self.monitorthree)
        self.monitor_layout.addLayout(self.monitorthree_layout)

        self.monitorfour_layout = QtWidgets.QHBoxLayout()
        self.monitorfour = MonitorPath(self)
        self.monitorfour_layout.addWidget(self.monitorfour)
        self.monitor_layout.addLayout(self.monitorfour_layout)

        self.monitorfive_layout = QtWidgets.QHBoxLayout()
        self.monitorfive = MonitorPath(self)
        self.monitorfive_layout.addWidget(self.monitorfive)
        self.monitor_layout.addLayout(self.monitorfive_layout)
        self.monitor_layout.addStretch()

        self.monitorone.monitor_file_event.connect(self.emit_file_event)
        self.monitortwo.monitor_file_event.connect(self.emit_file_event)
        self.monitorthree.monitor_file_event.connect(self.emit_file_event)
        self.monitorfour.monitor_file_event.connect(self.emit_file_event)
        self.monitorfive.monitor_file_event.connect(self.emit_file_event)

        self.monitorone.monitor_start.connect(self.emit_monitor_start)
        self.monitortwo.monitor_start.connect(self.emit_monitor_start)
        self.monitorthree.monitor_start.connect(self.emit_monitor_start)
        self.monitorfour.monitor_start.connect(self.emit_monitor_start)
        self.monitorfive.monitor_start.connect(self.emit_monitor_start)

        self.setLayout(self.monitor_layout)
        self.layout()

    def print(self, msg: str, loglevel: int):
        if self.parent is not None:
            self.parent.print(msg, loglevel)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int):
        if self.parent is not None:
            self.parent.debug_print(msg, loglevel)
        else:
            print(msg)

    def emit_file_event(self, newfile: str, file_event: str):
        """
        Triggered on a new file showing up in the MonitorPath

        Parameters
        ----------
        newfile
            file path
        file_event
            one of 'created', 'deleted'
        """

        self.monitor_file_event.emit(newfile, file_event)

    def emit_monitor_start(self, pth: str):
        """
        Triggered on the start button being pressed, emits the folder path

        Parameters
        ----------
        pth
            folder path as string
        """

        self.monitor_start.emit(pth)

    def stop_all_monitoring(self):
        """
        Stop all the monitors if they are running, this is triggered on closing the main gui
        """

        self.monitorone.stop_monitoring()
        self.monitortwo.stop_monitoring()
        self.monitorthree.stop_monitoring()
        self.monitorfour.stop_monitoring()
        self.monitorfive.stop_monitoring()

    def save_settings(self, settings: QtCore.QSettings):
        """
        Save the settings to the Qsettings
        """
        settings.setValue('Kluster/monitor_one_path', self.monitorone.fil_text.text())
        settings.setValue('Kluster/monitor_two_path', self.monitortwo.fil_text.text())
        settings.setValue('Kluster/monitor_three_path', self.monitorthree.fil_text.text())
        settings.setValue('Kluster/monitor_four_path', self.monitorfour.fil_text.text())
        settings.setValue('Kluster/monitor_five_path', self.monitorfive.fil_text.text())

        settings.setValue('Kluster/monitor_one_subdir', self.monitorone.include_subdirectories.isChecked())
        settings.setValue('Kluster/monitor_two_subdir', self.monitortwo.include_subdirectories.isChecked())
        settings.setValue('Kluster/monitor_three_subdir', self.monitorthree.include_subdirectories.isChecked())
        settings.setValue('Kluster/monitor_four_subdir', self.monitorfour.include_subdirectories.isChecked())
        settings.setValue('Kluster/monitor_five_subdir', self.monitorfive.include_subdirectories.isChecked())

    def read_settings(self, settings: QtCore.QSettings):
        """
        Read from the Qsettings
        """
        try:
            if settings.value('Kluster/monitor_one_path'):
                self.monitorone.fil_text.setText(settings.value('Kluster/monitor_one_path'))
            if settings.value('Kluster/monitor_two_path'):
                self.monitortwo.fil_text.setText(settings.value('Kluster/monitor_two_path'))
            if settings.value('Kluster/monitor_three_path'):
                self.monitorthree.fil_text.setText(settings.value('Kluster/monitor_three_path'))
            if settings.value('Kluster/monitor_four_path'):
                self.monitorfour.fil_text.setText(settings.value('Kluster/monitor_four_path'))
            if settings.value('Kluster/monitor_five_path'):
                self.monitorfive.fil_text.setText(settings.value('Kluster/monitor_five_path'))

            # loads as the word 'false' or 'true'...ugh
            self.monitorone.include_subdirectories.setChecked(settings.value('Kluster/monitor_one_subdir').lower() == 'true')
            self.monitortwo.include_subdirectories.setChecked(settings.value('Kluster/monitor_two_subdir').lower() == 'true')
            self.monitorthree.include_subdirectories.setChecked(settings.value('Kluster/monitor_three_subdir').lower() == 'true')
            self.monitorfour.include_subdirectories.setChecked(settings.value('Kluster/monitor_four_subdir').lower() == 'true')
            self.monitorfive.include_subdirectories.setChecked(settings.value('Kluster/monitor_five_subdir').lower() == 'true')
        except AttributeError:
            # no settings exist yet for this app, .lower failed
            pass


class KlusterMonitor(QtWidgets.QScrollArea):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.widget = KlusterMonitorWidget(parent)

        # Scroll Area Properties
        self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOn)
        self.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOn)
        self.setWidgetResizable(True)
        self.setWidget(self.widget)


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterExplorer for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Kluster Monitor')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.k_monitor = KlusterMonitor(self)
        layout.addWidget(self.k_monitor)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())
