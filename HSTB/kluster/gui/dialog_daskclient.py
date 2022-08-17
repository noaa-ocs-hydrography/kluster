from dask.distributed import get_client
import logging

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster.dask_helpers import dask_find_or_start_client
from HSTB.kluster import kluster_variables


class DaskClientStart(SaveStateDialog):
    """
    Widget that allows you to manually start the dask client if you need to run it in a specific way.  If you don't
    use this, we just autostart a default LocalCluster.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='daskclient')
        if self.parent() is not None:
            self.logger = self.parent().logger
        else:
            self.logger = None

        self.setWindowTitle('Setup Dask Client')

        self.client_vbox = QtWidgets.QVBoxLayout()

        self.local_box = QtWidgets.QGroupBox('Local Cluster')
        self.local_box.setToolTip('Uses your computer resources in starting the Local Cluster, this is what you want when running on a computer normally.')
        self.local_box.setCheckable(True)
        self.local_box.setChecked(True)
        self.local_layout = QtWidgets.QHBoxLayout()

        self.checkbox_layout = QtWidgets.QVBoxLayout()
        self.number_workers_checkbox = QtWidgets.QCheckBox('Override Number of Workers')
        self.number_workers_checkbox.setToolTip('Use this checkbox if you want to set a specific number of workers, default is the number of cores on your machine')
        self.number_workers_checkbox.setChecked(False)
        self.checkbox_layout.addWidget(self.number_workers_checkbox)
        self.number_threads_checkbox = QtWidgets.QCheckBox('Override Threads per Worker')
        self.number_threads_checkbox.setToolTip('Use this checkbox if you want to set a specific number of threads per worker, default is based on the number of cores on your machine')
        self.number_threads_checkbox.setChecked(False)
        self.checkbox_layout.addWidget(self.number_threads_checkbox)
        self.number_memory_checkbox = QtWidgets.QCheckBox('Override Memory (GB) per Worker')
        self.number_memory_checkbox.setToolTip('Use this amount of memory for each worker, default is the max memory available on your system')
        self.number_memory_checkbox.setChecked(False)
        self.checkbox_layout.addWidget(self.number_memory_checkbox)

        self.entry_layout = QtWidgets.QVBoxLayout()
        self.number_workers = QtWidgets.QLineEdit('')
        self.entry_layout.addWidget(self.number_workers)
        self.number_threads = QtWidgets.QLineEdit('')
        self.entry_layout.addWidget(self.number_threads)
        self.number_memory = QtWidgets.QLineEdit('')
        self.entry_layout.addWidget(self.number_memory)

        self.local_layout.addLayout(self.checkbox_layout)
        self.local_layout.addLayout(self.entry_layout)
        self.local_box.setLayout(self.local_layout)
        self.client_vbox.addWidget(self.local_box)

        self.remote_box = QtWidgets.QGroupBox('Remote Client')
        self.remote_box.setToolTip('Use this when you have set up a Dask Cluster on a remote server, the address given here is the address of that server.')
        self.remote_box.setCheckable(True)
        self.remote_box.setChecked(False)
        self.remote_layout = QtWidgets.QHBoxLayout()

        self.checkbox2_layout = QtWidgets.QVBoxLayout()
        self.remote_ip_radio = QtWidgets.QRadioButton('By IP')
        self.remote_ip_radio.setChecked(True)
        self.checkbox2_layout.addWidget(self.remote_ip_radio)
        self.remote_fqdn_radio = QtWidgets.QRadioButton('By FQDN')
        self.checkbox2_layout.addWidget(self.remote_fqdn_radio)
        self.remote_layout.addLayout(self.checkbox2_layout)

        self.addresslayout = QtWidgets.QVBoxLayout()
        self.remote_ip_address_label = QtWidgets.QLabel('Address')
        self.addresslayout.addWidget(self.remote_ip_address_label)
        self.remote_fqdn_address_label = QtWidgets.QLabel('Address')
        self.addresslayout.addWidget(self.remote_fqdn_address_label)
        self.remote_layout.addLayout(self.addresslayout)

        self.addressinputlayout = QtWidgets.QVBoxLayout()
        self.remote_ip_address = QtWidgets.QLineEdit('')
        self.remote_ip_address.setInputMask('000.000.000.000;_')
        self.addressinputlayout.addWidget(self.remote_ip_address)
        self.remote_fqdn_address = QtWidgets.QLineEdit('')
        self.addressinputlayout.addWidget(self.remote_fqdn_address)
        self.remote_layout.addLayout(self.addressinputlayout)

        self.portlayout = QtWidgets.QVBoxLayout()
        self.remote_ip_port_label = QtWidgets.QLabel('Port')
        self.portlayout.addWidget(self.remote_ip_port_label)
        self.remote_fqdn_port_label = QtWidgets.QLabel('Port')
        self.portlayout.addWidget(self.remote_fqdn_port_label)
        self.remote_layout.addLayout(self.portlayout)

        self.portinputlayout = QtWidgets.QVBoxLayout()
        self.remote_ip_port = QtWidgets.QLineEdit('')
        self.remote_ip_port.setInputMask('00000;_')
        self.portinputlayout.addWidget(self.remote_ip_port)
        self.remote_fqdn_port = QtWidgets.QLineEdit('')
        self.remote_fqdn_port.setInputMask('00000;_')
        self.portinputlayout.addWidget(self.remote_fqdn_port)
        self.remote_layout.addLayout(self.portinputlayout)

        self.remote_box.setLayout(self.remote_layout)
        self.client_vbox.addWidget(self.remote_box)

        self.noclient_box = QtWidgets.QGroupBox('No Client')
        self.noclient_box.setToolTip('Use this when you want to skip Dask parallel processing.')
        self.noclient_box.setCheckable(True)
        self.noclient_box.setChecked(False)
        self.noclient_box.setLayout(QtWidgets.QHBoxLayout())
        self.client_vbox.addWidget(self.noclient_box)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
        self.client_vbox.addWidget(self.status_msg)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.button_layout.addWidget(self.ok_button)
        self.button_layout.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addStretch(1)
        self.client_vbox.addLayout(self.button_layout)

        self.cl = None
        self.setLayout(self.client_vbox)

        self.remote_box.clicked.connect(self.remote_box_checked)
        self.local_box.clicked.connect(self.local_box_checked)
        self.noclient_box.clicked.connect(self.noclient_box_checked)
        self.ok_button.clicked.connect(self.setup_client)
        self.cancel_button.clicked.connect(self.cancel_client)

        self.text_controls = [['number_workers', self.number_workers], ['number_memory', self.number_memory],
                              ['number_threads', self.number_threads], ['remote_ip_address', self.remote_ip_address],
                              ['remote_ip_port', self.remote_ip_port], ['remote_fqdn_address', self.remote_fqdn_address],
                              ['remote_fqdn_port', self.remote_fqdn_port]]
        self.checkbox_controls = [['local_box', self.local_box], ['remote_box', self.remote_box], ['noclient_box', self.noclient_box],
                                  ['number_workers_checkbox', self.number_workers_checkbox],
                                  ['number_threads_checkbox', self.number_threads_checkbox],
                                  ['number_memory_checkbox', self.number_memory_checkbox]]
        self.read_settings()

    def remote_box_checked(self):
        if self.remote_box.isChecked():
            self.local_box.setChecked(False)
            self.noclient_box.setChecked(False)

    def local_box_checked(self):
        if self.local_box.isChecked():
            self.remote_box.setChecked(False)
            self.noclient_box.setChecked(False)

    def noclient_box_checked(self):
        if self.noclient_box.isChecked():
            self.remote_box.setChecked(False)
            self.local_box.setChecked(False)

    def setup_client(self):
        """
        Start a new dask client with the options you have here.  Save to the cl attribute so the main window can pull
        it out after user hits OK.
        """

        if self.local_box.isChecked() or self.remote_box.isChecked() or self.noclient_box.isChecked():
            self.accept()
            self.save_settings()
            if self.local_box.isChecked():
                try:  # have to close the existing local cluster/client first if you have one running before you can recreate
                    client = get_client()
                    client.close()
                except:
                    pass
                numworker = None
                threadsworker = None
                memoryworker = None
                multiprocessing = True
                if self.number_workers_checkbox.isChecked():
                    try:
                        numworker = int(self.number_workers.text())
                        if numworker < 1:
                            numworker = 1
                        if numworker == 1:
                            multiprocessing = False
                    except:
                        self.print('Invalid number of workers provided, number must be an integer, ex: 4', logging.ERROR)
                        return
                if self.number_threads_checkbox.isChecked():
                    try:
                        threadsworker = int(self.number_threads.text())
                        if threadsworker < 1:
                            threadsworker = 1
                    except:
                        self.print('Invalid number of threads provided, number must be an integer, ex: 2', logging.ERROR)
                        return
                if self.number_memory_checkbox.isChecked():
                    try:
                        memoryworker = str(self.number_memory.text()) + 'GB'
                    except:
                        self.print('Invalid memory per worker provided, number must be an integer, ex: 5', logging.ERROR)
                        return
                self.cl = dask_find_or_start_client(number_of_workers=numworker, threads_per_worker=threadsworker,
                                                    memory_per_worker=memoryworker, multiprocessing=multiprocessing,
                                                    logger=self.logger)
            elif self.remote_box.isChecked():
                if self.remote_ip_radio.isChecked():
                    full_address = self.remote_ip_address.text() + ':' + self.remote_ip_port.text()
                else:
                    full_address = self.remote_fqdn_address.text() + ':' + self.remote_fqdn_port.text()

                self.print('Starting client at address {}'.format(full_address), logging.INFO)
                try:
                    self.cl = dask_find_or_start_client(address=full_address, logger=self.logger)
                except:  # throws dask socket.gaierror, i'm not bothering to make this explicit
                    self.print('Unable to connect to remote Dask instance', logging.ERROR)
            else:
                self.print('Running without client.', logging.INFO)
                self.cl = None
        else:
            self.status_msg.setText('Please select one of the options above (Local, Remote, or No Client)')

    def cancel_client(self):
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = DaskClientStart()
    dlog.show()
    if dlog.exec_():
        pass
