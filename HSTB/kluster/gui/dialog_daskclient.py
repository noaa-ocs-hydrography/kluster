from PySide2 import QtWidgets
from dask.distributed import get_client

from HSTB.kluster.dask_helpers import dask_find_or_start_client


class DaskClientStart(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Setup Dask Client')

        self.client_vbox = QtWidgets.QVBoxLayout()

        self.local_box = QtWidgets.QGroupBox('Local Cluster - only use your computer, probably what you want')
        self.local_box.setCheckable(True)
        self.local_box.setChecked(True)
        self.client_vbox.addWidget(self.local_box)

        self.remote_box = QtWidgets.QGroupBox('Remote Client - use remotely setup Dask server cluster')
        self.remote_box.setCheckable(True)
        self.remote_box.setChecked(False)
        self.remote_layout = QtWidgets.QVBoxLayout()

        self.remote_ip_layout = QtWidgets.QHBoxLayout()
        self.remote_ip_radio = QtWidgets.QRadioButton('By IP      ')
        self.remote_ip_radio.setChecked(True)
        self.remote_ip_layout.addWidget(self.remote_ip_radio)
        self.remote_ip_address_label = QtWidgets.QLabel('Address')
        self.remote_ip_layout.addWidget(self.remote_ip_address_label)
        self.remote_ip_address = QtWidgets.QLineEdit('')
        self.remote_ip_address.setInputMask('000.000.000.000;_')
        self.remote_ip_address.setMaximumWidth(93)
        self.remote_ip_layout.addWidget(self.remote_ip_address)
        self.remote_ip_layout.addStretch(1)
        self.remote_ip_port_label = QtWidgets.QLabel('Port')
        self.remote_ip_layout.addWidget(self.remote_ip_port_label)
        self.remote_ip_port = QtWidgets.QLineEdit('')
        self.remote_ip_port.setInputMask('00000;_')
        self.remote_ip_port.setMaximumWidth(40)
        self.remote_ip_layout.addWidget(self.remote_ip_port)
        self.remote_layout.addLayout(self.remote_ip_layout)

        self.remote_fqdn_layout = QtWidgets.QHBoxLayout()
        self.remote_fqdn_radio = QtWidgets.QRadioButton('By FQDN')
        self.remote_fqdn_layout.addWidget(self.remote_fqdn_radio)
        self.remote_fqdn_address_label = QtWidgets.QLabel('Address')
        self.remote_fqdn_layout.addWidget(self.remote_fqdn_address_label)
        self.remote_fqdn_address = QtWidgets.QLineEdit('')
        self.remote_fqdn_address.setMaximumWidth(140)
        self.remote_fqdn_layout.addWidget(self.remote_fqdn_address)
        self.remote_fqdn_layout.addStretch(1)
        self.remote_fqdn_port_label = QtWidgets.QLabel('Port')
        self.remote_fqdn_layout.addWidget(self.remote_fqdn_port_label)
        self.remote_fqdn_port = QtWidgets.QLineEdit('')
        self.remote_fqdn_port.setInputMask('00000;_')
        self.remote_fqdn_port.setMaximumWidth(40)
        self.remote_fqdn_layout.addWidget(self.remote_fqdn_port)
        self.remote_layout.addLayout(self.remote_fqdn_layout)

        self.remote_box.setLayout(self.remote_layout)
        self.client_vbox.addWidget(self.remote_box)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : red; }")
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

        self.remote_box.clicked.connect(self.uncheck_local_box)
        self.local_box.clicked.connect(self.uncheck_remote_box)
        self.ok_button.clicked.connect(self.setup_client)
        self.cancel_button.clicked.connect(self.cancel_client)

    def uncheck_local_box(self):
        self.local_box.setChecked(False)

    def uncheck_remote_box(self):
        self.remote_box.setChecked(False)

    def setup_client(self):
        if self.local_box.isChecked() or self.remote_box.isChecked():
            self.accept()
            if self.local_box.isChecked():
                self.cl = dask_find_or_start_client()
            else:
                if self.remote_ip_radio.isChecked():
                    full_address = self.remote_ip_address.text() + ':' + self.remote_ip_port.text()
                else:
                    full_address = self.remote_fqdn_address.text() + ':' + self.remote_fqdn_port.text()

                print('Starting client at address {}'.format(full_address))
                try:
                    self.cl = dask_find_or_start_client(address=full_address)
                except:  # throws dask socket.gaierror, i'm not bothering to make this explicit
                    print('Unable to connect to remote Dask instance')
        else:
            self.status_msg.setText('Please select one of the options above (Local or Remote)')

    def cancel_client(self):
        self.accept()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = DaskClientStart()
    dlog.show()
    if dlog.exec_():
        pass
