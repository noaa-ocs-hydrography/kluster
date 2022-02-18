import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.shared import RegistryHelpers
from HSTB.drivers import par3, kmall, PCSio, sbet
from HSTB.kluster.fqpr_drivers import read_first_fifty_records, kluster_read_test


class FileAnalyzerDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('File Analyzer')
        self.mainlayout = QtWidgets.QVBoxLayout()

        self.instructions_msg = QtWidgets.QLabel('See Output tab for the results of the function.\nNOTE: posmv files are mapped on initialization, this can be a long process.')

        self.start_msg = QtWidgets.QLabel('Select a raw file to analyze (kmall, all, posmv, sbet, smrmsg):')

        self.ftypelabel = QtWidgets.QLabel('')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('', self)
        self.fil_text.setMinimumWidth(300)
        self.fil_text.setReadOnly(True)
        self.hlayout_one.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse")
        self.hlayout_one.addWidget(self.browse_button)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.functioncombobox = QtWidgets.QComboBox()
        self.functioncombobox.setMinimumWidth(250)
        self.hlayout_two.addWidget(self.functioncombobox)
        self.functionrun = QtWidgets.QPushButton('Run')
        self.hlayout_two.addWidget(self.functionrun)
        self.hlayout_two.addWidget(QtWidgets.QLabel(''))

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.close_button = QtWidgets.QPushButton('Close', self)
        self.button_layout.addWidget(self.close_button)
        self.button_layout.addStretch(1)

        self.mainlayout.addWidget(self.instructions_msg)
        self.mainlayout.addWidget(QtWidgets.QLabel(''))
        self.mainlayout.addWidget(self.start_msg)
        self.mainlayout.addWidget(self.ftypelabel)
        self.mainlayout.addLayout(self.hlayout_one)
        self.mainlayout.addLayout(self.hlayout_two)
        self.mainlayout.addStretch()
        self.mainlayout.addLayout(self.button_layout)

        self.setLayout(self.mainlayout)

        self.filename = ''
        self.filetype = ''
        self.fileobject = None

        self.browse_button.clicked.connect(self.file_browse)
        self.functionrun.clicked.connect(self.run_function)
        self.close_button.clicked.connect(self.close_button_clicked)

    def file_browse(self, e):
        msg, file_path = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                               Title='Select a raw file to read',
                                                               AppName='\\analyzer', bMulti=False, bSave=False,
                                                               fFilter='all files (*.*)')
        if file_path:
            self.fil_text.setText(file_path)
            self.filename = file_path
            fext = os.path.splitext(self.filename)[1]
            self.functioncombobox.clear()
            if fext == '.all':
                self.filetype = 'kongsberg_all'
                self.fileobject = par3.AllRead(self.filename)
                self.functioncombobox.addItems(['read_first_fifty_records', 'kluster_read_test'])
            elif fext == '.kmall':
                self.filetype = 'kongsberg_kmall'
                self.fileobject = kmall.kmall(self.filename)
                self.functioncombobox.addItems(['read_first_fifty_records'])
            elif fext in ['.out', '.sbet', 'smrmsg']:
                if sbet.is_sbet(file_path):
                    self.filetype = 'applanix_sbet'
                    self.fileobject = sbet.read(file_path, numcolumns=17)
                    self.functioncombobox.addItems(['read_first_fifty_records'])
                elif sbet.is_smrmsg(file_path):
                    self.filetype = 'applanix_smrmsg'
                    self.fileobject = sbet.read(file_path, numcolumns=10)
                    self.functioncombobox.addItems(['read_first_fifty_records'])
                else:
                    print(f'Not a recognized file type, tried sbet and smrmsg: {self.filename}')
                    self.filetype = ''
                    self.fileobject = None
            else:
                try:
                    poscheck = int(fext[1:])
                    self.filetype = 'posmv'
                    self.fileobject = PCSio.PCSFile(self.filename)
                    self.functioncombobox.addItems(['read_first_fifty_records'])
                except:
                    print(f'Not a recognized file type: {self.filename}')
                    self.filetype = ''
                    self.fileobject = None
        self.ftypelabel.setText(f'File Type: {self.filetype}')

    def run_function(self, e):
        funcname = self.functioncombobox.currentText()
        if funcname:
            if funcname == 'read_first_fifty_records':
                read_first_fifty_records(self.fileobject)
            elif funcname == 'kluster_read_test':
                kluster_read_test(self.fileobject)

    def close_button_clicked(self, e):
        self.close()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = FileAnalyzerDialog()
    dlog.show()
    app.exec_()
