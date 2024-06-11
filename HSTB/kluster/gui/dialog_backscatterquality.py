import os
import logging
import datetime as dt
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.drivers import par3, kmall, PCSio, sbet, prr3, raw
from HSTB.kluster.fqpr_drivers import read_first_fifty_records, kluster_read_test, bscorr_generation

from HSTB.kluster.modules import backscatterquality

class BackscatterQualityDialog(SaveStateDialog):
    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='BackscatterQualityDialog')

        self.setWindowTitle('Backscatter Quality Analyzer')
        self.mainlayout = QtWidgets.QVBoxLayout()

        self.instructions_msg = QtWidgets.QLabel('See Output tab for the results of the function.\n'
                                                 'This tool runs backscatter quality checks on .kmall or .all multibeam data. \n'
                                                 'The output is line-by-line image summaries and a csv statistical summary of the results.\n'
                                                 'This process searches all subdirectories in the selected folder. If same folder is selected multiple times,\n'
                                                 'only new files will be analyzed.')

        self.proc_msg = QtWidgets.QLabel(f'Select a directory for analysis. Any files in subdirectories will also be included:')
        self.results_msg = QtWidgets.QLabel(f'Select a directory for results .csv report and images:')

        self.ftypelabel = QtWidgets.QLabel('')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.fil_text1 = QtWidgets.QLineEdit('', self)
        self.fil_text1.setMinimumWidth(300)
        self.fil_text1.setReadOnly(True)
        self.hlayout_one.addWidget(self.fil_text1)
        self.browse_button_one = QtWidgets.QPushButton("Browse")
        self.hlayout_one.addWidget(self.browse_button_one)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.fil_text2 = QtWidgets.QLineEdit('', self)
        self.fil_text2.setMinimumWidth(300)
        self.fil_text2.setReadOnly(True)
        self.hlayout_two.addWidget(self.fil_text2)
        self.browse_button_two = QtWidgets.QPushButton("Browse")
        self.hlayout_two.addWidget(self.browse_button_two)

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.functioncombobox = QtWidgets.QComboBox()
        self.functioncombobox.setMinimumWidth(250)
        self.checkfiles = QtWidgets.QPushButton('Check Files')
        self.functionrun = QtWidgets.QPushButton('Run')
        self.hlayout_three.addWidget(self.checkfiles)
        self.hlayout_three.addWidget(self.functionrun)
        self.hlayout_three.addWidget(QtWidgets.QLabel(''))

        self.pbar = QtWidgets.QProgressBar(self)
        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.pbar.setGeometry(30,40,290,25)
        self.hlayout_four.addWidget(self.pbar)
        self.hlayout_four.addWidget(QtWidgets.QLabel(''))

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.close_button = QtWidgets.QPushButton('Close', self)
        self.button_layout.addWidget(self.close_button)
        self.button_layout.addStretch(1)

        self.mainlayout.addWidget(self.instructions_msg)
        self.mainlayout.addWidget(QtWidgets.QLabel(''))
        self.mainlayout.addWidget(self.proc_msg)
        self.mainlayout.addLayout(self.hlayout_one)
        self.mainlayout.addWidget(QtWidgets.QLabel(''))
        self.mainlayout.addWidget(self.results_msg)
        self.mainlayout.addLayout(self.hlayout_two)
        self.mainlayout.addLayout(self.hlayout_three)
        self.mainlayout.addLayout(self.hlayout_four)
        self.mainlayout.addStretch()
        self.mainlayout.addLayout(self.button_layout)

        self.setLayout(self.mainlayout)

        self.filename = ''
        self.filenametwo = ''
        self.filetype = ''
        self.fileobject = None
        self.fileobjecttwo = None

        self.browse_button_one.clicked.connect(self.get_proc_directory)
        self.browse_button_two.clicked.connect(self.get_results_directory)
        self.checkfiles.clicked.connect(self.print_files)
        self.functionrun.clicked.connect(self.run_function)
        self.close_button.clicked.connect(self.close_button_clicked)

        # self.mode_switch(None)

    def get_proc_directory(self):
        proc_directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory for Analysis")
        self.fil_text1.setText(proc_directory)

    def get_results_directory(self):
        results_directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory for Results")
        self.fil_text2.setText(results_directory)

    def print_files(self):
        processing_dir = self.fil_text1.text()
        results_dir = self.fil_text2.text()
        fles, results_csv, df_existing = backscatterquality.find_files(processing_dir, results_dir)
        print('Total Number of new Lines to Process: ' + str(len(fles)))
        for fle in fles:
            print(fle)
        print('Press Run to Proceed')

    def run_function(self, e):
        processing_dir = self.fil_text1.text()
        results_dir = self.fil_text2.text()
        # self.close()
        # backscatterquality.create_raw_bs_evaluation(processing_dir, results_dir)
        # print('Started processing: ', dt.datetime.now())
        fles, results_csv, df_existing = backscatterquality.find_files(processing_dir, results_dir)
        combined_results = {}
        for q in range(len(fles)):
            fle = fles[q]
            self.print('Opening ' + fle + ' for evaluation. File ' + str(q + 1) + ' of ' + str(len(fles)), logging.INFO)

            results = backscatterquality.evaluate_raw_backscatter_for_file(fle)
            combined_results = backscatterquality.make_plot_for_line(fle, results, combined_results, results_dir)
            self.pbar.setValue(int((q+1)*100/len(fles)))
            QtWidgets.QApplication.processEvents()
        backscatterquality.assemble_results_csv(combined_results, results_csv, df_existing)
        print('Completed processing: ', dt.datetime.now())


    def close_button_clicked(self, e):
        self.close()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = BackscatterQualityDialog()
    dlog.show()
    app.exec_()