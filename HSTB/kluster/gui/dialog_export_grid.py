import os
from datetime import datetime

from bathygrid.grid_variables import allowable_grid_root_names
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster.gdal_helpers import return_gdal_version
from HSTB.shared import RegistryHelpers
from HSTB.kluster import __version__ as kluster_version
from HSTB.kluster import kluster_variables


class ExportGridDialog(SaveStateDialog):
    """
    Dialog allows for providing kluster surface data for exporting and the desired export type, in self.export_opts.

    Uses GDAL, all GDAL formats are achievable with the dialog, currently only supports, GTiff and BAG
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='export_grid')

        self.setWindowTitle('Export Surface')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Export from the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('', self)
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout_zero.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_zero.addWidget(self.browse_button)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Export to: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.export_opts = QtWidgets.QComboBox()
        self.export_opts.addItems(['Geotiff', 'BAG', 'csv'])
        self.hlayout_one.addWidget(self.export_opts)
        self.zdirect_check = QtWidgets.QCheckBox('Z as Elevation (+ UP)')
        self.zdirect_check.setChecked(True)
        self.hlayout_one.addWidget(self.zdirect_check)
        self.hlayout_one.addStretch()

        self.bag_options_widget = QtWidgets.QWidget()
        self.hlayout_bag_toplevel = QtWidgets.QHBoxLayout()
        self.vlayout_bag_leftside = QtWidgets.QVBoxLayout()
        self.vlayout_bag_rightside = QtWidgets.QVBoxLayout()

        self.bag_individual_label = QtWidgets.QLabel('Individual Name: ')
        self.vlayout_bag_leftside.addWidget(self.bag_individual_label)
        self.bag_individual = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_individual)

        self.bag_organizational_label = QtWidgets.QLabel('Organizational Name: ')
        self.vlayout_bag_leftside.addWidget(self.bag_organizational_label)
        self.bag_organizational = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_organizational)

        self.bag_position_label = QtWidgets.QLabel('Position Name: ')
        self.vlayout_bag_leftside.addWidget(self.bag_position_label)
        self.bag_position = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_position)

        self.bag_date_label = QtWidgets.QLabel('Date: ')
        self.vlayout_bag_leftside.addWidget(self.bag_date_label)
        self.bag_date = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_date)

        self.bag_vert_crs_label = QtWidgets.QLabel('Vertical Coordinate WKT: ')
        self.vlayout_bag_leftside.addWidget(self.bag_vert_crs_label)
        self.bag_vert_crs = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_vert_crs)

        self.bag_abstract_label = QtWidgets.QLabel('Abstract: ')
        self.vlayout_bag_leftside.addWidget(self.bag_abstract_label)
        self.bag_abstract = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_abstract)

        self.bag_process_step_label = QtWidgets.QLabel('Process Step Description: ')
        self.vlayout_bag_leftside.addWidget(self.bag_process_step_label)
        self.bag_process_step = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_process_step)

        self.bag_datetime_label = QtWidgets.QLabel('Datetime: ')
        self.vlayout_bag_leftside.addWidget(self.bag_datetime_label)
        self.bag_datetime = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_datetime)

        self.bag_restriction_label = QtWidgets.QLabel('Restriction Code: ')
        self.vlayout_bag_leftside.addWidget(self.bag_restriction_label)
        self.bag_restriction = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_restriction)

        self.bag_constraints_label = QtWidgets.QLabel('Other Constraints: ')
        self.vlayout_bag_leftside.addWidget(self.bag_constraints_label)
        self.bag_constraints = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_constraints)

        self.bag_classification_label = QtWidgets.QLabel('Classification: ')
        self.vlayout_bag_leftside.addWidget(self.bag_classification_label)
        self.bag_classification = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_classification)

        self.bag_security_label = QtWidgets.QLabel('Security User Note: ')
        self.vlayout_bag_leftside.addWidget(self.bag_security_label)
        self.bag_security = QtWidgets.QLineEdit('')
        self.vlayout_bag_rightside.addWidget(self.bag_security)

        self.hlayout_bag_toplevel.addLayout(self.vlayout_bag_leftside)
        self.hlayout_bag_toplevel.addLayout(self.vlayout_bag_rightside)
        self.bag_options_widget.setLayout(self.hlayout_bag_toplevel)

        self.output_msg = QtWidgets.QLabel('Export to the following:')

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.output_text = QtWidgets.QLineEdit('', self)
        self.output_text.setMinimumWidth(400)
        self.output_text.setReadOnly(True)
        self.hlayout_one_one.addWidget(self.output_text)
        self.output_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one_one.addWidget(self.output_button)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_two.addWidget(self.ok_button)
        self.hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_two.addWidget(self.cancel_button)
        self.hlayout_two.addStretch(1)

        layout.addWidget(self.input_msg)
        layout.addLayout(self.hlayout_zero)
        layout.addLayout(self.hlayout_one)
        layout.addWidget(self.bag_options_widget)
        layout.addStretch(1)
        layout.addWidget(self.output_msg)
        layout.addLayout(self.hlayout_one_one)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.input_pth = ''
        self.output_pth = ''
        self.canceled = False

        self.browse_button.clicked.connect(self.grid_folder_browse)
        self.output_button.clicked.connect(self.output_file_browse)
        self.export_opts.currentTextChanged.connect(self._event_update_status)
        self.ok_button.clicked.connect(self.start_export)
        self.cancel_button.clicked.connect(self.cancel_export)

        self.text_controls = [['export_ops', self.export_opts]]
        self.checkbox_controls = [['zdirect_check', self.zdirect_check]]

        self.read_settings()
        self._event_update_status(self.export_opts.currentText())
        self._set_default_bag_options()

    def _set_default_bag_options(self):
        self.bag_individual.setText('unknown')
        self.bag_organizational.setText('unknown')
        self.bag_position.setText('unknown')
        self.bag_date.setText(datetime.now().strftime('%Y-%m-%d'))
        self.bag_vert_crs.setText('VERT_CS["unknown", VERT_DATUM["unknown", 2000]]')
        self.bag_abstract.setText('none')
        self.bag_process_step.setText('Generated By GDAL {} and Kluster {}'.format(return_gdal_version(), kluster_version))
        self.bag_datetime.setText(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        self.bag_restriction.setText('otherRestrictions')
        self.bag_constraints.setText('unknown')
        self.bag_classification.setText('unclassified')
        self.bag_security.setText('none')

    def grid_folder_browse(self):
        # dirpath will be None or a string
        msg, self.input_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                               Title='Select Grid directory', AppName='\\reghelp')
        if self.input_pth is not None:
            self.update_input_path(self.input_pth)

    def update_input_path(self, foldername: str):
        if not any([os.path.exists(os.path.join(foldername, allw)) for allw in allowable_grid_root_names]):
            foldername = ''

        self.input_pth = foldername
        # rerun update status to clear the status if this grid_folder_browse attempt raises no warning
        curr_opts = self.export_opts.currentText().lower()
        self._event_update_status(curr_opts)
        self.fil_text.setText(self.input_pth)
        if self.input_pth:
            if not self.output_pth:
                if curr_opts != 'geotiff':
                    ext = curr_opts
                else:
                    ext = 'tif'
                self.output_pth = os.path.join(self.input_pth, 'export.{}'.format(ext))
                self.output_text.setText(self.output_pth)
        else:
            self.status_msg.setText('"Export from the following" folder must contain a root folder (i.e. VRGridTile_Root)')
            self.ok_button.setEnabled(False)

    def update_vert_ref(self, vertical_reference: str):
        if vertical_reference in ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW']:
            pass  # leave as unknown
        else:
            self.bag_vert_crs.setText(vertical_reference)

    def output_file_browse(self):
        curr_opts = self.export_opts.currentText().lower()
        if curr_opts == 'csv':
            titl = 'Select output csv file'
            ffilter = "csv file|*.csv"
        elif curr_opts == 'bag':
            titl = 'Select output bag file'
            ffilter = "bag file|*.bag"
        elif curr_opts == 'geotiff':
            titl = 'Select output geotiff file'
            ffilter = "geotiff file|*.tif"
        else:
            raise ValueError('dialog_export_grid: unrecognized method: {}'.format(curr_opts))

        # dirpath will be None or a string
        msg, outpth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster', DefaultFile=self.output_pth,
                                                            Title=titl, fFilter=ffilter, AppName='\\reghelp')
        if outpth:
            self.output_pth = outpth
            self.output_text.setText(self.output_pth)

    def _event_update_status(self, combobox_text: str):
        """
        Update the status message if an Error presents itself, triggered on changing the export type

        Parameters
        ----------
        combobox_text
            value of the combobox as text
        """

        curr_opts = combobox_text.lower()
        self._show_hide_options(curr_opts)
        if curr_opts == 'bag':
            vers = return_gdal_version()
            majr, minr, hfix = vers.split('.')
            self.zdirect_check.hide()
            if (int(majr) == 3 and int(minr) >= 2) or (int(majr) > 3):  # If this is the pydro environment, we know it has Entwine
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
                self.status_msg.setText('Gdal > 3.2 found, BAG export allowed')
                self.ok_button.setEnabled(True)
            else:
                self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
                self.status_msg.setText('Gdal > 3.2 not found, BAG export not allowed')
                self.ok_button.setEnabled(False)
        else:
            self.status_msg.setText('')
            self.ok_button.setEnabled(True)
            self.zdirect_check.show()

        if curr_opts != 'geotiff':
            ext = curr_opts
        else:
            ext = 'tif'
        if self.output_pth:
            self.output_pth = os.path.splitext(self.output_pth)[0] + '.' + ext
            self.output_text.setText(self.output_pth)

    def _show_hide_options(self, combobox_text):
        curr_opts = combobox_text.lower()
        if curr_opts == 'bag':
            self.bag_options_widget.setVisible(True)
        else:
            self.bag_options_widget.setVisible(False)

    def return_processing_options(self):
        """
        Return processing options to run the grid export worker.

        Returns
        -------
        dict
            processing options from the dialog controls
        """

        return {'output_path': self.output_pth, 'export_format': self.export_opts.currentText(),
                'z_positive_up': self.zdirect_check.isChecked(),
                'individual_name': self.bag_individual.text(), 'organizational_name': self.bag_organizational.text(),
                'position_name': self.bag_position.text(), 'attr_date': self.bag_date.text(),
                'vert_crs': self.bag_vert_crs.text(), 'abstract': self.bag_abstract.text(),
                'process_step_description': self.bag_process_step.text(), 'attr_datetime': self.bag_datetime.text(),
                'restriction_code': self.bag_restriction.text(), 'other_constraints': self.bag_constraints.text(),
                'classification': self.bag_classification.text(), 'security_user_note': self.bag_security.text()}

    def start_export(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if not self.input_pth:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No data provided')
        elif not self.output_pth:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No output path provided')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()

    def cancel_export(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ExportGridDialog()
    dlog.show()
    if dlog.exec_():
        pass
