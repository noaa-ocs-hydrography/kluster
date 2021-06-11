from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

try:
    from HSTB.kluster import __version__ as klustervers
    if not isinstance(klustervers, str):
        klustervers = klustervers.__version__
except:
    klustervers = 'not found'

try:
    from HSTB.drivers import __version__ as driververs
    if not isinstance(driververs, str):
        driververs = driververs.__version__
except:
    driververs = 'not found'

try:
    from HSTB.shared import __version__ as sharedvers
    if not isinstance(sharedvers, str):
        sharedvers = sharedvers.__version__
except:
    sharedvers = 'not found'

try:
    from bathygrid import __version__ as bathyvers
    if not isinstance(bathyvers, str):
        bathyvers = bathyvers.__version__
except:
    bathyvers = 'not found'

try:
    from vyperdatum import __version__ as vypervers
    if not isinstance(vypervers, str):
        vypervers = vypervers.__version__
except:
    vypervers = 'not found'

try:
    import dask
    daskvers = dask._version.get_versions()['version']
except:
    daskvers = 'not found'

try:
    from xarray import __version__ as xarrvers
except:
    xarrvers = 'not found'

try:
    from osgeo.gdal import __version__ as gdalvers
except:
    gdalvers = 'not found'


class AboutDialog(QtWidgets.QDialog):
    """
    Display versions of modules in this environment
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.toplayout = QtWidgets.QVBoxLayout()
        self.setWindowTitle('About')

        self.title_label = QtWidgets.QLabel('Kluster Version information:')
        self.toplayout.addWidget(self.title_label)
        self.toplayout.addSpacing(30)

        self.datalayout = QtWidgets.QHBoxLayout()
        self.leftlayout = QtWidgets.QVBoxLayout()
        self.rightlayout = QtWidgets.QVBoxLayout()

        self.kluster_label = QtWidgets.QLabel('Kluster Version:')
        self.leftlayout.addWidget(self.kluster_label)
        self.kluster_data = QtWidgets.QLabel('{}'.format(klustervers))
        self.rightlayout.addWidget(self.kluster_data)

        self.driver_label = QtWidgets.QLabel('Drivers Version:')
        self.leftlayout.addWidget(self.driver_label)
        self.driver_data = QtWidgets.QLabel('{}'.format(driververs))
        self.rightlayout.addWidget(self.driver_data)

        self.shared_label = QtWidgets.QLabel('HSTB Shared Version:')
        self.leftlayout.addWidget(self.shared_label)
        self.shared_data = QtWidgets.QLabel('{}'.format(sharedvers))
        self.rightlayout.addWidget(self.shared_data)

        self.bgrid_label = QtWidgets.QLabel('Bathygrid Version:')
        self.leftlayout.addWidget(self.bgrid_label)
        self.bgrid_data = QtWidgets.QLabel('{}'.format(bathyvers))
        self.rightlayout.addWidget(self.bgrid_data)

        self.vyper_label = QtWidgets.QLabel('Vyperdatum Version:')
        self.leftlayout.addWidget(self.vyper_label)
        self.vyper_data = QtWidgets.QLabel('{}'.format(vypervers))
        self.rightlayout.addWidget(self.vyper_data)

        self.dask_label = QtWidgets.QLabel('Dask Version:')
        self.leftlayout.addWidget(self.dask_label)
        self.dask_data = QtWidgets.QLabel('{}'.format(daskvers))
        self.rightlayout.addWidget(self.dask_data)

        self.xarray_label = QtWidgets.QLabel('Xarray Version:')
        self.leftlayout.addWidget(self.xarray_label)
        self.xarray_data = QtWidgets.QLabel('{}'.format(xarrvers))
        self.rightlayout.addWidget(self.xarray_data)

        self.gdal_label = QtWidgets.QLabel('GDAL Version:')
        self.leftlayout.addWidget(self.gdal_label)
        self.gdal_data = QtWidgets.QLabel('{}'.format(gdalvers))
        self.rightlayout.addWidget(self.gdal_data)

        self.datalayout.addLayout(self.leftlayout)
        self.datalayout.addStretch()
        self.datalayout.addLayout(self.rightlayout)
        self.toplayout.addLayout(self.datalayout)
        self.toplayout.addStretch()

        self.hlayout_button = QtWidgets.QHBoxLayout()
        self.hlayout_button.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_button.addWidget(self.ok_button)
        self.hlayout_button.addStretch(1)
        self.toplayout.addLayout(self.hlayout_button)

        self.setLayout(self.toplayout)
        self.setMinimumWidth(210)
        self.setMaximumWidth(210)
        self.setMinimumHeight(250)
        self.setMaximumHeight(250)

        self.ok_button.clicked.connect(self.ok_dialog)

    def ok_dialog(self):
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = AboutDialog()
    dlog.show()
    app.exec_()
