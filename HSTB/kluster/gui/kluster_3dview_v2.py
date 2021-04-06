import numpy as np
import sys
from matplotlib import cm

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, backend

from vispy import use
use(backend, 'gl2')

from vispy import visuals, scene


class DockableCanvas(scene.SceneCanvas):
    """
    Calls to on_draw when the widget is undocked and you are dragging it around fail.  We need to catch those failed
    attempts here and just let them go.  Assumes it fails because of undocking actions...
    """
    def __init__(self, keys, show, parent):
        super().__init__(keys=keys, show=show, parent=parent)

    def on_draw(self, event):
        try:
            super().on_draw(event)
        except:
            pass


class ThreeDView(QtWidgets.QWidget):
    """
    Widget containing the Vispy scene.  Controlled with mouse (rotation/translation) and dictated by the values
    shown in the OptionsWidget.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.canvas = DockableCanvas(keys='interactive', show=True, parent=parent)
        self.view = self.canvas.central_widget.add_view()
        # our z is positive down, up=-z tells the camera how to behave in this case
        self.view.camera = scene.TurntableCamera(up='-z', fov=45)  # arcball does not support -z up

        self.scatter = None
        self.id = np.array([])
        self.x = np.array([])
        self.y = np.array([])
        self.z = np.array([])
        self.tvu = np.array([])
        self.rejected = np.array([])
        self.pointtime = np.array([])
        self.beam = np.array([])

        self.x_offset = 0.0
        self.y_offset = 0.0
        self.z_offset = 0.0
        self.vertical_exaggeration = 1.0

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str):
        """
        Add points to the 3d view widget, we only display points after all points are added, hence the separate methods

        Parameters
        ----------
        x
            easting
        y
            northing
        z
            depth value, positive down assumed
        tvu
            vertical uncertainty
        rejected
            The accepted/rejected state of each beam.  2 = rejected, 1 = phase detection, 0 = amplitude detection
        pointtime
            time of the sounding
        beam
            beam number of the sounding
        newid
            container name the sounding came from, ex: 'EM710_234_02_10_2019'
        """

        # expand the identifier to be the size of the input arrays
        self.id = np.concatenate([self.id, np.full(x.shape[0], newid)])
        self.x = np.concatenate([self.x, x])
        self.y = np.concatenate([self.y, y])
        self.z = np.concatenate([self.z, z])
        self.tvu = np.concatenate([self.tvu, tvu])
        self.rejected = np.concatenate([self.rejected, rejected])
        self.pointtime = np.concatenate([self.pointtime, pointtime])
        self.beam = np.concatenate([self.beam, beam])

    def display_points(self, color_by: str = 'depth', vertical_exaggeration: float = 1.0):
        """
        After adding all the points you want to add, call this method to then load them in opengl and draw them to the
        scatter plot

        Parameters
        ----------
        color_by
            identifer for the variable you want to color by.  One of 'depth', 'vertical_uncertainty', 'beam',
            'rejected', 'system'
        vertical_exaggeration
            multiplier for z value
        """

        if not self.z.any():
            return

        self.vertical_exaggeration = vertical_exaggeration
        print('displaying {} points'.format(len(self.z)))
        # normalize the arrays and build the colors for each sounding
        if color_by == 'depth':
            clrs = normalized_arr_to_rgb_v2((self.z - self.z.min()) / (self.z.max() - self.z.min()), reverse=True)
        elif color_by == 'vertical_uncertainty':
            clrs = normalized_arr_to_rgb_v2((self.tvu - self.tvu.min()) / (self.tvu.max() - self.tvu.min()))
        elif color_by == 'beam':
            clrs = normalized_arr_to_rgb_v2(self.beam / self.beam.max(), band_count=self.beam.max())
        elif color_by == 'rejected':
            clrs = normalized_arr_to_rgb_v2((self.rejected - self.rejected.min()) / (self.rejected.max() - self.rejected.min()),
                                            band_count=3, colormap='bwr')
        elif color_by == 'system':
            usystem = np.unique(self.id)
            sys_idx = np.zeros(self.id.shape[0], dtype=np.int32)
            for cnt, us in enumerate(usystem):
                usidx = self.id == us
                sys_idx[usidx] = cnt + 1
            clrs = normalized_arr_to_rgb_v2(sys_idx / (len(usystem)), band_count=(len(usystem)))
        else:
            raise ValueError('Coloring by {} is not supported at this time'.format(color_by))

        # we need to zero center our arrays.  There is a known issue with vispy (maybe in opengl in general) that large
        # values (like northings/eastings) cause floating point problems and the point positions jitter as you move
        # the camera (as successive redraw commands are run).  By zero centering and saving the offset, we can display
        # the zero centered northing/easting and rebuild the original value by adding the offset back in if we need to
        self.x_offset = self.x.mean()
        self.y_offset = self.y.mean()
        self.z_offset = self.z.mean()
        centered_x = self.x - self.x_offset
        centered_y = self.y - self.y_offset
        centered_z = self.z - self.z_offset

        centered_z = centered_z * vertical_exaggeration

        pts = np.stack([centered_x, centered_y, centered_z], axis=1)
        scatter = scene.visuals.create_visual_node(visuals.MarkersVisual)
        self.scatter = scatter(parent=self.view.scene)
        self.scatter.set_gl_state('translucent', blend=True, depth_test=True)
        self.scatter.set_data(pts, edge_color=clrs, face_color=clrs, symbol='o', size=3)
        self.view.camera.center = (0, 0, 0)
        self.view.camera.distance = centered_x.max()

    def clear_display(self):
        if self.scatter is not None:
            # By setting the scatter visual parent to None, we delete it (clearing the widget)
            self.scatter.parent = None
            self.scatter = None

    def clear(self):
        self.clear_display()
        self.id = np.array([])
        self.x = np.array([])
        self.y = np.array([])
        self.z = np.array([])
        self.tvu = np.array([])
        self.rejected = np.array([])
        self.pointtime = np.array([])
        self.beam = np.array([])


class ThreeDWidget(QtWidgets.QWidget):
    """
    Widget containing the OptionsWidget (left pane) and VesselView (right pane).  Manages the signals that connect the
    two widgets.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.three_d_window = ThreeDView(self)

        self.mainlayout = QtWidgets.QVBoxLayout()

        self.opts_layout = QtWidgets.QHBoxLayout()
        self.colorby_label = QtWidgets.QLabel('Color By: ')
        self.opts_layout.addWidget(self.colorby_label)
        self.colorby = QtWidgets.QComboBox()
        self.colorby.addItems(['depth', 'vertical_uncertainty', 'beam', 'rejected', 'system'])
        self.opts_layout.addWidget(self.colorby)
        self.vertexag_label = QtWidgets.QLabel('Vertical Exaggeration: ')
        self.opts_layout.addWidget(self.vertexag_label)
        self.vertexag = QtWidgets.QDoubleSpinBox()
        self.vertexag.setMaximum(99.0)
        self.vertexag.setMinimum(1.0)
        self.vertexag.setSingleStep(0.5)
        self.vertexag.setValue(1.0)
        self.opts_layout.addWidget(self.vertexag)
        self.movespeed_label = QtWidgets.QLabel('Move Speed: ')
        self.opts_layout.addWidget(self.movespeed_label)
        self.movespeed = QtWidgets.QDoubleSpinBox()
        self.movespeed.setMaximum(99.0)
        self.movespeed.setMinimum(1.0)
        self.movespeed.setSingleStep(1.0)
        self.movespeed.setValue(1.0)
        self.opts_layout.addWidget(self.movespeed)
        self.opts_layout.addStretch()

        self.mainlayout.addLayout(self.opts_layout)
        self.mainlayout.addWidget(self.three_d_window)

        instruct = 'Left Mouse Button: Rotate,  Right Mouse Button/Mouse wheel: Zoom,  Shift + Left Mouse Button: Move'
        self.instructions = QtWidgets.QLabel(instruct)
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic }")
        self.mainlayout.addWidget(self.instructions)

        self.mainlayout.setStretchFactor(self.three_d_window, 1)
        self.mainlayout.setStretchFactor(self.instructions, 0)
        self.setLayout(self.mainlayout)

        self.colorby.currentTextChanged.connect(self.refresh_settings)
        self.vertexag.valueChanged.connect(self.refresh_settings)
        self.movespeed.valueChanged.connect(self.adjust_camera)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str):
        self.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid)

    def display_points(self):
        self.three_d_window.display_points(color_by=self.colorby.currentText(),
                                           vertical_exaggeration=self.vertexag.value())

    def adjust_camera(self):
        move_speed = self.movespeed.value()
        self.three_d_window.view.camera.translate_speed = move_speed

    def refresh_settings(self, e):
        self.clear_display()
        self.display_points()

    def clear_display(self):
        self.three_d_window.clear_display()

    def clear(self):
        self.three_d_window.clear()


def normalized_arr_to_rgb_v2(z_array_normalized: np.array, reverse: bool = False, colormap: str = 'rainbow',
                             band_count: int = 50):
    """
    Build an RGB array from a normalized input array.  Colormap will be a string identifier that can be parsed by
    the matplotlib colormap object.

    Parameters
    ----------
    z_array_normalized
        1d array that you want to convert to an RGB array, values should be between 0 and 1
    reverse
        if true, reverses the colormap
    colormap
        string identifier that is accepted by matplotlib
    band_count
        number of color bands to pull from the colormap

    Returns
    -------
    np.array
        (n,4) array, where n is the length of z_array_normalized
    """
    if reverse:
        rainbow = cm.get_cmap(colormap + '_r', band_count)
    else:
        rainbow = cm.get_cmap(colormap, band_count)
    return rainbow(z_array_normalized)


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    win = ThreeDWidget()
    x = np.random.rand(200) * 500000
    y = np.random.rand(200) * 50000
    xx, yy = np.meshgrid(x, y)
    x = xx.ravel()
    y = yy.ravel()
    z = np.arange(x.shape[0])
    tvu = np.random.rand(x.shape[0])
    rejected = np.random.randint(0, 3, size=x.shape[0])
    pointtime = np.arange(x.shape[0])
    beam = np.random.randint(0, 399, size=x.shape[0])
    newid = 'test'

    win.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid)
    win.three_d_window.display_points()
    win.show()
    app.exec_()

