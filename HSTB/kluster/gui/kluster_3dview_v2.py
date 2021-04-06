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
    Widget containing the Vispy scene.  Controled with mouse (rotation/translation) and dictated by the values
    shown in the OptionsWidget.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.canvas = DockableCanvas(keys='interactive', show=True, parent=parent)
        self.view = self.canvas.central_widget.add_view()
        self.view.camera = scene.TurntableCamera(up='-z', fov=45, translate_speed=5.0)  # arcball does not support -z up

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

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str):
        self.id = np.concatenate([self.id, np.full(x.shape[0], newid)])
        self.x = np.concatenate([self.x, x])
        self.y = np.concatenate([self.y, y])
        self.z = np.concatenate([self.z, z])
        self.tvu = np.concatenate([self.tvu, tvu])
        self.rejected = np.concatenate([self.rejected, rejected])
        self.pointtime = np.concatenate([self.pointtime, pointtime])
        self.beam = np.concatenate([self.beam, beam])

    def display_points(self, color_by: str = 'z'):
        if not self.z.any():
            return

        print('displaying {} points'.format(len(self.z)))
        if color_by == 'z':
            clrs = normalized_arr_to_rgb_v2((self.z - self.z.min()) / (self.z.max() - self.z.min()), reverse=True)
        elif color_by == 'tvu':
            clrs = normalized_arr_to_rgb_v2((self.tvu - self.tvu.min()) / (self.tvu.max() - self.tvu.min()))

        self.x_offset = self.x.mean()
        self.y_offset = self.y.mean()
        self.z_offset = self.z.mean()
        centered_x = self.x - self.x_offset
        centered_y = self.y - self.y_offset
        centered_z = self.z - self.z_offset

        pts = np.stack([centered_x, centered_y, centered_z], axis=1)
        scatter = scene.visuals.create_visual_node(visuals.MarkersVisual)
        self.scatter = scatter(parent=self.view.scene)
        self.scatter.set_gl_state('translucent', blend=True, depth_test=True)
        self.scatter.set_data(pts, edge_color=clrs, face_color=clrs, symbol='o', size=3)
        self.view.camera.center = (0, 0, 0)
        self.view.camera.distance = centered_x.max()

    def clear(self):
        if self.scatter is not None:
            self.scatter.parent = None
            self.scatter = None
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
        self.mainlayout.addWidget(self.three_d_window)

        instruct = 'Left Mouse Button: Rotate,  Right Mouse Button/Mouse wheel: Zoom,  Shift + Left Mouse Button: Move'
        self.instructions = QtWidgets.QLabel(instruct)
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic }")
        self.mainlayout.addWidget(self.instructions)

        self.mainlayout.setStretchFactor(self.three_d_window, 1)
        self.mainlayout.setStretchFactor(self.instructions, 0)
        self.setLayout(self.mainlayout)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str):
        self.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid)

    def display_points(self):
        self.three_d_window.display_points()

    def clear(self):
        self.three_d_window.clear()


def normalized_arr_to_rgb(z_array_normalized):
    clrs = [np.array([1, 0, 0, 1]), np.array([1, 0.6470588, 0, 1]), np.array([1, 1, 0, 1]), np.array([0, 0.5, 0, 1]),
            np.array([0, 0, 1, 1]), np.array([0.2941176, 0, 0.509804, 1]), np.array([0.93333, 0.509804, 0.93333, 1]),
            np.array([0, 0, 1, 1])]
    num_clrs = len(clrs)
    array_clrs = np.zeros((z_array_normalized.shape[0], 4))
    for i in range(num_clrs - 1):
        if i != 0:
            arr_idx = np.where(np.logical_and(z_array_normalized <= (i + 1) / (num_clrs - 1),
                                              z_array_normalized > i / (num_clrs - 1)))[0]
        else:
            arr_idx = np.where(z_array_normalized <= (i + 1) / (num_clrs - 1))[0]
        array_clrs[arr_idx] = np.linspace(clrs[i], clrs[i + 1], len(arr_idx))
    return array_clrs


def normalized_arr_to_rgb_v2(z_array_normalized, reverse=False):
    if reverse:
        rainbow = cm.get_cmap('rainbow_r', 50)
    else:
        rainbow = cm.get_cmap('rainbow', 50)
    return rainbow(z_array_normalized)


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    win = ThreeDWidget()
    x = np.array([508509.2345, 508510.2345])
    y = np.array([23509.2345, 23510.2345])
    xx, yy = np.meshgrid(x, y)
    x = xx.ravel()
    y = yy.ravel()
    z = np.array([60.123, 58.235, 57.234, 62.123])
    tvu = np.random.rand(x.shape[0])
    rejected = np.random.randint(0, 2, size=x.shape[0])
    pointtime = np.arange(x.shape[0])
    beam = np.random.randint(0, 399, size=x.shape[0])
    newid = 'test'

    win.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid)
    win.three_d_window.display_points()
    win.show()
    app.exec_()

