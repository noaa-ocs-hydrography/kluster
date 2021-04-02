import numpy as np
import sys


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
        self.view.camera = scene.TurntableCamera(up='-z', fov=45)  # arcball does not support -z up

        self.scatter = None

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

        self.add_points(np.array([0]), np.array([0]), np.array([0]))

    def add_points(self, x: np.array, y: np.array, z: np.array):
        if z.any():
            clrs = normalized_arr_to_rgb(z / z.max())
        else:
            clrs = normalized_arr_to_rgb(z)
        pts = np.stack([x, y, z], axis=1)
        scatter = scene.visuals.create_visual_node(visuals.MarkersVisual)
        self.scatter = scatter(parent=self.view.scene)
        self.scatter.set_gl_state('translucent', blend=True, depth_test=True)
        self.scatter.set_data(pts, face_color=clrs, symbol='o', size=3,
                              edge_width=0.5, edge_color='blue')
        self.view.camera.center = (x.mean(), y.mean(), z.mean())
        self.view.camera.distance = ((x.max() - x.mean()) + (y.max() - y.mean())) * 2




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

        self.setLayout(self.mainlayout)


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


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    win = ThreeDWidget()
    x = np.linspace(1000, 2000, 100)
    y = np.linspace(1000, 2000, 100)
    xx, yy = np.meshgrid(x, y)
    x = xx.ravel()
    y = yy.ravel()
    z = np.random.rand(x.shape[0]) * 1000
    win.three_d_window.add_points(x, y, z)
    win.show()
    app.exec_()



# # build your visuals, that's all
# Scatter3D = scene.visuals.create_visual_node(visuals.MarkersVisual)
#
# # The real-things : plot using scene
# # build canvas
# canvas = scene.SceneCanvas(keys='interactive', show=True)
#
# # Add a ViewBox to let the user zoom/rotate
# view = canvas.central_widget.add_view()
# view.camera = 'turntable'
# view.camera.fov = 45
# view.camera.distance = 500
#
# # data
# n = 500
# pos = np.zeros((n, 3))
# colors = np.ones((n, 4), dtype=np.float32)
# radius, theta, dtheta = 1.0, 0.0, 10.5 / 180.0 * np.pi
# for i in range(500):
#     theta += dtheta
#     x = 0.0 + radius * np.cos(theta)
#     y = 0.0 + radius * np.sin(theta)
#     z = 1.0 * radius
#     r = 10.1 - i * 0.02
#     radius -= 0.45
#     pos[i] = x, y, z
#     colors[i] = (i/500, 1.0-i/500, 0, 0.8)
#
# # plot ! note the parent parameter
# p1 = Scatter3D(parent=view.scene)
# p1.set_gl_state('translucent', blend=True, depth_test=True)
# p1.set_data(pos, face_color=colors, symbol='o', size=10,
#             edge_width=0.5, edge_color='blue')
#
# # run
# if __name__ == '__main__':
#     if sys.flags.interactive != 1:
#         app.run()