import numpy as np
import sys
from matplotlib import cm

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, backend

from vispy import use
from vispy.util import keys
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


class TurntableCameraInteractive(scene.TurntableCamera):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selected_callback = None
        self.fresh_camera = True

    def _bind_selecting_event(self, selfunc):
        self.selected_callback = selfunc

    def _handle_translate_event(self, start_pos, end_pos):
        self.fresh_camera = False
        norm = np.mean(self._viewbox.size)
        if self._event_value is None or len(self._event_value) == 2:
            self._event_value = self.center
        dist = (start_pos - end_pos) / norm * self._scale_factor * self.distance
        dist[1] *= -1
        # Black magic part 1: turn 2D into 3D translations
        dx, dy, dz = self._dist_to_trans(dist)
        # Black magic part 2: take up-vector and flipping into account
        ff = self._flip_factors
        up, forward, right = self._get_dim_vectors()
        dx, dy, dz = right * dx + forward * dy + up * dz
        dx, dy, dz = ff[0] * dx, ff[1] * dy, dz * ff[2]
        c = self._event_value
        self.center = c[0] + dx, c[1] + dy, c[2] + dz

    def _mouse_to_data_coordinates(self, mouse_position):
        cntr = self.center
        dist = mouse_position - (np.array(self._viewbox.size) / 2)
        dist = dist / np.array(self._viewbox.size) * self.distance
        dist[1] *= -1

        az_rad = np.deg2rad(self.azimuth)
        # take the max of az or el to determine persepective
        dx = dist[0] * np.cos(az_rad) - dist[1] * np.sin(az_rad)
        dy = dist[0] * np.sin(az_rad) + dist[1] * np.cos(az_rad)
        dz = 0
        newpt = cntr[0] + dx, cntr[1] + dy, cntr[2] + dz
        return newpt

    def _handle_zoom_event(self, distance):
        # Zoom
        self.fresh_camera = False
        if self._event_value is None:
            self._event_value = (self._scale_factor, self._distance)
        zoomy = (1 + self.zoom_factor) ** distance[1]

        self.scale_factor = self._event_value[0] * zoomy
        # Modify distance if its given
        if self._distance is not None:
            self._distance = self._event_value[1] * zoomy
        self.view_changed()

    def _handle_mousewheel_zoom_event(self, event):
        self.fresh_camera = False
        s = 1.1 ** - event.delta[1]
        self._scale_factor *= s
        if self._distance is not None:
            self._distance *= s
        self.view_changed()

    def _handle_fov_move_event(self, dist):
        self.fresh_camera = False
        # Change fov
        if self._event_value is None:
            self._event_value = self._fov
        fov = self._event_value - dist[1] / 5.0
        try:
            self.fov = min(180.0, max(0.0, fov))
        except TypeError:  # user let go of shift while dragging
            pass

    def _handle_data_selected(self, startpos, endpos):
        if self.selected_callback:
            if (startpos == endpos).all():
                startpos -= 10
                endpos += 10
            new_startpos = np.array([int(min(startpos[0], endpos[0])), int(min(startpos[1], endpos[1]))])
            new_endpos = np.array([int(max(startpos[0], endpos[0])), int(max(startpos[1], endpos[1]))])
            new_startpos = self._mouse_to_data_coordinates(new_startpos)
            new_endpos = self._mouse_to_data_coordinates(new_endpos)
            final_startpos = np.array([int(min(new_startpos[0], new_endpos[0])), int(min(new_startpos[1], new_endpos[1])),
                                       int(min(new_startpos[2], new_endpos[2]))])
            final_endpos = np.array([int(max(new_startpos[0], new_endpos[0])), int(max(new_startpos[1], new_endpos[1])),
                                     int(max(new_startpos[2], new_endpos[2]))])
            self.selected_callback(final_startpos, final_endpos)

    def viewbox_mouse_event(self, event):
        if event.handled or not self.interactive:
            return

        if event.type == 'mouse_wheel':
            self._handle_mousewheel_zoom_event(event)
        elif event.type == 'mouse_release':
            if event.press_event is None:
                self._event_value = None  # Reset
                return

            modifiers = event.mouse_event.modifiers
            p1 = event.mouse_event.press_event.pos
            p2 = event.mouse_event.pos
            if 1 in event.buttons and keys.CONTROL in modifiers:
                self._handle_data_selected(p1, p2)
            self._event_value = None  # Reset
        elif event.type == 'mouse_press':
            event.handled = True
        elif event.type == 'mouse_move':
            if event.press_event is None:
                return

            modifiers = event.mouse_event.modifiers
            p1 = event.mouse_event.press_event.pos
            p2 = event.mouse_event.pos
            d = p2 - p1
            if 1 in event.buttons and not modifiers:
                self._update_rotation(event)
            elif 1 in event.buttons and keys.SHIFT in modifiers:
                self._handle_translate_event(p1, p2)
            elif 2 in event.buttons:
                self._handle_zoom_event(d)


class ThreeDView(QtWidgets.QWidget):
    """
    Widget containing the Vispy scene.  Controlled with mouse (rotation/translation) and dictated by the values
    shown in the OptionsWidget.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.canvas = DockableCanvas(keys='interactive', show=True, parent=parent)
        self.view = self.canvas.central_widget.add_view()

        self.is_3d = True

        self.scatter = None
        self.scatter_transform = None
        self.scatter_select_range = None

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
        self.displayed_points = None
        self.selected_points = None

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

    def _select_points(self, startpos, endpos):
        if self.displayed_points is not None and self.parent is not None:
            self.parent.select_points(startpos, endpos)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str, is_3d: bool = True):
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
        is_3d
            Set this flag to notify widget that we are in 3d mode
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
        self.is_3d = is_3d

        if is_3d:
            self.view.camera = TurntableCameraInteractive()
            self.view.camera._bind_selecting_event(self._select_points)
        else:
            self.view.camera = 'panzoom'

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
        # print('displaying {} points'.format(len(self.z)))
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

        # we need to subtract the min of our arrays.  There is a known issue with vispy (maybe in opengl in general) that large
        # values (like northings/eastings) cause floating point problems and the point positions jitter as you move
        # the camera (as successive redraw commands are run).  By zero centering and saving the offset, we can display
        # the centered northing/easting and rebuild the original value by adding the offset back in if we need to
        self.x_offset = self.x.min()
        self.y_offset = self.y.min()
        self.z_offset = self.z.min()
        centered_x = self.x - self.x_offset
        centered_y = self.y - self.y_offset
        centered_z = self.z - self.z_offset

        # camera assumes z is positive up, flip the values
        centered_z = (centered_z - centered_z.max()) * -1 * vertical_exaggeration

        self.displayed_points = np.stack([centered_x, centered_y, centered_z], axis=1)
        scatter = scene.visuals.create_visual_node(visuals.MarkersVisual)
        self.scatter = scatter(parent=self.view.scene)
        self.scatter.set_gl_state('translucent', blend=True, depth_test=True)

        if self.selected_points is not None and self.selected_points.any():
            msk = np.zeros(self.displayed_points.shape[0], dtype=bool)
            msk[self.selected_points] = True
            clrs[msk, :] = (1, 1, 1, 1)

        if self.is_3d:
            self.scatter.set_data(self.displayed_points, edge_color=clrs, face_color=clrs, symbol='o', size=3)
            if self.view.camera.fresh_camera:
                self.view.camera.center = (centered_x.mean(), centered_y.mean(), centered_z.mean())
                self.view.camera.distance = centered_x.max()
                self.view.camera.fresh_camera = False
        else:
            self.scatter.set_data(self.displayed_points[:, [0, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
            self.view.camera.center = (centered_x.mean(), centered_z.mean())
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
        self.opts_layout.addStretch()

        self.mainlayout.addLayout(self.opts_layout)
        self.mainlayout.addWidget(self.three_d_window)

        instruct = 'Left Mouse Button: Rotate,  Right Mouse Button/Mouse wheel: Zoom,  Shift + Left Mouse Button: Move'
        instruct += ' Ctrl + Left Mouse Button: Query'
        self.instructions = QtWidgets.QLabel(instruct)
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic }")
        self.mainlayout.addWidget(self.instructions)

        self.mainlayout.setStretchFactor(self.three_d_window, 1)
        self.mainlayout.setStretchFactor(self.instructions, 0)
        self.setLayout(self.mainlayout)

        self.colorby.currentTextChanged.connect(self.refresh_settings)
        self.vertexag.valueChanged.connect(self.refresh_settings)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str, is_3d: bool):
        self.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid, is_3d)

    def select_points(self, startpos, endpos):
        startpos[2] = self.three_d_window.displayed_points[:, 2].min()
        endpos[2] = self.three_d_window.displayed_points[:, 2].max()
        m1 = self.three_d_window.displayed_points >= startpos
        m2 = self.three_d_window.displayed_points <= endpos
        self.three_d_window.selected_points = np.argwhere(m1[:, 0] & m1[:, 1] & m2[:, 0] & m2[:, 1])
        self.refresh_settings(None)

    def display_points(self):
        self.three_d_window.display_points(color_by=self.colorby.currentText(),
                                           vertical_exaggeration=self.vertexag.value())

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

