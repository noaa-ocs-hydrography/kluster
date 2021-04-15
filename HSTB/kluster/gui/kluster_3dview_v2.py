import numpy as np
import sys
from matplotlib import cm

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled, qgis_path, qgis_path_pydro, backend
from HSTB.kluster import kluster_variables

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


class PanZoomInteractive(scene.PanZoomCamera):
    """
    A custom camera for the 2d scatter plots.  Allows for interaction (i.e. querying of points) using the
    handle_data_selected callback.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selected_callback = None
        self.fresh_camera = True

    def _bind_selecting_event(self, selfunc):
        """
        Emit the top left/bottom right 3d coordinates of the selection.  Parent widget will control the 3dview and
        highlight the points as well as populating the explorer widget with the values so you can see.
        """
        self.selected_callback = selfunc

    def _handle_mousewheel_zoom_event(self, event):
        """
        Simple mousewheel zoom event handler, alters the zoom attribute
        """
        center = self._scene_transform.imap(event.pos)
        self.zoom((1 + self.zoom_factor) ** (-event.delta[1] * 30), center)
        self.fresh_camera = False

    def _handle_zoom_event(self, event):
        """
        Set the scale and center according to the new zoom.  Triggered on moving the mouse with right click.
        """

        p1c = np.array(event.last_event.pos)[:2]
        p2c = np.array(event.pos)[:2]
        scale = ((1 + self.zoom_factor) ** ((p1c - p2c) *
                                            np.array([1, -1])))
        center = self._transform.imap(event.press_event.pos[:2])
        self.zoom(scale, center)
        self.fresh_camera = False

    def _handle_translate_event(self, event):
        """
        Move the camera center according event pos, last pos.  This is called when dragging the mouse in translate
        mode.
        """
        p1 = np.array(event.last_event.pos)[:2]
        p2 = np.array(event.pos)[:2]
        p1s = self._transform.imap(p1)
        p2s = self._transform.imap(p2)
        self.pan(p1s - p2s)
        self.fresh_camera = False

    def _handle_data_selected(self, startpos, endpos):
        """
        Runs the parent method (selected_callback) to select points when the user holds down control and selects points
        with this camera.  Parent will highlight data and populate explorer widget with attributes

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

        if self.selected_callback:
            startpos = self._transform.imap(startpos)
            endpos = self._transform.imap(endpos)
            new_startpos = np.array([min(startpos[0], endpos[0]), min(startpos[1], endpos[1])])
            new_endpos = np.array([max(startpos[0], endpos[0]), max(startpos[1], endpos[1])])
            if (new_startpos == new_endpos).all():
                new_startpos -= 0.2
                new_endpos += 0.2
            self.selected_callback(new_startpos, new_endpos, three_d=False)

    def viewbox_mouse_event(self, event):
        """
        The SubScene received a mouse event; update transform
        accordingly.

        Parameters
        ----------
        event : instance of Event
            The event.
        """
        if event.handled or not self.interactive:
            return

        if event.type == 'mouse_wheel':
            self._handle_mousewheel_zoom_event(event)
            event.handled = True
        elif event.type == 'mouse_release':
            if event.press_event is None:
                return

            modifiers = event.mouse_event.modifiers
            p1 = event.mouse_event.press_event.pos
            p2 = event.mouse_event.pos
            if 1 in event.buttons and keys.CONTROL in modifiers:
                self._handle_data_selected(p1, p2)
            event.handled = True
        elif event.type == 'mouse_move':
            if event.press_event is None:
                return

            modifiers = event.mouse_event.modifiers
            if 1 in event.buttons and not modifiers:
                self._handle_translate_event(event)
                event.handled = True
            elif 1 in event.buttons and keys.CONTROL in modifiers:
                event.handled = True
            elif 2 in event.buttons and not modifiers:
                self._handle_zoom_event(event)
                event.handled = True
            else:
                event.handled = False
        elif event.type == 'mouse_press':
            event.handled = True
        else:
            event.handled = False


class TurntableCameraInteractive(scene.TurntableCamera):
    """
    A custom camera for 3d scatter plots.  Allows for interaction (i.e. querying of points) using the handle_data_selected
    callback.  I haven't quite yet figured out the selecting 3d points using 2d pixel coordinates just yet, it's been
    a bit of a struggle.  Currently I try to just select all in the z range so that I only have to deal with x and y.
    This is somewhat successful.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selected_callback = None
        self.fresh_camera = True

    def _bind_selecting_event(self, selfunc):
        """
        Emit the top left/bottom right 3d coordinates of the selection.  Parent widget will control the 3dview and
        highlight the points as well as populating the explorer widget with the values so you can see.
        """
        self.selected_callback = selfunc

    def _handle_translate_event(self, start_pos, end_pos):
        """
        Move the camera center according to the points provided.  This is called when dragging the mouse in translate
        mode.

        Parameters
        ----------
        start_pos
            Point where you first clicked
        end_pos
            Point where you released the mouse button after dragging
        """
        self.fresh_camera = False
        norm = np.mean(self._viewbox.size)
        if self._event_value is None or len(self._event_value) == 2:
            self._event_value = self.center
        dist = (start_pos - end_pos) / norm * self._scale_factor * (self.distance / 2)
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
        """
        Try and convert the mouse_position which is in screen coordinates to data coordinates.  This is a hell of a
        problem.  You can see that we currently just try and assume top down perspective, so we can just deal with
        x and y.  We rotate by the azimuth of the camera and combine camera center and the new distance to the mouse
        position to get the final mouse position.

        Parameters
        ----------
        mouse_position
            Position of the mouse in screen coordinates (top left is 0,0;  bottom right is size of screen, ex:800,600)

        Returns
        -------
        tuple
            mouse position in data coordinates (x, y, z).  Currently z is left zero, as we just query all points in min-max z
        """

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
        """
        Set the scale_factor and distance according to the new zoom.  Triggered on moving the mouse with right click.

        Parameters
        ----------
        distance
            distance the mouse moved since right click in 2d screen coordinates, i.e. (200,150)
        """

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
        """
        Simple mousewheel zoom event handler, alters the scale_factor and distance using the 1.1 constant
        """
        self.fresh_camera = False
        s = 1.1 ** - event.delta[1]
        self._scale_factor *= s
        if self._distance is not None:
            self._distance *= s
        self.view_changed()

    def _handle_data_selected(self, startpos, endpos):
        """
        Runs the parent method (selected_callback) to select points when the user holds down control and selects points
        with this camera.  Parent will highlight data and populate explorer widget with attributes

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

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
            self.selected_callback(final_startpos, final_endpos, three_d=True)

    def viewbox_mouse_event(self, event):
        """
        Handles all the mouse events allowed in this camera.  Includes:
        - mouse wheel zoom
        - move camera center with shift+left click and drag
        - select points with ctrl+left click and drag
        - rotate camera with left click and drag
        - zoom with right click and drag

        Parameters
        ----------
        event
            mouse event
        """

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
        self.axis_x = None
        self.axis_y = None
        self.axis_z = None

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
        self.linename = np.array([])

        self.x_offset = 0.0
        self.y_offset = 0.0
        self.z_offset = 0.0
        self.vertical_exaggeration = 1.0
        self.view_direction = 'north'
        self.displayed_points = None
        self.selected_points = None
        self.superselected_index = None

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

    def _select_points(self, startpos, endpos, three_d):
        """
        Trigger the parent method to highlight and display point data within the bounds provided by the two points

        Parameters
        ----------
        startpos
            Point where you first clicked
        endpos
            Point where you released the mouse button after dragging
        """

        if self.displayed_points is not None and self.parent is not None:
            self.parent.select_points(startpos, endpos, three_d=three_d)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str, linename: np.array, is_3d: bool):
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
        linename
            1d array of line names for each time
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
        self.linename = np.concatenate([self.linename, linename])
        self.is_3d = is_3d

        if is_3d:
            self.view.camera = TurntableCameraInteractive()
        else:
            self.view.camera = PanZoomInteractive()
        self.view.camera._bind_selecting_event(self._select_points)

    def setup_axes(self):
        if self.axis_x is not None:
            self.axis_x.parent = None
            self.axis_x = None
        if self.axis_y is not None:
            self.axis_y.parent = None
            self.axis_y = None
        if self.axis_z is not None:
            self.axis_z.parent = None
            self.axis_z = None

        if self.is_3d:
            max_x = self.x.max() - self.x.min()
            self.axis_x = scene.visuals.Arrow(pos=np.array([[0, 0], [max_x, 0]]), color='r', parent=self.view.scene,
                                              arrows=np.array([[max_x/50, 0, max_x, 0], [max_x/50, 0, max_x, 0]]),
                                              arrow_size=8, arrow_color='r', arrow_type='triangle_60')
            max_y = self.y.max() - self.y.min()
            self.axis_y = scene.visuals.Arrow(pos=np.array([[0, 0], [0, max_y]]), color='g', parent=self.view.scene,
                                              arrows=np.array([[0, max_y / 50, 0, max_y], [0, max_y / 50, 0, max_y]]),
                                              arrow_size=8, arrow_color='g', arrow_type='triangle_60')
            max_z = self.z.max()
            min_z = self.z.min()
            diff_z = (max_z - min_z) * self.vertical_exaggeration
            self.axis_z = scene.visuals.Arrow(pos=np.array([[0, 0, 0], [0, 0, diff_z]]), color='b', parent=self.view.scene,
                                              arrows=np.array([[0, 0, diff_z/50, 0, 0, diff_z], [0, 0, diff_z/50, 0, 0, diff_z]]),
                                              arrow_size=8, arrow_color='b', arrow_type='triangle_60')
        else:
            if self.view_direction == 'north':
                self.axis_x = scene.AxisWidget(orientation='bottom', domain=(0, self.x.max() - self.x.min()))
                self.axis_x.size = (self.x.max() - self.x.min(), 3)
            elif self.view_direction == 'east':
                self.axis_x = scene.AxisWidget(orientation='bottom', domain=(0, self.y.max() - self.y.min()))
                self.axis_x.size = (self.y.max() - self.y.min(), 3)
            self.view.add(self.axis_x)
            self.axis_z = scene.AxisWidget(orientation='right', domain=(self.z.min(), self.z.max()))
            self.axis_z.size = (3, (self.z.max() - self.z.min()) * self.vertical_exaggeration)
            self.view.add(self.axis_z)

    # def _rotate_along_across(self):
    #     rot_head = np.pi/2 - np.deg2rad(self.heading)
    #     newx = self.displayed_points[:, 0] * np.cos(rot_head) - self.displayed_points[:, 1] * np.sin(rot_head)
    #     neg_newx = np.where(newx < 0)
    #     newx[neg_newx] += self.displayed_points[:, 0].max()
    #     # lastbeam = np.where(self.beam == self.beam.max())[0]
    #     # max_across = np.zeros_like(self.displayed_points[:, 0])
    #     # strt = 0
    #     # for cnt, lb in enumerate(lastbeam):
    #     #     if cnt == len(lastbeam) - 1:
    #     #         max_across[strt:] = np.nanmax(self.displayed_points[:,0][strt:])
    #     #         break
    #     #     else:
    #     #         max_across[strt:lb + 1] = np.nanmax(self.displayed_points[:, 0][strt:lb + 1])
    #     #     strt = lb + 1
    #     # neg_newx = np.where(newx < 0)
    #     # newx[neg_newx] += max_across[neg_newx]
    #     return newx

    def display_points(self, color_by: str = 'depth', vertical_exaggeration: float = 1.0, view_direction: str = 'north'):
        """
        After adding all the points you want to add, call this method to then load them in opengl and draw them to the
        scatter plot

        Parameters
        ----------
        color_by
            identifer for the variable you want to color by.  One of 'depth', 'vertical_uncertainty', 'beam',
            'rejected', 'system', 'linename'
        vertical_exaggeration
            multiplier for z value
        view_direction
            picks either northings or eastings for display, only for 2d view
        """

        if not self.z.any():
            return

        self.view_direction = view_direction
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
        elif color_by in ['system', 'linename']:
            if color_by == 'system':
                vari = self.id
            else:
                vari = self.linename
            uvari = np.unique(vari)
            sys_idx = np.zeros(vari.shape[0], dtype=np.int32)
            for cnt, us in enumerate(uvari):
                usidx = vari == us
                sys_idx[usidx] = cnt + 1
            clrs = normalized_arr_to_rgb_v2(sys_idx / (len(uvari)), band_count=(len(uvari) + 1))
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
        self.scatter = scene.visuals.Markers(parent=self.view.scene)
        self.scatter.set_gl_state('translucent', blend=True, depth_test=True)

        if self.selected_points is not None and self.selected_points.any():
            msk = np.zeros(self.displayed_points.shape[0], dtype=bool)
            msk[self.selected_points] = True
            clrs[msk, :] = kluster_variables.selected_point_color
            if self.superselected_index is not None:
                msk[:] = False
                msk[self.selected_points[self.superselected_index]] = True
                clrs[msk, :] = kluster_variables.super_selected_point_color

        if self.is_3d:
            self.scatter.set_data(self.displayed_points, edge_color=clrs, face_color=clrs, symbol='o', size=3)
            if self.view.camera.fresh_camera:
                self.view.camera.center = (centered_x.mean(), centered_y.mean(), centered_z.mean())
                self.view.camera.distance = centered_x.max()
                self.view.camera.fresh_camera = False
        else:
            if self.view_direction == 'north':
                self.scatter.set_data(self.displayed_points[:, [0, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
            elif self.view_direction == 'east':
                self.scatter.set_data(self.displayed_points[:, [1, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
            self.view.camera.center = (centered_x.mean(), centered_z.mean())
            if self.view.camera.fresh_camera:
                self.view.camera.zoom(centered_x.max() + 10)  # try and fit the swath in view on load
                self.view.camera.fresh_camera = False
        self.setup_axes()

    def clear_display(self):
        """
        Have to clear the scatterplot each time we update the display, do so by setting the parent of the plot to None
        """
        if self.scatter is not None:
            # By setting the scatter visual parent to None, we delete it (clearing the widget)
            self.scatter.parent = None
            self.scatter = None

    def clear(self):
        """
        Clear display and all stored data
        """
        self.clear_display()
        self.id = np.array([])
        self.x = np.array([])
        self.y = np.array([])
        self.z = np.array([])
        self.tvu = np.array([])
        self.rejected = np.array([])
        self.pointtime = np.array([])
        self.beam = np.array([])
        self.linename = np.array([])


class ThreeDWidget(QtWidgets.QWidget):
    """
    Widget containing the OptionsWidget (left pane) and VesselView (right pane).  Manages the signals that connect the
    two widgets.
    """

    points_selected = Signal(object, object, object, object, object, object, object, object, object, object)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.three_d_window = ThreeDView(self)

        self.mainlayout = QtWidgets.QVBoxLayout()

        self.opts_layout = QtWidgets.QHBoxLayout()
        self.colorby_label = QtWidgets.QLabel('Color By: ')
        self.opts_layout.addWidget(self.colorby_label)
        self.colorby = QtWidgets.QComboBox()
        self.colorby.addItems(['depth', 'vertical_uncertainty', 'beam', 'rejected', 'system', 'linename'])
        self.opts_layout.addWidget(self.colorby)
        self.vertexag_label = QtWidgets.QLabel('Vertical Exaggeration: ')
        self.opts_layout.addWidget(self.vertexag_label)
        self.vertexag = QtWidgets.QDoubleSpinBox()
        self.vertexag.setMaximum(99.0)
        self.vertexag.setMinimum(1.0)
        self.vertexag.setSingleStep(0.5)
        self.vertexag.setValue(1.0)
        self.opts_layout.addWidget(self.vertexag)
        self.viewdirection_label = QtWidgets.QLabel('View Direction: ')
        self.viewdirection_label.hide()
        self.opts_layout.addWidget(self.viewdirection_label)
        self.viewdirection = QtWidgets.QComboBox()
        self.viewdirection.addItems(['north', 'east'])
        self.viewdirection.hide()
        self.opts_layout.addWidget(self.viewdirection)
        self.opts_layout.addStretch()

        self.mainlayout.addLayout(self.opts_layout)
        self.mainlayout.addWidget(self.three_d_window)

        instruct = 'Left Mouse Button: Rotate,  Right Mouse Button/Mouse wheel: Zoom,  Shift + Left Mouse Button: Move,'
        instruct += ' Ctrl + Left Mouse Button: Query'
        self.instructions = QtWidgets.QLabel(instruct)
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic }")
        self.mainlayout.addWidget(self.instructions)

        self.mainlayout.setStretchFactor(self.three_d_window, 1)
        self.mainlayout.setStretchFactor(self.instructions, 0)
        self.setLayout(self.mainlayout)

        self.colorby.currentTextChanged.connect(self.refresh_settings)
        self.viewdirection.currentTextChanged.connect(self.refresh_settings)
        self.vertexag.valueChanged.connect(self.refresh_settings)

    def add_points(self, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str, linename: str, is_3d: bool):
        if is_3d:
            self.viewdirection.hide()
            self.viewdirection_label.hide()
        else:
            self.viewdirection.show()
            self.viewdirection_label.show()
        self.three_d_window.selected_points = None
        self.three_d_window.superselected_index = None
        self.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid, linename, is_3d)

    def select_points(self, startpos, endpos, three_d):
        vd = self.viewdirection.currentText()
        if three_d:
            startpos[2] = self.three_d_window.displayed_points[:, 2].min()
            endpos[2] = self.three_d_window.displayed_points[:, 2].max()
            m1 = self.three_d_window.displayed_points[:, [0, 1, 2]] >= startpos[0:3]
            m2 = self.three_d_window.displayed_points[:, [0, 1, 2]] <= endpos[0:3]
        else:
            if vd == 'north':
                m1 = self.three_d_window.displayed_points[:, [0, 2]] >= startpos[0:2]
                m2 = self.three_d_window.displayed_points[:, [0, 2]] <= endpos[0:2]
            elif vd == 'east':
                m1 = self.three_d_window.displayed_points[:, [1, 2]] >= startpos[0:2]
                m2 = self.three_d_window.displayed_points[:, [1, 2]] <= endpos[0:2]
        self.three_d_window.selected_points = np.argwhere(m1[:, 0] & m1[:, 1] & m2[:, 0] & m2[:, 1])[:, 0]
        self.points_selected.emit(np.arange(self.three_d_window.selected_points.shape[0]),
                                  self.three_d_window.linename[self.three_d_window.selected_points],
                                  self.three_d_window.pointtime[self.three_d_window.selected_points],
                                  self.three_d_window.beam[self.three_d_window.selected_points],
                                  self.three_d_window.x[self.three_d_window.selected_points],
                                  self.three_d_window.y[self.three_d_window.selected_points],
                                  self.three_d_window.z[self.three_d_window.selected_points],
                                  self.three_d_window.tvu[self.three_d_window.selected_points],
                                  self.three_d_window.rejected[self.three_d_window.selected_points],
                                  self.three_d_window.id[self.three_d_window.selected_points])
        self.refresh_settings(None)

    def superselect_point(self, superselect_index):
        self.three_d_window.superselected_index = superselect_index
        self.refresh_settings(None)

    def display_points(self):
        self.three_d_window.display_points(color_by=self.colorby.currentText(),
                                           vertical_exaggeration=self.vertexag.value(),
                                           view_direction=self.viewdirection.currentText())

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
    linename = np.full(x.shape[0], '')
    newid = 'test'

    win.three_d_window.add_points(x, y, z, tvu, rejected, pointtime, beam, newid, linename, is_3d=True)
    win.three_d_window.display_points()
    win.show()
    app.exec_()

