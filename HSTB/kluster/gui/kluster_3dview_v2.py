# Import qt first, to resolve the backend issues youll get in matplotlib if you dont import this first, as it prefers PySide2
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled, backend
if qgis_enabled:
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

import matplotlib
matplotlib.use('qt5agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib import cm
from matplotlib.figure import Figure
from matplotlib.colors import ListedColormap

import numpy as np

from HSTB.kluster import kluster_variables

from vispy import use, visuals, scene
from vispy.util import keys
use(backend, 'gl2')


def rectangle_vertice(center, height, width):
    """
    See https://github.com/vispy/vispy/blob/64e76c40c8d7d38efc53c9a1a50ca7f336b9ffc2/examples/basics/scene/points_selection.py
    for source.  Build rectangle coordinates using the provided center, height, width
    """

    half_height = height / 2.
    half_width = width / 2.

    bias1 = np.ones(4) * half_width
    bias2 = np.ones(4) * half_height

    corner1 = np.empty([1, 3], dtype=np.float32)
    corner2 = np.empty([1, 3], dtype=np.float32)
    corner3 = np.empty([1, 3], dtype=np.float32)
    corner4 = np.empty([1, 3], dtype=np.float32)

    corner1[:, 0] = center[0] - bias1[0]
    corner1[:, 1] = center[1] - bias2[0]
    corner1[:, 2] = 0

    corner2[:, 0] = center[0] + bias1[1]
    corner2[:, 1] = center[1] - bias2[1]
    corner2[:, 2] = 0

    corner3[:, 0] = center[0] + bias1[2]
    corner3[:, 1] = center[1] + bias2[2]
    corner3[:, 2] = 0

    corner4[:, 0] = center[0] - bias1[3]
    corner4[:, 1] = center[1] + bias2[3]
    corner4[:, 2] = 0

    # Get vertices between each corner of the rectangle for border drawing
    vertices = np.concatenate(([[center[0], center[1], 0.]],
                               [[center[0] - half_width, center[1], 0.]],
                               corner1,
                               [[center[0], center[1] - half_height, 0.]],
                               corner2,
                               [[center[0] + half_width, center[1], 0.]],
                               corner3,
                               [[center[0], center[1] + half_height, 0.]],
                               corner4,
                               [[center[0] - half_width, center[1], 0.]]))

    # vertices = np.array(output, dtype=np.float32)
    return vertices[1:, ..., :2]


class ScaleAxisWidget(scene.AxisWidget):
    def __init__(self, add_factor: float = 0.0, **kwargs):
        super().__init__(**kwargs)
        self.unfreeze()
        self.add_factor = add_factor

    def _view_changed(self, event=None):
        """Linked view transform has changed; update ticks.
        """
        tr = self.node_transform(self._linked_view.scene)
        p1, p2 = tr.map(self._axis_ends())
        if self.orientation in ('left', 'right'):
            self.axis.domain = (p1[1] + self.add_factor, p2[1] + self.add_factor)
        else:
            self.axis.domain = (p1[0] + self.add_factor, p2[0] + self.add_factor)


class ColorBar(FigureCanvasQTAgg):
    """
    Custom widget with QT backend showing just a colorbar.  We can tack this on to our 3dview 2dview widgets to show
    a colorbar.  Seems like using matplotlib is the way to go, I wasn't able to find any native qt widget for this.
    """
    def __init__(self, parent=None, width: int = 1.1, height: int = 4, dpi: int = 100):
        self.parent = parent
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.fig.gca().set_visible(False)
        super().__init__(self.fig)
        self.c_map_ax = self.fig.add_axes([0.05, 0.05, 0.25, 0.9])
        self.c_map_ax.get_xaxis().set_visible(False)
        self.c_map_ax.get_yaxis().set_visible(False)
        # self.fig.set_facecolor('black')

    def setup_colorbar(self, cmap: matplotlib.colors.Colormap, minval: float, maxval: float, is_rejected: bool = False,
                       by_name: list = None, invert_y: bool = False):
        """
        Provide a color map and a min max value to build the colorbar

        Parameters
        ----------
        cmap
            provide a colormap to use for the color bar
        minval
            min value of the color bar
        maxval
            max value of the color bar
        is_rejected
            if is rejected, set the custom tick labels
        by_name
            if this is populated, we will show the colorbar with ticks equal to the length of the list and labels equal
            to the list
        invert_y
            invert the y axis
        """

        self.c_map_ax.get_xaxis().set_visible(True)
        self.c_map_ax.get_yaxis().set_visible(True)
        self.c_map_ax.clear()
        norm = matplotlib.colors.Normalize(vmin=minval, vmax=maxval)
        if is_rejected:
            self.fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), orientation='vertical', cax=self.c_map_ax,
                              ticks=[3, 2, 1, 0])
            self.c_map_ax.set_yticklabels(['Re-Accept', 'Reject', 'Phase', 'Amplitude'])
            self.c_map_ax.tick_params(labelsize=8)
        elif by_name:
            self.fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), orientation='vertical', cax=self.c_map_ax,
                              ticks=(np.arange(len(by_name)) + 0.5).tolist())
            self.c_map_ax.set_yticklabels(by_name)
            self.c_map_ax.tick_params(labelsize=7)
        else:
            try:
                self.fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), orientation='vertical', cax=self.c_map_ax)
                self.c_map_ax.tick_params(labelsize=9)
            except IndexError:  # some colorbars like 'system' can rely on data that might not be loaded on starting Kluster
                pass
        if invert_y:
            self.c_map_ax.invert_yaxis()
        self.draw()


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
        self.clean_callback = None
        self.accept_callback = None
        self.undo_callback = None
        self.fresh_camera = True

    def _bind_event(self, selfunc, eventname):
        """
        Emit the top left/bottom right 3d coordinates of the selection.  Parent widget will control the 3dview and
        highlight the points as well as populating the explorer widget with the values so you can see.
        """
        if eventname == 'select':
            self.selected_callback = selfunc
        elif eventname == 'clean':
            self.clean_callback = selfunc
        elif eventname == 'undo':
            self.undo_callback = selfunc
        elif eventname == 'accept':
            self.accept_callback = selfunc
        else:
            raise NotImplementedError('Only select, clean, accept and undo functions currently supported, received {}'.format(eventname))

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

    def _handle_data_events(self, startpos, endpos):
        """
        Take the provided startpos/endpos mouse positions and build the data coordinates for the selection box using
        the camera transform

        Parameters
        ----------
        startpos
            [x coordinate, y coordinate] in pixel coordinates of the start of the box
        endpos
            [x coordinate, y coordinate] in pixel coordinates of the end of the box

        Returns
        -------
        list
            [x coordinate, y coordinate] in data coordinates of the start of the box
        list
            [x coordinate, y coordinate] in data coordinates of the end of the box
        """

        startpos = [startpos[0] - 80, startpos[1] - 10]  # camera transform seems to not handle the new twod_grid
        endpos = [endpos[0] - 80, endpos[1] - 10]  # add these on to handle the buffers until we figure it out
        startpos = self._transform.imap(startpos)
        endpos = self._transform.imap(endpos)
        new_startpos = np.array([min(startpos[0], endpos[0]), min(startpos[1], endpos[1])])
        new_endpos = np.array([max(startpos[0], endpos[0]), max(startpos[1], endpos[1])])

        if (new_startpos == new_endpos).all():
            new_startpos -= np.array([0.1, 0.02])
            new_endpos += np.array([0.1, 0.02])
        return new_startpos, new_endpos

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
            new_startpos, new_endpos = self._handle_data_events(startpos, endpos)
            self.selected_callback(new_startpos, new_endpos, three_d=False)

    def _handle_data_cleaned(self, startpos, endpos):
        """
        Runs the parent method (clean_callback) to reject points when the user holds down alt and selects points
        with this camera.

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

        if self.clean_callback:
            new_startpos, new_endpos = self._handle_data_events(startpos, endpos)
            self.clean_callback(new_startpos, new_endpos, three_d=False)

    def _handle_data_accepted(self, startpos, endpos):
        """
        Runs the parent method (clean_callback) to accept points when the user holds down alt and selects points
        with this camera.

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

        if self.accept_callback:
            new_startpos, new_endpos = self._handle_data_events(startpos, endpos)
            self.accept_callback(new_startpos, new_endpos, three_d=False)

    def _handle_data_undo(self):
        """
        Runs the parent method (undo_callback) to undo reject points when the user holds down alt and right clicks with this camera.
        """

        if self.undo_callback:
            self.undo_callback()

    def viewbox_mouse_event(self, event):
        """
        The SubScene received a mouse event; update transform accordingly.

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
            if 1 in event.buttons and keys.ALT in modifiers:
                self._handle_data_cleaned(p1, p2)
            if 2 in event.buttons and keys.ALT in modifiers:
                self._handle_data_accepted(p1, p2)
            if 3 in event.buttons and keys.ALT in modifiers:
                self._handle_data_undo()
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
        self.clean_callback = None
        self.accept_callback = None
        self.undo_callback = None
        self.fresh_camera = True

    def _bind_event(self, selfunc, eventname):
        """
        Emit the top left/bottom right 3d coordinates of the selection.  Parent widget will control the 3dview and
        highlight the points as well as populating the explorer widget with the values so you can see.
        """
        if eventname == 'select':
            self.selected_callback = selfunc
        elif eventname == 'clean':
            self.clean_callback = selfunc
        elif eventname == 'undo':
            self.undo_callback = selfunc
        elif eventname == 'accept':
            self.accept_callback = selfunc
        else:
            raise NotImplementedError(
                'Only select, clean, accept and undo functions currently supported, received {}'.format(eventname))

    def _3drot_vector(self, x, y, z, inv: bool = False):
        """
        Rotate the provided vector coordinates to camera roll, azimuth, elevation
        """
        # modeled after the _dist_to_trans method, appears to be some kind of almost YXZ tait-bryan standard.  I can't
        # seem to replicate this using scipy rotation
        rae = np.array([self.roll, self.azimuth, self.elevation]) * np.pi / 180
        sro, saz, sel = np.sin(rae)
        cro, caz, cel = np.cos(rae)
        if not inv:
            dx = (+ x * (cro * caz + sro * sel * saz)
                  + y * (sro * caz - cro * sel * saz)
                  + z * (cel * saz))
            dy = (+ x * (cro * saz - sro * sel * caz)
                  + y * (sro * saz + cro * sel * caz)
                  + z * (cel * caz))
            dz = (- x * (sro * cel)
                  + y * (cro * cel)
                  + z * sel)
        else:  # this rotates from already rotated data coordinates to pixel camera coordinates
            dx = (+ x * (cro * caz + sro * sel * saz)
                  + y * (cro * saz - sro * sel * caz)
                  - z * (sro * cel))
            dy = (+ x * (sro * caz - cro * sel * saz)
                  + y * (sro * saz + cro * sel * caz)
                  + z * (cro * cel))
            dz = (+ x * (cel * saz)
                  + y * (cel * caz)
                  + z * sel)
        return dx, dy, dz

    def _dist_between_mouse_coords(self, start_pos: np.array, end_pos: np.array):
        """
        Build the distance between the two screen coordinate arrays, taking into account the camera distance (i.e. zoom
        level).  Returns distance in data coordinates

        Parameters
        ----------
        start_pos
            (x position, y position) for start of distance vector
        end_pos
            (x position, y position) for end of distance vector

        Returns
        -------
        np.array
            (x distance, y distance)
        """

        dist = (start_pos - end_pos) / self._viewbox.size * self.distance
        return dist

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
        if self._event_value is None or len(self._event_value) == 2:
            self._event_value = self.center
        dist = self._dist_between_mouse_coords(start_pos, end_pos)
        dist[1] *= -1
        dx, dy, dz = self._3drot_vector(dist[0], dist[1], 0)
        # Black magic part 2: take up-vector and flipping into account
        ff = self._flip_factors
        up, forward, right = self._get_dim_vectors()
        dx, dy, dz = right * dx + forward * dy + up * dz
        dx, dy, dz = ff[0] * dx, ff[1] * dy, dz * ff[2]
        c = self._event_value
        self.center = c[0] + dx, c[1] + dy, c[2] + dz
        self.view_changed()

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
            self.selected_callback(startpos, endpos, three_d=True)

    def _handle_data_cleaned(self, startpos, endpos):
        """
        Runs the parent method (clean_callback) to reject points when the user holds down alt and selects points
        with this camera.

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

        if self.clean_callback:
            self.clean_callback(startpos, endpos, three_d=True)

    def _handle_data_accepted(self, startpos, endpos):
        """
        Runs the parent method (clean_callback) to Accept points when the user holds down alt and selects points
        with this camera.

        Parameters
        ----------
        startpos
            click position in screen coordinates
        endpos
            mouse click release position in screen coordinates
        """

        if self.accept_callback:
            self.accept_callback(startpos, endpos, three_d=True)

    def _handle_data_undo(self):
        """
        Runs the parent method (undo_callback) to undo reject points when the user holds down alt and right clicks with this camera.
        """

        if self.undo_callback:
            self.undo_callback()

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
            if 1 in event.buttons and keys.ALT in modifiers:
                self._handle_data_cleaned(p1, p2)
            if 2 in event.buttons and keys.ALT in modifiers:
                self._handle_data_accepted(p1, p2)
            if 3 in event.buttons and keys.ALT in modifiers:
                self._handle_data_undo()
            self._event_value = None  # Reset
            event.handled = True
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
            elif 2 in event.buttons and not modifiers:
                self._handle_zoom_event(d)
        else:
            event.handled = False


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
        self.twod_grid = None
        # self.axis_labels = None

        self._select_rect_color = 'white'
        self.is_3d = True

        self.scatter = None
        self.scatter_transform = None
        self.scatter_select_range = None

        self.idrange = {}
        self.idlookup = {}

        self.azimuth = None
        self.id = np.array([], dtype=object)
        self.head = np.array([], dtype=np.int8)
        self.x = np.array([], dtype=np.float64)
        self.y = np.array([], dtype=np.float64)
        self.z = np.array([], dtype=np.float32)
        self.rotx = np.array([], dtype=np.float64)
        self.roty = np.array([], dtype=np.float64)
        self.tvu = np.array([], dtype=np.float32)
        self.rejected = np.array([], dtype=np.int32)
        self.pointtime = np.array([], dtype=np.float64)
        self.beam = np.array([], dtype=np.int32)
        self.linename = np.array([], dtype=object)

        # statistics are populated on display_points
        self.x_offset = 0.0
        self.y_offset = 0.0
        self.z_offset = 0.0
        self.min_x = 0
        self.min_y = 0
        self.min_z = 0
        self.min_tvu = 0
        self.min_rejected = 0
        self.min_beam = 0
        self.max_x = 0
        self.max_y = 0
        self.max_z = 0
        self.max_tvu = 0
        self.max_rejected = 0
        self.max_beam = 0
        self.mean_x = 0
        self.mean_y = 0
        self.mean_z = 0
        self.mean_tvu = 0
        self.mean_rejected = 0
        self.mean_beam = 0
        self.unique_systems = []
        self.unique_linenames = []

        self.vertical_exaggeration = 1.0
        self.view_direction = 'north'
        self.show_axis = True
        self.show_rejected = True
        self.hide_lines = []

        self.displayed_points = None
        self.selected_points = None
        self.superselected_index = None

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.canvas.native)
        self.setLayout(layout)

        # Connect events just for handling the box display.  Otherwise the camera handles the translation/rotation/picking
        self.canvas.events.mouse_press.connect(self._on_mouse_press)
        self.canvas.events.mouse_release.connect(self._on_mouse_release)
        self.canvas.events.mouse_move.connect(self._on_mouse_move)

        # Set up for rectangle drawing
        self.line_pos = []
        self.line_origin = None
        self.line = scene.visuals.Line(color=self.select_rect_color, method='gl', parent=self.canvas.scene)
        self.line.visible = False  # set initially to invisible so it doesn't block the initial camera event

    @property
    def select_rect_color(self):
        return self._select_rect_color

    @select_rect_color.setter
    def select_rect_color(self, clr):
        self._select_rect_color = clr
        self.line._color = clr

    @property
    def is_empty(self):
        if not self.z.any():
            return True
        return False

    def _on_mouse_press(self, event):
        """
        Capture the mouse event before the camera gets it for point selection/cleaning to set the origin of the drawn
        selection box.
        """

        self.line_origin = event.pos

    def _on_mouse_release(self, event):
        """
        Capture the mouse event before the camera gets it for point selection/cleaning to clear the selection box
        """

        self.line_pos = []
        self.line_origin = None
        self.line.visible = False

    def _on_mouse_move(self, event):
        """
        Capture the mouse event before the camera gets it for point selection/cleaning to update the selection box
        """

        if self.line_origin is not None:
            modifiers = event.modifiers
            if keys.CONTROL in modifiers or keys.ALT in modifiers:  # these are the cleaning and selection mode buttons
                self.line.visible = True
                width = event.pos[0] - self.line_origin[0]
                height = event.pos[1] - self.line_origin[1]
                center = (width / 2. + self.line_origin[0], height / 2. + self.line_origin[1], 0)
                self.line_pos = rectangle_vertice(center, height, width)
                self.line.set_data(np.array(self.line_pos))

    def _select_points(self, startpos, endpos, three_d: bool = False):
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

    def _clean_points(self, startpos, endpos, three_d: bool = False):
        """
        Trigger the parent method to clean (reject) points data within the bounds provided by the two points

        Parameters
        ----------
        startpos
            Point where you first clicked
        endpos
            Point where you released the mouse button after dragging
        """

        if self.displayed_points is not None and self.parent is not None:
            self.parent.clean_points(startpos, endpos, three_d=three_d)

    def _accept_points(self, startpos, endpos, three_d: bool = False):
        """
        Trigger the parent method to accept (re-accept) points data within the bounds provided by the two points

        Parameters
        ----------
        startpos
            Point where you first clicked
        endpos
            Point where you released the mouse button after dragging
        """

        if self.displayed_points is not None and self.parent is not None:
            self.parent.accept_points(startpos, endpos, three_d=three_d)

    def _undo_clean(self):
        """
        Trigger the parent method to undo the last cleaning action
        """

        if self.displayed_points is not None and self.parent is not None:
            self.parent.undo_clean()

    def add_points(self, head: np.array, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array,
                   pointtime: np.array, beam: np.array, newid: str, linename: np.array, azimuth: float = None):
        """
        Add points to the 3d view widget, we only display points after all points are added, hence the separate methods

        Parameters
        ----------
        head
            head index of the sounding
        x
            easting
        y
            northing
        z
            depth value, positive down assumed
        tvu
            vertical uncertainty
        rejected
            The accepted/rejected state of each beam.  3 = reaccepted, 2 = rejected, 1 = phase detection, 0 = amplitude detection
        pointtime
            time of the sounding
        beam
            beam number of the sounding
        newid
            container name the sounding came from, ex: 'EM710_234_02_10_2019'
        linename
            1d array of line names for each time
        azimuth
            azimuth of the selection polygon in radians
        """

        self.azimuth = azimuth
        if azimuth:
            cos_az = np.cos(azimuth)
            sin_az = np.sin(azimuth)

            rotx = cos_az * x - sin_az * y
            roty = sin_az * x + cos_az * y
            self.rotx = np.concatenate([self.rotx, rotx])
            self.roty = np.concatenate([self.roty, roty])
        else:
            self.rotx = np.concatenate([self.rotx, x])
            self.roty = np.concatenate([self.roty, y])

        self.head = np.concatenate([self.head, head])

        # expand the identifier to be the size of the input arrays
        newid_array = np.full(head.shape[0], newid, dtype=object) + '_' + head.astype(str)
        self.id = np.concatenate([self.id, newid_array])
        uniqids = np.unique(newid_array)
        for unid in uniqids:
            headnum = int(unid[-1])
            self.idlookup[unid] = newid
            headwhere = np.where(head == headnum)[0]
            headstart, headend = headwhere[0], headwhere[-1] + 1
            self.idrange[unid] = [self.x.shape[0] + headstart, self.x.shape[0] + headend]

        self.x = np.concatenate([self.x, x])
        self.y = np.concatenate([self.y, y])
        self.z = np.concatenate([self.z, z])
        self.tvu = np.concatenate([self.tvu, tvu])
        self.rejected = np.concatenate([self.rejected, rejected])
        self.pointtime = np.concatenate([self.pointtime, pointtime])
        self.beam = np.concatenate([self.beam, beam])
        self.linename = np.concatenate([self.linename, linename])

    def remove_points(self, system_id: str = None):
        idrange = self.idrange.pop(system_id)
        idlkup = self.idlookup.pop(system_id)
        self.rotx = np.delete(self.rotx, slice(idrange[0], idrange[1]))
        self.roty = np.delete(self.roty, slice(idrange[0], idrange[1]))
        self.head = np.delete(self.head, slice(idrange[0], idrange[1]))
        self.id = np.delete(self.id, slice(idrange[0], idrange[1]))
        self.x = np.delete(self.x, slice(idrange[0], idrange[1]))
        self.y = np.delete(self.y, slice(idrange[0], idrange[1]))
        self.z = np.delete(self.z, slice(idrange[0], idrange[1]))
        self.tvu = np.delete(self.tvu, slice(idrange[0], idrange[1]))
        self.rejected = np.delete(self.rejected, slice(idrange[0], idrange[1]))
        self.pointtime = np.delete(self.pointtime, slice(idrange[0], idrange[1]))
        self.beam = np.delete(self.beam, slice(idrange[0], idrange[1]))
        self.linename = np.delete(self.linename, slice(idrange[0], idrange[1]))

    def return_points(self):
        """
        Return all the data in the 3dview
        """

        return [self.id, self.head, self.x, self.y, self.z, self.tvu, self.rejected, self.pointtime, self.beam, self.linename]

    def return_lines_and_times(self):
        """
        Return the unique line names and the associated time segments for each line in the points view.  This assumes that
        data is added by line, in other words, that the self.linename array is sorted.  This should always be true the
        way we add data to points view.

        Returns
        -------
        list
            list of the system name for each line that the line came from
        list
            list of the line names in the points view
        list
            list of lists for the start time/end time for the line in utc seconds
        """

        time_segments = []
        systems = []
        if self.linename.size > 0:
            lsort = self.linename.argsort()  # necessary for dual head sonar, lines are not in order with multiple heads
            linesort = self.linename[lsort]
            timesort = self.pointtime[lsort]
            idsort = self.id[lsort]
            linenames, linestarts = np.unique(linesort, return_index=True)
            lines_in_order = linestarts.argsort()
            linenames, linestarts = linenames[lines_in_order], linestarts[lines_in_order]
            for i in range(len(linenames) - 1):
                line_timesort = timesort[linestarts[i]:linestarts[i + 1] - 1]
                line_id = idsort[linestarts[i]:linestarts[i + 1] - 1]
                for usystem in np.unique(line_id):
                    tsegs = line_timesort[line_id == usystem]
                    time_segments.append([tsegs.min(), tsegs.max()])
                    systems.append(usystem)
            line_timesort = timesort[linestarts[-1]:]
            line_id = idsort[linestarts[-1]:]
            for usystem in np.unique(line_id):
                tsegs = line_timesort[line_id == usystem]
                time_segments.append([tsegs.min(), tsegs.max()])
                systems.append(usystem)
        else:
            linenames = []
        return systems, linenames, time_segments

    def _configure_2d_3d_view(self):
        """
        Due to differences in how the view is constructed when we switch back and forth between 2d and 3d views, we
        have to rebuild the view for each mode
        """

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
            if self.twod_grid:
                self.canvas.central_widget.remove_widget(self.twod_grid)
            self.twod_grid = None
            self.view = None
            self.view = self.canvas.central_widget.add_view()
            self.view.camera = TurntableCameraInteractive()
        else:
            self.view = None
            self.twod_grid = self.canvas.central_widget.add_grid(margin=10)
            self.twod_grid.spacing = 0
            self.view = self.twod_grid.add_view(row=1, col=1, border_color='white')
            self.view.camera = PanZoomInteractive()
        self.view.camera._bind_event(self._select_points, 'select')
        self.view.camera._bind_event(self._clean_points, 'clean')
        self.view.camera._bind_event(self._accept_points, 'accept')
        self.view.camera._bind_event(self._undo_clean, 'undo')

    def setup_axes(self):
        """
        Build the axes to match the scatter data loaded.  I use the axiswidget for 2d view, doesn't seem to work
        for 3d view.  I really like the ticks though.  I need something more sophisticated for 3d view.
        """

        if self.axis_x is not None:
            self.axis_x.parent = None
            self.axis_x = None
        if self.axis_y is not None:
            self.axis_y.parent = None
            self.axis_y = None
        if self.axis_z is not None:
            self.axis_z.parent = None
            self.axis_z = None

        if self.show_axis:
            if self.is_3d:  # just using arrows for now, nothing that cool
                if self.twod_grid:
                    self.canvas.central_widget.remove_widget(self.twod_grid)
                self.twod_grid = None
                diff_x = self.max_x - self.min_x
                self.axis_x = scene.visuals.Arrow(pos=np.array([[0, 0], [diff_x, 0]]), color='r', parent=self.view.scene,
                                                  arrows=np.array([[diff_x/50, 0, diff_x, 0], [diff_x/50, 0, diff_x, 0]]),
                                                  arrow_size=8, arrow_color='r', arrow_type='triangle_60')
                diff_y = self.max_y - self.min_y
                self.axis_y = scene.visuals.Arrow(pos=np.array([[0, 0], [0, diff_y]]), color='g', parent=self.view.scene,
                                                  arrows=np.array([[0, diff_y / 50, 0, diff_y], [0, diff_y / 50, 0, diff_y]]),
                                                  arrow_size=8, arrow_color='g', arrow_type='triangle_60')
                diff_z = (self.max_z - self.min_z) * self.vertical_exaggeration
                self.axis_z = scene.visuals.Arrow(pos=np.array([[0, 0, 0], [0, 0, diff_z]]), color='b', parent=self.view.scene,
                                                  arrows=np.array([[0, 0, diff_z/50, 0, 0, diff_z], [0, 0, diff_z/50, 0, 0, diff_z]]),
                                                  arrow_size=8, arrow_color='b', arrow_type='triangle_60')
            else:
                title = scene.Label(" ", color='white')
                title.height_max = 10
                self.twod_grid.add_widget(title, row=0, col=0, col_span=2)
                # if self.view_direction in ['north', 'right']:
                #     self.axis_x = ScaleAxisWidget(add_factor=self.x_offset, orientation='bottom', axis_font_size=12, axis_label_margin=50, tick_label_margin=18)
                # else:
                #     self.axis_x = ScaleAxisWidget(add_factor=self.y_offset, orientation='bottom', axis_font_size=12, axis_label_margin=50, tick_label_margin=18)
                self.axis_x = scene.AxisWidget(orientation='bottom', axis_font_size=12, axis_label_margin=50, tick_label_margin=18)
                self.axis_x.height_max = 80
                self.twod_grid.add_widget(self.axis_x, row=2, col=1)
                self.axis_z = ScaleAxisWidget(add_factor=-self.min_z, orientation='left', axis_font_size=12, axis_label_margin=50, tick_label_margin=5)
                self.axis_z.width_max = 80
                self.twod_grid.add_widget(self.axis_z, row=1, col=0)
                right_padding = self.twod_grid.add_widget(row=1, col=2, row_span=1)
                right_padding.width_max = 50
                self.axis_x.link_view(self.view)
                self.axis_z.link_view(self.view)

    def _build_color_by_soundings(self, color_by: str = 'depth', color_selected: bool = True):
        """
        Build out a RGBA value for each point based on the color_by argument.  We use the matplotlib colormap to
        return these values.  If you pick something like system or linename, we just return a mapped value for each
        unique entry.

        Parameters
        ----------
        color_by
            one of depth, vertical_uncertainty, beam, rejected, system, linename
        color_selected
            to color the user selected points or not

        Returns
        -------
        np.ndarray
            (N,4) array, where N is the number of points and the values are the RGBA values for each point
        matplotlib.colors.ColorMap
            cmap object that we use later to build the color bar
        float
            minimum value to use for the color bar
        float
            maximum value to use for the color bar
        """

        # normalize the arrays and build the colors for each sounding
        if self.is_empty:
            cmap = None
            clrs = np.array([], dtype=object)
            min_val = 0
            max_val = 0
        elif color_by == 'id':
            if len(self.z) + 1 > 2**32:
                raise NotImplementedError('Got more than 2^32 points, cant encode an ID as RGBA...')
            # color each sounding by a unique id encoded as RGBA
            ids = np.arange(1, len(self.z) + 1, dtype=np.uint32).view(np.uint8)
            ids = ids.reshape(-1, 4)
            clrs = np.divide(ids, 255, dtype=np.float32)
            cmap = None
            if color_selected:
                raise NotImplementedError('color_selected not allowed when coloring by ID, this is just for picking points...')
            min_val = 0
            max_val = 0
        elif color_by == 'depth':
            min_val = self.min_z
            max_val = self.max_z
            clrs, cmap = normalized_arr_to_rgb_v2((self.z - self.min_z) / (self.max_z - self.min_z), reverse=True)
        elif color_by == 'vertical_uncertainty':
            min_val = self.min_tvu
            max_val = self.max_tvu
            clrs, cmap = normalized_arr_to_rgb_v2((self.tvu - self.min_tvu) / (self.max_tvu - self.min_tvu))
        elif color_by == 'beam':
            min_val = self.min_beam
            max_val = self.max_beam
            clrs, cmap = normalized_arr_to_rgb_v2(self.beam / self.max_beam, band_count=self.max_beam)
        elif color_by == 'rejected':
            min_val = 0
            max_val = 3
            cmap = ListedColormap([kluster_variables.amplitude_color, kluster_variables.phase_color,
                                   kluster_variables.reject_color, kluster_variables.reaccept_color])
            clrs = cmap(self.rejected / 3)
        elif color_by in ['system', 'linename']:
            min_val = 0
            if color_by == 'system':
                vari = self.id
                uvari = self.unique_systems
            else:
                vari = self.linename
                uvari = self.unique_linenames
            sys_idx = np.zeros(vari.shape[0], dtype=np.int32)
            for cnt, us in enumerate(uvari):
                usidx = vari == us
                sys_idx[usidx] = cnt
            max_val = len(uvari)
            clrs, cmap = normalized_arr_to_rgb_v2((sys_idx / max_val), band_count=max_val)
        else:
            raise ValueError('Coloring by {} is not supported at this time'.format(color_by))

        if self.selected_points is not None and self.selected_points.any() and color_selected:
            msk = np.zeros(self.displayed_points.shape[0], dtype=bool)
            msk[self.selected_points] = True
            clrs[msk, :] = kluster_variables.selected_point_color
            if self.superselected_index is not None:
                msk[:] = False
                msk[self.selected_points[self.superselected_index]] = True
                clrs[msk, :] = kluster_variables.super_selected_point_color

        msk = self._build_display_mask()
        clrs = clrs[msk]

        return clrs, cmap, min_val, max_val

    def _build_scatter(self, clrs: np.ndarray):
        """
        Populate the scatter plot with data.  3d view gets the xyz, 2d view gets either xz or yz depending on view
        direction.  We center the camera on the mean value, and if this is the first time the camera is used (fresh
        camera) we automatically set the zoom level.

        Parameters
        ----------
        clrs
            (N,4) array, where N is the number of points and the values are the RGBA values for each point
        """

        msk = self._build_display_mask()
        if self.is_3d:
            self.scatter.set_data(self.displayed_points[msk], edge_color=clrs, face_color=clrs, symbol='o', size=3)
            if self.view.camera.fresh_camera:
                self.view.camera.center = (self.mean_x - self.x_offset, self.mean_y - self.y_offset, self.mean_z - self.min_z)
                self.view.camera.distance = (self.max_x - self.x_offset) * 2
                self.view.camera.fresh_camera = False
                self.view.camera.view_changed()
        else:
            if self.view_direction in ['north']:
                self.scatter.set_data(self.displayed_points[msk][:, [0, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
                self.view.camera.center = (self.mean_x - self.x_offset, self.mean_z - self.min_z)
                if self.view.camera.fresh_camera:
                    self.view.camera.zoom((self.max_x - self.x_offset) + 10)  # try and fit the swath in view on load
                    self.view.camera.fresh_camera = False
            elif self.view_direction in ['east', 'arrow']:
                self.scatter.set_data(self.displayed_points[msk][:, [1, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
                self.view.camera.center = (self.mean_y - self.y_offset, self.mean_z - self.min_z)
                if self.view.camera.fresh_camera:
                    self.view.camera.zoom((self.max_y - self.y_offset) + 10)  # try and fit the swath in view on load
                    self.view.camera.fresh_camera = False

    def _build_statistics(self):
        """
        Triggered on display_points.  After all the points are added (add_points called over and over for each new
        point source) we run display_points, which calls this method to build the statistics for each variable.

        These are used later for constructing colormaps and setting the camera.
        """

        if self.view_direction in ['north', 'east', 'top']:
            self.min_x = np.nanmin(self.x)
            self.min_y = np.nanmin(self.y)
            self.max_x = np.nanmax(self.x)
            self.max_y = np.nanmax(self.y)
            self.mean_x = np.nanmean(self.x)
            self.mean_y = np.nanmean(self.y)
        else:
            self.min_x = np.nanmin(self.rotx)
            self.min_y = np.nanmin(self.roty)
            self.max_x = np.nanmax(self.rotx)
            self.max_y = np.nanmax(self.roty)
            self.mean_x = np.nanmean(self.rotx)
            self.mean_y = np.nanmean(self.roty)

        self.min_z = np.nanmin(self.z)
        self.min_tvu = np.nanmin(self.tvu)
        self.min_rejected = np.nanmin(self.rejected)
        self.min_beam = np.nanmin(self.beam)

        self.max_z = np.nanmax(self.z)
        self.max_tvu = np.nanmax(self.tvu)
        self.max_rejected = np.nanmax(self.rejected)
        self.max_beam = np.nanmax(self.beam)

        self.mean_z = np.nanmean(self.z)
        self.mean_tvu = np.nanmean(self.tvu)
        self.mean_rejected = np.nanmean(self.rejected)
        self.mean_beam = np.nanmean(self.beam)

        self.unique_systems = np.unique(self.id).tolist()
        self.unique_linenames = np.unique(self.linename).tolist()

    def _build_display_mask(self):
        if not self.show_rejected:
            msk = self.rejected != kluster_variables.rejected_flag
        else:
            msk = np.ones_like(self.rejected, dtype=bool)
        if self.hide_lines:
            linemsk = ~np.isin(self.linename, self.hide_lines)
            msk = np.logical_and(msk, linemsk)
        return msk

    def display_points(self, color_by: str = 'depth', vertical_exaggeration: float = 1.0, view_direction: str = 'north',
                       show_axis: bool = True, show_rejected: bool = True):
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
        show_axis
            to build or not build the axis
        show_rejected
            to show or not to show the rejected soundings

        Returns
        -------
        matplotlib.colors.ColorMap
            cmap object that we use later to build the color bar
        float
            minimum value to use for the color bar
        float
            maximum value to use for the color bar
        """

        if self.is_empty:
            return None, None, None

        self._configure_2d_3d_view()
        self.view_direction = view_direction
        self.vertical_exaggeration = vertical_exaggeration
        self.show_axis = show_axis
        self.show_rejected = show_rejected

        self._build_statistics()
        # we need to subtract the min of our arrays.  There is a known issue with vispy (maybe in opengl in general) that large
        # values (like northings/eastings) cause floating point problems and the point positions jitter as you move
        # the camera (as successive redraw commands are run).  By zero centering and saving the offset, we can display
        # the centered northing/easting and rebuild the original value by adding the offset back in if we need to
        self.x_offset = self.min_x
        self.y_offset = self.min_y
        self.z_offset = self.min_z
        centered_z = self.z - self.z_offset

        if view_direction in ['north', 'east', 'top']:
            centered_x = self.x - self.x_offset
            centered_y = self.y - self.y_offset
        else:
            centered_x = self.rotx - self.x_offset
            centered_y = self.roty - self.y_offset

        # camera assumes z is positive up, flip the values
        if self.is_3d:
            centered_z = (centered_z - centered_z.max()) * -1 * vertical_exaggeration
        else:
            centered_z = centered_z * -1

        self.displayed_points = np.stack([centered_x, centered_y, centered_z], axis=1)
        clrs, cmap, minval, maxval = self._build_color_by_soundings(color_by)

        self.scatter = scene.visuals.Markers(parent=self.view.scene)
        # still need to figure this out.  Disabling depth test handles the whole plot-is-dark-from-one-angle,
        #   but you lose the intelligence it seems to have with depth of field of view, where stuff shows up in front
        #   of other stuff.  For now we just deal with the darkness issue, and leave depth_test=True (the default).
        if self.is_3d:
            self.scatter.set_gl_state(depth_test=True, blend=True, blend_func=('src_alpha', 'one_minus_src_alpha'))  # default
        else:  # two d we dont need to worry about the field of view, we can just show all as a blob
            self.scatter.set_gl_state(depth_test=False, blend=True, blend_func=('src_alpha', 'one_minus_src_alpha'))

        self._build_scatter(clrs)
        self.setup_axes()

        return cmap, minval, maxval

    def highlight_selected_scatter(self, color_by, color_selected=True):
        """
        A quick highlight method that circumvents the slower set_data.  Simply set the new colors and update the data.
        """

        clrs, cmap, minval, maxval = self._build_color_by_soundings(color_by, color_selected)
        if self.scatter is not None:
            msk = self._build_display_mask()
            if self.is_3d:
                self.scatter.set_data(self.displayed_points[msk], edge_color=clrs, face_color=clrs, symbol='o', size=3)
            else:
                if self.view_direction in ['north']:
                    self.scatter.set_data(self.displayed_points[msk][:, [0, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
                elif self.view_direction in ['east', 'arrow']:
                    self.scatter.set_data(self.displayed_points[msk][:, [1, 2]], edge_color=clrs, face_color=clrs, symbol='o', size=3)
        return cmap, minval, maxval

    def clear_display(self):
        """
        Have to clear the scatterplot each time we update the display, do so by setting the parent of the plot to None
        """
        if self.scatter is not None:
            # By setting the scatter visual parent to None, we delete it (clearing the widget)
            self.scatter.parent = None
            self.scatter = None
        if self.twod_grid:
            self.canvas.central_widget.remove_widget(self.twod_grid)
        self.twod_grid = None

    def clear(self):
        """
        Clear display and all stored data
        """
        self.clear_display()
        self.id = np.array([], dtype=object)
        self.head = np.array([], dtype=np.int8)
        self.x = np.array([], dtype=np.float64)
        self.y = np.array([], dtype=np.float64)
        self.z = np.array([], dtype=np.float32)
        self.rotx = np.array([], dtype=np.float64)
        self.roty = np.array([], dtype=np.float64)
        self.tvu = np.array([], dtype=np.float32)
        self.rejected = np.array([], dtype=np.int32)
        self.pointtime = np.array([], dtype=np.float64)
        self.beam = np.array([], dtype=np.int32)
        self.linename = np.array([], dtype=object)

        self.idrange = {}
        self.idlookup = {}
        self.hide_lines = []

        if self.axis_x is not None:
            self.axis_x.parent = None
            self.axis_x = None
        if self.axis_y is not None:
            self.axis_y.parent = None
            self.axis_y = None
        if self.axis_z is not None:
            self.axis_z.parent = None
            self.axis_z = None
        if self.twod_grid:
            self.canvas.central_widget.remove_widget(self.twod_grid)
        self.twod_grid = None


class ThreeDWidget(QtWidgets.QWidget):
    """
    Widget containing the OptionsWidget (left pane) and VesselView (right pane).  Manages the signals that connect the
    two widgets.
    """

    points_selected = Signal(object, object, object, object, object, object, object, object, object, object)
    points_cleaned = Signal(object)
    patch_test_sig = Signal(bool)

    def __init__(self, parent=None, settings=None):
        super().__init__(parent)

        self.external_settings = settings
        self.widgetname = '3dview'
        self.appname = 'Kluster'

        self.three_d_window = ThreeDView(self)

        self.mainlayout = QtWidgets.QVBoxLayout()

        self.opts_layout = QtWidgets.QHBoxLayout()
        self.dimension = QtWidgets.QComboBox()
        self.dimension.addItems(['2d view', '3d view'])
        self.dimension.setToolTip('Change the view to either 2 or 3 dimensions.')
        self.opts_layout.addWidget(self.dimension)
        self.colorby_label = QtWidgets.QLabel('Color: ')
        self.opts_layout.addWidget(self.colorby_label)
        self.colorby = QtWidgets.QComboBox()
        self.colorby.addItems(['depth', 'vertical_uncertainty', 'beam', 'rejected', 'system', 'linename'])
        self.colorby.setToolTip('Attribute used to color the soundings, see the colorbar for values')
        self.opts_layout.addWidget(self.colorby)
        self.viewdirection_label = QtWidgets.QLabel('View: ')
        self.opts_layout.addWidget(self.viewdirection_label)
        self.viewdirection2d = QtWidgets.QComboBox()
        self.viewdirection2d.addItems(['north', 'east', 'arrow'])
        self.viewdirection2d.setToolTip('View direction shown in the Points View:\n\n' +
                                        'north - this will show the eastings (x) vs depth\n' +
                                        'east - this will show the northings (y) vs depth\n' +
                                        'arrow - this will show the rotated soundings looking down the direction shown as the arrow of the box select tool in 2dview')
        self.opts_layout.addWidget(self.viewdirection2d)
        self.viewdirection3d = QtWidgets.QComboBox()
        self.viewdirection3d.addItems(['top', 'arrow'])
        self.viewdirection3d.setToolTip('View direction shown in the Points View:\n\n' +
                                        'top - this will show the unrotated soundings (soundings as is)\n' +
                                        'arrow - this will show the rotated soundings in a top view, using the direction shown as the arrow of the box select tool in 2dview')
        self.viewdirection3d.hide()
        self.opts_layout.addWidget(self.viewdirection3d)

        self.vertexag_label = QtWidgets.QLabel('Exaggeration: ')
        self.opts_layout.addWidget(self.vertexag_label)
        self.vertexag = QtWidgets.QDoubleSpinBox()
        self.vertexag.setMaximum(99.0)
        self.vertexag.setMinimum(1.0)
        self.vertexag.setSingleStep(0.5)
        self.vertexag.setValue(1.0)
        self.vertexag.setToolTip('Multiplier used for the depth values, displayed z is multiplied by this number')
        self.opts_layout.addWidget(self.vertexag)
        self.show_axis = QtWidgets.QCheckBox('Axis')
        self.show_axis.setChecked(True)
        self.opts_layout.addWidget(self.show_axis)
        self.show_colorbar = QtWidgets.QCheckBox('Colorbar')
        self.show_colorbar.setChecked(True)
        self.show_colorbar.setToolTip('Uncheck to hide the colorbar, check to show the colorbar')
        self.opts_layout.addWidget(self.show_colorbar)
        self.show_rejected = QtWidgets.QCheckBox('Rejected')
        self.show_rejected.setChecked(True)
        self.show_rejected.setToolTip('Check this box to show all soundings that have been rejected.')
        self.opts_layout.addWidget(self.show_rejected)
        self.opts_layout.addStretch()

        self.second_opts_layout = QtWidgets.QHBoxLayout()
        self.hide_lines_btn = QtWidgets.QPushButton('Show Lines')
        self.hide_lines_btn.setToolTip('Select the lines you want to show in Points View.')
        self.second_opts_layout.addWidget(self.hide_lines_btn)
        self.patch_button = QtWidgets.QPushButton('Patch Test')
        self.patch_button.setToolTip('Run the Patch Test on the data currently in Points View.')
        self.second_opts_layout.addWidget(self.patch_button)
        self.second_opts_layout.addStretch()

        self.colorbar = ColorBar()

        self.viewlayout = QtWidgets.QHBoxLayout()
        self.viewlayout.addWidget(self.three_d_window)
        self.viewlayout.addWidget(self.colorbar)
        self.viewlayout.setStretchFactor(self.three_d_window, 6)
        self.viewlayout.setStretchFactor(self.colorbar, 1)

        self.mainlayout.addLayout(self.opts_layout)
        self.mainlayout.addLayout(self.second_opts_layout)
        self.mainlayout.addLayout(self.viewlayout)

        instruct = 'You can interact with Points View using the following keyboard/mouse shortcuts:\n\n'
        instruct += 'Left Mouse Button: Hold down and move to rotate the camera\n'
        instruct += 'Right Mouse Button: Hold down and move to zoom the camera\n'
        instruct += 'Mouse Wheel: Wheel in/out to zoom the camera\n'
        instruct += '(3D ONLY) Shift + Left Mouse Button: Move/Translate the camera center location\n'
        instruct += 'Ctrl + Left Mouse Button: Query points (see Explorer window)\n'
        instruct += 'Alt + Left Mouse Button: Clean points (mark as Rejected, see Color: Rejected)\n'
        instruct += 'Alt + Right Mouse Button: Accept points (mark as Accepted, see Color: Rejected)\n'
        instruct += 'Alt + Middle Mouse Button: Undo last cleaning operation'

        self.instructions = QtWidgets.QLabel('Mouse over for Instructions')
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic }")
        self.instructions.setWordWrap(True)
        self.instructions.setToolTip(instruct)
        self.mainlayout.addWidget(self.instructions)

        self.mainlayout.setStretchFactor(self.viewlayout, 1)
        self.mainlayout.setStretchFactor(self.instructions, 0)
        self.setLayout(self.mainlayout)

        self.dimension.currentTextChanged.connect(self._handle_dimension_change)
        self.colorby.currentTextChanged.connect(self.change_color_by)
        self.vertexag.valueChanged.connect(self.refresh_settings)
        self.show_axis.stateChanged.connect(self.refresh_settings)
        self.show_colorbar.stateChanged.connect(self._event_show_colorbar)
        self.show_rejected.stateChanged.connect(self.refresh_settings)
        self.patch_button.clicked.connect(self._event_patch_test)
        self.hide_lines_btn.clicked.connect(self._event_hide_lines)

        self.is_3d = None
        self.patch_test_running = False
        self.last_change_buffer = []
        self.text_controls = [['dimension', self.dimension], ['colorby', self.colorby],
                              ['viewdirection2d', self.viewdirection2d],
                              ['viewdirection3d', self.viewdirection3d], ['vertexag', self.vertexag]]
        self.checkbox_controls = [['show_colorbar', self.show_colorbar], ['show_rejected', self.show_rejected]]
        self._select_rect_color = 'black'
        self._cached_view_settings = None

        self._handle_dimension_change()
        self.viewdirection2d.currentTextChanged.connect(self.refresh_settings)
        self.viewdirection3d.currentTextChanged.connect(self.refresh_settings)
        self.read_settings()

    @property
    def azimuth(self):
        return self.three_d_window.azimuth

    @property
    def settings_object(self):
        if self.external_settings:
            return self.external_settings
        else:
            return QtCore.QSettings("NOAA", self.appname)

    @property
    def select_rect_color(self):
        return self._select_rect_color

    @select_rect_color.setter
    def select_rect_color(self, clr):
        self._select_rect_color = clr
        self.three_d_window.select_rect_color = clr

    def save_settings(self):
        """
        Save the settings to the Qsettings registry
        """
        settings = self.settings_object
        if self.text_controls:
            for cname, tcntrl in self.text_controls:
                try:
                    settings.setValue('{}/{}_{}'.format(self.appname, self.widgetname, cname), tcntrl.currentText())
                except:
                    settings.setValue('{}/{}_{}'.format(self.appname, self.widgetname, cname), tcntrl.text())
        if self.checkbox_controls:
            for cname, ccntrl in self.checkbox_controls:
                settings.setValue('{}/{}_{}'.format(self.appname, self.widgetname, cname), ccntrl.isChecked())
        settings.sync()

    def read_settings(self):
        """
        Read from the Qsettings registry
        """
        settings = self.settings_object
        try:
            if self.text_controls:
                for cname, tcntrl in self.text_controls:
                    base_value = settings.value('{}/{}_{}'.format(self.appname, self.widgetname, cname))
                    if base_value is None:
                        base_value = ''
                    text_value = str(base_value)
                    if text_value:
                        try:
                            tcntrl.setCurrentText(text_value)
                        except:
                            try:
                                tcntrl.setText(text_value)
                            except:
                                tcntrl.setValue(float(text_value))
            if self.checkbox_controls:
                for cname, ccntrl in self.checkbox_controls:
                    check_value = settings.value('{}/{}_{}'.format(self.appname, self.widgetname, cname))
                    try:
                        ccntrl.setChecked(check_value.lower() == 'true')
                    except AttributeError:
                        try:
                            ccntrl.setChecked(check_value)
                        except:
                            pass
        except TypeError:
            # no settings exist yet for this app
            pass

    def _event_show_colorbar(self, e):
        """
        On checking the show colorbar, we show/hide the colorbar
        """
        show_colorbar = self.show_colorbar.isChecked()
        if show_colorbar:
            self.colorbar.show()
        else:
            self.colorbar.hide()

    def _event_patch_test(self, e):
        self.patch_test_running = True
        self.patch_test_sig.emit(True)

    def _event_hide_lines(self):
        dlog = HideLinesDialog()
        dlog.initial_hide_lines = self.three_d_window.hide_lines
        dlog.add_lines(self.three_d_window.unique_linenames)
        if dlog.exec_():
            if dlog.canceled:
                pass
            else:
                hidelines = dlog.notselected_rows
                self.three_d_window.hide_lines = hidelines
                self.refresh_settings(None)

    def _init_pointsview(self):
        self.patch_test_running = False
        self.last_change_buffer = []
        self.three_d_window.hide_lines = []
        self.three_d_window.selected_points = None
        self.three_d_window.superselected_index = None

    def add_points(self, head: np.array, x: np.array, y: np.array, z: np.array, tvu: np.array, rejected: np.array, pointtime: np.array,
                   beam: np.array, newid: str, linename: np.array, azimuth: float = None):
        """
        Adding new points will update the three d window with the boints and set the controls to show/hide
        """
        self._init_pointsview()
        self.three_d_window.add_points(head, x, y, z, tvu, rejected, pointtime, beam, newid, linename, azimuth=azimuth)

    def remove_points(self, system_id: str = None):
        self._init_pointsview()
        self.three_d_window.remove_points(system_id=system_id)

    def return_points(self):
        return self.three_d_window.return_points()

    def _handle_point_selection(self, startpos, endpos, three_d: bool = False):
        linemsk = None
        if self.three_d_window.hide_lines:
            linemsk = np.isin(self.three_d_window.linename, self.three_d_window.hide_lines)
        if three_d:
            # color the points by a unique rgba value, render with the new color and pull the id of the point by its color
            #  this is a workaround for the 3d camera transforms not working.  See https://github.com/vispy/vispy/issues/1336
            startpos_canvas = self.three_d_window.canvas.transforms.canvas_transform.map(startpos)
            endpos_canvas = self.three_d_window.canvas.transforms.canvas_transform.map(endpos)
            points_in_screen = np.zeros_like(self.three_d_window.displayed_points[:, 0], dtype=bool)
            self.three_d_window.scatter.update_gl_state(blend=False)
            self.three_d_window.scatter.antialias = 0
            self.three_d_window.highlight_selected_scatter('id', False)
            minx, miny = min(startpos_canvas[0], endpos_canvas[0]), min(startpos_canvas[1], endpos_canvas[1])
            window_width, window_height = max(abs(startpos_canvas[0] - endpos_canvas[0]), 1), max(abs(startpos_canvas[1] - endpos_canvas[1]), 1)
            img = self.three_d_window.canvas.render((int(minx), int(miny), int(window_width), int(window_height)), bgcolor=(0, 0, 0, 0))
            idxs = img.ravel().view(np.uint32)
            if idxs.any():
                idxs = np.unique(idxs)
                idx = idxs[idxs != 0]
                if idx.any():
                    # subtract one; color 0 was reserved for the background
                    idx = idx - 1
                    # filter out the out of bounds indices
                    idx = idx[np.logical_and(idx > 0, idx < points_in_screen.shape[0])]
                    points_in_screen[idx] = True
            self.three_d_window.scatter.update_gl_state(blend=True)
            self.three_d_window.scatter.antialias = 1
        else:
            vd = self.viewdirection2d.currentText()
            if vd in ['north']:
                m1 = self.three_d_window.displayed_points[:, [0, 2]] >= startpos[0:2]
                m2 = self.three_d_window.displayed_points[:, [0, 2]] <= endpos[0:2]
            elif vd in ['east', 'arrow']:
                m1 = self.three_d_window.displayed_points[:, [1, 2]] >= startpos[0:2]
                m2 = self.three_d_window.displayed_points[:, [1, 2]] <= endpos[0:2]
            else:
                raise NotImplementedError('View direction not one of north, east, arrow: {}'.format(vd))
            points_in_screen = m1[:, 0] & m1[:, 1] & m2[:, 0] & m2[:, 1]
        if linemsk is not None:
            points_in_screen[linemsk] = False
        points_in_screen = np.argwhere(points_in_screen)[:, 0]
        return points_in_screen

    def select_points(self, startpos, endpos, three_d: bool = False):
        """
        Triggers when the user CTRL+Mouse selects data in the 3dview.  We set the selected points and let the view know
        to highlight those points and set the attributes in the Kluster explorer widget.
        """

        points_in_screen = self._handle_point_selection(startpos, endpos, three_d)
        self.three_d_window.selected_points = points_in_screen
        self.three_d_window.superselected_index = None
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
        self.three_d_window.highlight_selected_scatter(self.colorby.currentText())

    def clear_selection(self):
        self.three_d_window.selected_points = None

    def clean_points(self, startpos, endpos, three_d: bool = False):
        """
        Triggers when the user ALT+Mouse1 selects data in the 3dview.  We set the selected points and let the widget know to reject these points.
        """

        if not self.patch_test_running:
            points_in_screen = self._handle_point_selection(startpos, endpos, three_d)
            self.three_d_window.selected_points = points_in_screen
            self.last_change_buffer.append([self.three_d_window.selected_points, self.three_d_window.rejected[self.three_d_window.selected_points]])
            self.three_d_window.rejected[self.three_d_window.selected_points] = kluster_variables.rejected_flag
            self.points_cleaned.emit(kluster_variables.rejected_flag)
            self.three_d_window.highlight_selected_scatter(self.colorby.currentText(), False)

            if len(self.last_change_buffer) > kluster_variables.last_change_buffer_size:
                print('WARNING: Points view will only retain the last {} cleaning actions for undo'.format(kluster_variables.last_change_buffer_size))
                self.last_change_buffer.pop(0)
        else:
            print('Point cloud cleaning disabled while patch test is running')

    def override_sounding_status(self, new_status: np.ndarray):
        if not self.patch_test_running:
            try:
                assert new_status.size == self.three_d_window.rejected.size
            except AssertionError:
                print(f'override_sounding_status: unable to override Points View rejected with new array, size does not match (new size {new_status.size} != {self.three_d_window.rejected.size}')
            self.three_d_window.selected_points = np.ones(self.three_d_window.rejected.shape[0], dtype=bool)
            self.last_change_buffer.append([self.three_d_window.selected_points, self.three_d_window.rejected.copy()])
            self.three_d_window.rejected[self.three_d_window.selected_points] = new_status
            self.three_d_window.selected_points = None
            self.three_d_window.highlight_selected_scatter(self.colorby.currentText(), False)
            if len(self.last_change_buffer) > kluster_variables.last_change_buffer_size:
                print('WARNING: Points view will only retain the last {} cleaning actions for undo'.format(
                    kluster_variables.last_change_buffer_size))
                self.last_change_buffer.pop(0)
        else:
            print('Point cloud cleaning disabled while patch test is running')

    def accept_points(self, startpos, endpos, three_d: bool = False):
        """
        Triggers when the user ALT+Mouse2 selects data in the 3dview.  We set the selected points and let the widget know to accept these points.
        """

        points_in_screen = self._handle_point_selection(startpos, endpos, three_d)
        self.three_d_window.selected_points = points_in_screen
        is_rejected = self.three_d_window.rejected[self.three_d_window.selected_points] == kluster_variables.rejected_flag
        if is_rejected.any():
            self.last_change_buffer.append([self.three_d_window.selected_points, self.three_d_window.rejected[self.three_d_window.selected_points]])
            self.three_d_window.rejected[self.three_d_window.selected_points[is_rejected]] = kluster_variables.accepted_flag
            self.points_cleaned.emit(kluster_variables.accepted_flag)
            if len(self.last_change_buffer) > kluster_variables.last_change_buffer_size:
                print('WARNING: Points view will only retain the last {} cleaning actions for undo'.format(kluster_variables.last_change_buffer_size))
                self.last_change_buffer.pop(0)
        self.three_d_window.highlight_selected_scatter(self.colorby.currentText(), False)

    def undo_clean(self):
        if self.last_change_buffer:
            last_select, last_status = self.last_change_buffer.pop(-1)
            self.three_d_window.selected_points = last_select
            self.three_d_window.rejected[self.three_d_window.selected_points] = last_status
            self.points_cleaned.emit(last_status)
            self.three_d_window.highlight_selected_scatter(self.colorby.currentText())
        else:
            print('Points View: No changes to undo')

    def return_array(self, arr_name: str):
        idx = {}
        select_id = self.three_d_window.id
        selarray = self.three_d_window.__getattribute__(arr_name)
        uniq_ids = np.unique(select_id)
        source_ids = [self.three_d_window.idlookup[uqid] for uqid in uniq_ids]
        for uid, sid in zip(uniq_ids, source_ids):
            headnum = int(uid[-1])
            uid_filter = np.where(select_id == uid)[0]
            selarray_filtered = selarray[uid_filter]
            idx_key = self.three_d_window.idlookup[uid]
            if idx_key not in idx:
                idx[idx_key] = selarray_filtered
            else:
                idx[idx_key] = np.concatenate([idx[idx_key], selarray_filtered])
        return idx

    def split_by_selected(self, selarray: np.array):
        """
        Takes an array of the same size as the selected index and returns that array split by system/head indexes

        Will be a dictionary of {container name: [head0_select_index, head1_select_index...]}

        Returns
        -------
        dict
            dictionary of {container name: [head0_select_index, head1_select_index...]}
        """

        idx = {}
        if self.three_d_window.selected_points is not None:
            select_id = self.three_d_window.id[self.three_d_window.selected_points]
            uniq_ids = np.unique(select_id)
            source_ids = [self.three_d_window.idlookup[uqid] for uqid in uniq_ids]
            for uid, sid in zip(uniq_ids, source_ids):
                headnum = int(uid[-1])
                uid_filter = np.where(select_id == uid)[0]
                selarray_filtered = selarray[uid_filter]
                idx_key = self.three_d_window.idlookup[uid]
                if idx_key not in idx:
                    if headnum == 0:
                        idx[idx_key] = [selarray_filtered]
                    else:
                        idx[idx_key] = []
                        for i in range(headnum):
                            idx[idx_key].append([])
                        idx[idx_key].append(selarray_filtered)
                else:
                    if len(idx[idx_key]) == headnum:
                        idx[idx_key].append(selarray_filtered)
                    else:
                        for i in range(headnum - len(idx[idx_key])):
                            idx[idx_key].append([])
        return idx

    def return_fqpr_paths(self):
        fqprs = [pth[:-2] for pth in self.three_d_window.idlookup.keys()]
        return fqprs

    def return_lines_and_times(self):
        return self.three_d_window.return_lines_and_times()

    def return_select_index(self):
        """
        Returns the selected point index as a raveled index that can be used with fqpr_generation.subset.ping_filter

        Will be a dictionary of {container name: [head0_select_index, head1_select_index...]}

        You can use these head select indexes to index the ping_filter record to get the index of the original record
        in the fqpr object.  This is used with selecting and attributing points in the points view as 'rejected' by
        saving a new rejected flag for the selected points to the original data on disk.

        Returns
        -------
        dict
            dictionary of {container name: [head0_select_index, head1_select_index...]}
        """

        idx = {}
        if self.three_d_window.selected_points is not None:
            select_id = self.three_d_window.id[self.three_d_window.selected_points]
            uniq_ids = np.unique(select_id)
            source_ids = [self.three_d_window.idlookup[uqid] for uqid in uniq_ids]
            for uid, sid in zip(uniq_ids, source_ids):
                headnum = int(uid[-1])
                data_start, data_end = self.three_d_window.idrange[uid]
                uid_filter = np.where(select_id == uid)[0]
                select_filtered = self.three_d_window.selected_points[uid_filter]
                dat = select_filtered - data_start
                idx_key = self.three_d_window.idlookup[uid]
                if idx_key not in idx:
                    if headnum == 0:
                        idx[idx_key] = [dat]
                    else:
                        idx[idx_key] = []
                        for i in range(headnum):
                            idx[idx_key].append([])
                        idx[idx_key].append(dat)
                else:
                    if len(idx[idx_key]) == headnum:
                        idx[idx_key].append(dat)
                    else:
                        for i in range(headnum - len(idx[idx_key])):
                            idx[idx_key].append([])
        return idx

    def superselect_point(self, superselect_index):
        """
        Clicking on a row in Kluster explorer tells the 3dview to super-select that point, highlighting it white
        in the 3d view
        """

        self.three_d_window.superselected_index = superselect_index
        self.three_d_window.highlight_selected_scatter(self.colorby.currentText())

    def change_colormap(self, cmap, minval, maxval):
        """
        on adding new points or changing the colorby, we update the colorbar
        """

        if self.colorby.currentText() == 'rejected':
            self.colorbar.setup_colorbar(cmap, minval, maxval, is_rejected=True)
        elif self.colorby.currentText() == 'system':
            self.colorbar.setup_colorbar(cmap, minval, maxval,
                                         by_name=self.three_d_window.unique_systems)
        elif self.colorby.currentText() == 'linename':
            self.colorbar.setup_colorbar(cmap, minval, maxval,
                                         by_name=self.three_d_window.unique_linenames)
        else:
            inverty = False
            if self.colorby.currentText() == 'depth':
                inverty = True
            self.colorbar.setup_colorbar(cmap, minval, maxval, invert_y=inverty)

    def change_color_by(self, e):
        """
        Triggers on a new dropdown in colorby
        """
        cmap, minval, maxval = self.three_d_window.highlight_selected_scatter(self.colorby.currentText())
        if cmap is not None:
            self.change_colormap(cmap, minval, maxval)

    def display_points(self):
        """
        After adding points, we trigger the display by running display_points
        """
        if self.dimension.currentText() == '2d view':
            vd = self.viewdirection2d.currentText()
        else:
            vd = self.viewdirection3d.currentText()
        showaxis = True  # freezes when hiding axes on 3d for some reason
        showrejected = self.show_rejected.isChecked()
        if self.vertexag.isHidden():
            vertexag = 1
        else:
            vertexag = self.vertexag.value()
        cmap, minval, maxval = self.three_d_window.display_points(color_by=self.colorby.currentText(),
                                                                  vertical_exaggeration=vertexag, view_direction=vd,
                                                                  show_axis=showaxis, show_rejected=showrejected)
        if cmap is not None:
            self.change_colormap(cmap, minval, maxval)

    def _handle_dimension_change(self):
        if self.dimension.currentText() == '2d view':
            is_3d = False
        else:
            is_3d = True

        if is_3d:
            self.viewdirection3d.show()
            self.viewdirection2d.hide()
            self.show_axis.hide()
            self.vertexag_label.show()
            self.vertexag.show()
        else:
            self.viewdirection3d.hide()
            self.viewdirection2d.show()
            self.show_axis.hide()
            self.vertexag_label.hide()
            self.vertexag.hide()
        self.three_d_window.is_3d = is_3d
        self.refresh_settings(None)

    def refresh_settings(self, e):
        """
        After any substantial change to the point data or scale, we clear and redraw the points
        """

        self.store_view_settings()
        self.clear_display()
        if self.three_d_window.x.any():
            self.display_points()
        self.load_view_settings()

    def store_view_settings(self):
        self._cached_view_settings = [self.dimension.currentText(), self.three_d_window.view.camera.get_state()]

    def load_view_settings(self):
        if self._cached_view_settings is not None:
            dimname, camstate = self._cached_view_settings
            try:
                self.three_d_window.view.camera.set_state(camstate)
            except:
                pass
        else:
            print('Points View: Unable to load state, no saved state')

    def clear_display(self):
        self.three_d_window.clear_display()

    def clear(self):
        self.three_d_window.clear()


class HideLinesDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Show Lines')
        self.setMinimumWidth(400)
        self.setMinimumHeight(400)

        self.main_layout = QtWidgets.QVBoxLayout()

        self.instructions_layout = QtWidgets.QVBoxLayout()
        self.instructions = QtWidgets.QLabel('Select the Lines you want to show.')
        self.instructions_layout.addWidget(self.instructions)
        self.main_layout.addLayout(self.instructions_layout)

        self.helperbutton_layout = QtWidgets.QHBoxLayout()
        self.checkall_button = QtWidgets.QPushButton('Check All', self)
        self.helperbutton_layout.addWidget(self.checkall_button)
        self.uncheckall_button = QtWidgets.QPushButton('Uncheck All', self)
        self.helperbutton_layout.addWidget(self.uncheckall_button)
        self.helperbutton_layout.addStretch()
        self.main_layout.addLayout(self.helperbutton_layout)

        self.line_list = LineList(self)
        self.main_layout.addWidget(self.line_list)

        self.hlayout_msg = QtWidgets.QHBoxLayout()
        self.warning_message = QtWidgets.QLabel('', self)
        self.warning_message.setStyleSheet("color : {};".format(kluster_variables.error_color))
        self.hlayout_msg.addWidget(self.warning_message)
        self.main_layout.addLayout(self.hlayout_msg)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('Load', self)
        self.button_layout.addWidget(self.ok_button)
        self.button_layout.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addStretch(1)
        self.main_layout.addLayout(self.button_layout)

        self.setLayout(self.main_layout)
        self.canceled = False
        self.initial_hide_lines = []
        self.notselected_rows = []

        self.ok_button.clicked.connect(self.return_hidden)
        self.cancel_button.clicked.connect(self.cancel_hide)
        self.checkall_button.clicked.connect(self.checkall)
        self.uncheckall_button.clicked.connect(self.uncheckall)

    def err_message(self, text: str = ''):
        if text:
            self.warning_message.setText('ERROR: ' + text)
        else:
            self.warning_message.setText('')

    def add_lines(self, lines: list):
        self.line_list.add_lines(lines, self.initial_hide_lines)

    def return_hidden(self):
        self.canceled = False
        notselected_rows, err, msg = self._get_selected_data()
        if err:
            self.err_message(msg)
            self.notselected_rows = []
        else:
            self.notselected_rows = notselected_rows
            self.accept()

    def _get_selected_data(self):
        notselected_rows = []
        for row in range(self.line_list.rowCount()):
            notchkd = not self.line_list.cellWidget(row, 0).isChecked()
            if notchkd:
                notselected_rows.append(self.line_list.item(row, 1).text())
        err = False
        msg = ''
        if len(notselected_rows) == self.line_list.rowCount():
            msg = 'Must show at least one line!'
        if msg:
            err = True
        return notselected_rows, err, msg

    def checkall(self):
        for row in range(self.line_list.rowCount()):
            self.line_list.cellWidget(row, 0).setChecked(True)

    def uncheckall(self):
        for row in range(self.line_list.rowCount()):
            self.line_list.cellWidget(row, 0).setChecked(False)

    def cancel_hide(self):
        self.canceled = True
        self.accept()


class LineList(QtWidgets.QTableWidget):
    def __init__(self, parent):
        super().__init__(parent)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.headr = ['', 'Line']
        self.setColumnCount(2)
        self.setHorizontalHeaderLabels(self.headr)
        self.setColumnWidth(0, 20)
        self.setColumnWidth(1, 320)
        self.row_full_attribution = []

    def setup_table(self):
        self.clearContents()
        self.setRowCount(0)
        self.row_full_attribution = []

    def add_lines(self, line_data: list, initial_hide_lines: list):
        if line_data:
            for line in line_data:
                next_row = self.rowCount()
                self.insertRow(next_row)
                self.row_full_attribution.append(line)
                for column_index, _ in enumerate(self.headr):
                    if column_index == 0:
                        item = QtWidgets.QCheckBox()
                        if str(line) not in initial_hide_lines:
                            item.setChecked(True)
                        self.setCellWidget(next_row, column_index, item)
                    else:
                        item = QtWidgets.QTableWidgetItem(str(line))
                        self.setItem(next_row, column_index, item)


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
    matplotlib.colors.ColorMap
        cmap object that we use later to build the color bar
    """
    if reverse:
        cmap = cm.get_cmap(colormap + '_r', band_count)
    else:
        cmap = cm.get_cmap(colormap, band_count)
    return cmap(z_array_normalized), cmap


if __name__ == '__main__':
    if qgis_enabled:
        app = qgis_core.QgsApplication([], True)
        app.initQgis()
    else:
        try:  # pyside2
            app = QtWidgets.QApplication()
        except TypeError:  # pyqt5
            app = QtWidgets.QApplication([])

    win = ThreeDWidget()
    data = np.random.randn(10000, 3)
    tvu = np.random.rand(data.shape[0])
    rejected = np.random.randint(0, 3, size=data.shape[0])
    pointtime = np.arange(data.shape[0])
    beam = np.random.randint(0, 399, size=data.shape[0])
    linename = np.full(data.shape[0], '')
    linename[:5000] = 'a'
    linename[5000:] = 'b'
    head = np.full(data.shape[0], 0)
    newid = 'test'
    win.add_points(head, data[:, 0], data[:, 1], data[:, 2], tvu, rejected, pointtime, beam, newid, linename)
    win.display_points()
    win.show()
    app.exec_()
