import numpy as np

from bathygrid.convenience import create_grid


class PatchTest:
    """

    """

    def __init__(self, fqpr, azimuth: float = None, initial_xyzrph: dict = None):
        self.fqpr = fqpr
        self.azimuth = azimuth
        self.initial_xyzrph = initial_xyzrph

        self.points = None
        self.min_x = None
        self.min_y = None
        self.grid = None
        self.a_matrix = None

        self._generate_rotated_points()
        self._grid()

        print('here')

    def _generate_rotated_points(self):
        ang = self.azimuth - 90  # rotations are counter clockwise, we want it eventually facing east
        cos_az = np.cos(np.deg2rad(ang))
        sin_az = np.sin(np.deg2rad(ang))
        finalx = None
        finaly = None
        finalz = None
        for rp in self.fqpr.multibeam.raw_ping:
            x, y, z = rp.x.values.ravel(), rp.y.values.ravel(), rp.z.values.ravel()
            if finalx is None:
                finalx = x
                finaly = y
                finalz = z
            else:
                finalx = np.concatenate([finalx, x])
                finaly = np.concatenate([finaly, y])
                finalz = np.concatenate([finalz, z])
        dtyp = [('x', np.float64), ('y', np.float64), ('z', np.float32)]
        self.points = np.empty(len(finalx), dtype=dtyp)
        self.points['x'] = finalx
        self.points['y'] = finaly
        self.points['z'] = finalz

        # calculate center of rotation
        self.min_x = self.points['x'].min()
        self.min_y = self.points['y'].min()
        centered_x = self.points['x'] - self.min_x
        centered_y = self.points['y'] - self.min_y
        # rotate
        self.points['x'] = self.min_x + cos_az * centered_x - sin_az * centered_y
        self.points['y'] = self.min_y + sin_az * centered_x + cos_az * centered_y

    def _grid(self):
        grid_class = create_grid(grid_type='single_resolution')
        grid_class.add_points(self.points, 'patch', ['line1', 'line2'])
        grid_class.grid(progress_bar=False)
        self.grid = grid_class

    def _build_patch_test_values(self):
        dpth, xslope, yslope = self.grid.get_layers_by_name(['depth', 'x_slope', 'y_slope'])
        valid_index = ~np.isnan(dpth)
        xval = np.arange(self.grid.min_x, self.grid.max_x, self.grid.resolutions[0])
        yval = np.arange(self.grid.min_y, self.grid.max_y, self.grid.resolutions[0])
        grid_rez = self.grid.resolutions[0]
        x_node_locs, y_node_locs = np.meshgrid(xval + grid_rez / 2, yval + grid_rez / 2, copy=False)

        dpth_valid = dpth[valid_index]
        y_node_valid = y_node_locs[valid_index]
        xslope_valid = xslope[valid_index]
        yslope_valid = yslope[valid_index]

        # A-matrix is in order of roll, pitch, heading, x_translation, y_translation, horizontal scale factor
        self.a_matrix = np.column_stack([yslope_valid * dpth_valid - y_node_valid, xslope_valid * dpth_valid,
                                         xslope_valid * y_node_valid, xslope_valid, yslope_valid,
                                         yslope_valid * y_node_valid])
