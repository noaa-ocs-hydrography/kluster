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

        self._generate_rotated_points()
        self._grid()

    def _generate_rotated_points(self):
        ang = self.azimuth - 90
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
        grid_class.add_points(self.points, 'patch')
        grid_class.grid()
        self.grid = grid_class
        self.grid.plot()
