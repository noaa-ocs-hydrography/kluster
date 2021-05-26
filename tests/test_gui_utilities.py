import numpy as np
import matplotlib.path as mpltPath


def test_points_in_polygon():
    # used in QGIS backend to select points in a rotated rectangle (see 2d/3d points maptools)
    #                   top                left                 bottom               right
    polygon = np.array([[-72.9357, 41.1072], [-73.0000, 41.0743], [-72.9560, 41.0393], [-72.8949, 41.0797]])
    # points that are inside and outside the polygon for testing (they alternate between inside/outside)
    points = np.array([[-72.9917, 41.1035], [-72.9374, 41.0927], [-72.8935, 41.1024],
                       [-72.9444, 41.0795], [-72.9005, 41.0391], [-72.9527, 41.0614],
                       [-72.9981, 41.0405], [-72.9670, 41.0722], [-72.9225, 41.0322]])
    polypath = mpltPath.Path(polygon)
    are_inside = polypath.contains_points(points)
    assert np.array_equal(are_inside, np.array([False,  True, False,  True, False,  True, False,  True, False]))
