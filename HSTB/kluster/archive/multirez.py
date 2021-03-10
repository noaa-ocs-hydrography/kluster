# https://scipython.com/blog/quadtrees-2-implementation-in-python/
from __future__ import annotations  # this allows typing of self within a class (see rect intersects)

import numpy as np
from typing import Union
import matplotlib.pyplot as plt
import numba


class Point:
    """
    A point located at (x,y) in 2D space with a z value
    """

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __repr__(self):
        return '{}: {}'.format(str((self.x, self.y)), str(self.z))

    def __str__(self):
        return 'P({:.2f}, {:.2f}, {:.2f)'.format(self.x, self.y, self.z)

    def distance_to(self, other):
        try:
            other_x, other_y = other.x, other.y
        except AttributeError:
            other_x, other_y = other
        return np.hypot(self.x - other_x, self.y - other_y)


class Rect:
    """
    A rectangle centered at (cx, cy) with width w and height h
    """

    def __init__(self, center_x, center_y, width, height):
        self.center_x = center_x
        self.center_y = center_y
        self.width = width
        self.height = height

        self.west_edge = center_x - width / 2
        self.east_edge = center_x + width / 2
        self.north_edge = center_y - height / 2
        self.south_edge = center_y + height / 2

    def __repr__(self):
        return str((self.west_edge, self.east_edge, self.north_edge, self.south_edge))

    def __str__(self):
        return '({:.2f}, {:.2f}, {:.2f}, {:.2f})'.format(self.west_edge, self.north_edge, self.east_edge, self.south_edge)

    def contains(self, new_points: np.array):
        """
        are points inside this Rect?  Return boolean mask of points that are

        Parameters
        ----------
        new_points
            numpy array of shape (N,3) where first axis is the easting, second axis is northing

        Returns
        -------
        np.array
            True if this rect contains the given point
        """
        in_x_direction = np.logical_and(self.west_edge <= new_points[:, 0], new_points[:, 0] <= self.east_edge)
        in_y_direction = np.logical_and(self.north_edge <= new_points[:, 1], new_points[:, 1] <= self.south_edge)

        return np.logical_and(in_x_direction, in_y_direction)

    def intersects(self, other_rect: Rect):
        """
        Does the provided rect intersect with this rect?

        Parameters
        ----------
        other_rect
            other rect object

        Returns
        -------
        bool
            True if intersects
        """

        return not (other_rect.west_edge > self.east_edge or other_rect.east_edge < self.west_edge or other_rect.north_edge > self.south_edge or other_rect.south_edge < self.north_edge)

    def draw(self, ax: plt.Subplot, c: str = 'k', lw: int = 1, **kwargs):
        """
        draw this rect

        Parameters
        ----------
        ax
            subplot object to use for the plotting
        c
            color argument passed to plot
        lw
            linewidth argument passed to plot
        """

        ax.plot([self.west_edge, self.east_edge, self.east_edge, self.west_edge, self.west_edge],
                [self.north_edge, self.north_edge, self.south_edge, self.south_edge, self.north_edge],
                c=c, lw=lw, **kwargs)


class QuadTree:
    """
    A class implementing a quadtree structure
    """

    def __init__(self, boundary: Rect, max_points: int = 5, depth: int = 0):
        """
        Initialize this node of the quadtree

        Parameters
        ----------
        boundary
            a Rect object defining the region from which points are placed into this node
        max_points
            the maximum number of points the node can hold before it must divide (branch into four more nodes)
        depth
            override the index that keeps track of how deep into the quadtree this node is
        """

        self.boundary = boundary
        self.max_points = max_points
        self.points = None
        self.depth = depth
        self.divided = False

        # subtrees start out not initialized, if points tries to grow larger than max_points, divide() to create subgrids
        #   and add the point to one of those
        self.ne_quad = None
        self.nw_quad = None
        self.se_quad = None
        self.sw_quad = None

    def __str__(self):
        sp = '' * self.depth * 2
        s = str(self.boundary) + '\n'
        s += sp + ', '.join(str(point) for point in self.points)
        if not self.divided:
            return s
        return s + '\n' + '\n'.join([sp + 'nw: ' + str(self.nw_quad), sp + 'ne: ' + str(self.ne_quad)])

    def __len__(self):
        try:
            npoints = len(self.points)
        except TypeError:
            npoints = 0
        if self.divided:
            npoints += len(self.nw_quad) + len(self.ne_quad) + len(self.se_quad) + len(self.sw_quad)
        return npoints

    def divide(self):
        """
        Divide this node by spawning four child nodes
        """

        cx, cy = self.boundary.center_x, self.boundary.center_y
        w, h = self.boundary.width / 2, self.boundary.height / 2

        # The boundaries of the four children nodes are "northwest", "northeast", "southeast" and "southwest"
        #   quadrants within the boundary of the current node
        self.nw_quad = QuadTree(Rect(cx - w / 2, cy - h / 2, w, h), self.max_points, self.depth + 1)
        self.ne_quad = QuadTree(Rect(cx + w / 2, cy - h / 2, w, h), self.max_points, self.depth + 1)
        self.se_quad = QuadTree(Rect(cx + w / 2, cy + h / 2, w, h), self.max_points, self.depth + 1)
        self.sw_quad = QuadTree(Rect(cx - w / 2, cy + h / 2, w, h), self.max_points, self.depth + 1)
        self.divided = True

    def insert(self, points: np.array):
        """
        Try to insert a new Point object into this Quadtree

        Parameters
        ----------
        points
            new points as numpy array (N, 3))
        """

        dont_divide = False
        valid_points = points[self.boundary.contains(points)]
        if not np.all(valid_points):
            # The point does not lie inside boundary: bail
            dont_divide = True
        if valid_points.shape[0] < self.max_points:
            # There's room for our point without dividing the QuadTree
            self.points = valid_points
            dont_divide = True

        if not dont_divide:
            # no room, divide if necessary then try the sub quads
            if not self.divided:
                self.divide()

            return self.ne_quad.insert(valid_points) or self.nw_quad.insert(valid_points) or self.se_quad.insert(valid_points) or self.sw_quad.insert(valid_points)
        else:
            return False

    def query(self, boundary: Rect):
        """
        Find the points in the quadtree that lie within boundary

        Parameters
        ----------
        boundary
            boundary to use for the query

        Returns
        -------
        list
            list of Point objects that lie within the given boundary
        """

        # initialize with an empty array that we can concatenate with no effect
        found_points = []
        if not self.boundary.intersects(boundary):
            # If the domain of this node does not intersect the search region, we don't need to look in it for points
            return found_points

        # search this node's points to see if they lie within the boundary
        if self.points is not None:
            found_points = self.points[boundary.contains(self.points)].tolist()

        # if this node has children, search them too
        if self.divided:
            found_points.extend(self.nw_quad.query(boundary))
            found_points.extend(self.ne_quad.query(boundary))
            found_points.extend(self.sw_quad.query(boundary))
            found_points.extend(self.se_quad.query(boundary))
        return found_points

    def _query_circle(self, boundary: Rect, center: tuple, radius: float):
        """
        Find the points in the quadtree that lie within radius of centre

        boundary is a Rect object (a square) that bounds the search circle.  There is no need to call this method
        directly: use query_radius

        Parameters
        ----------
        boundary
            boundary to use for the query
        center
            tuple(x,y) that represents the center of the circle
        radius
            radius of circle

        Returns
        -------
        list
            list of Point objects that lie within the given boundary
        """

        # initialize with an empty array that we can concatenate with no effect
        found_points = np.array([]).reshape(0, 3)
        if not self.boundary.intersects(boundary):
            # If the domain of this node does not intersect the search region, we don't need to look in it for points
            return found_points

        # Search this node's points to see if they lie within boundary
        # and also lie within a circle of given radius around the centre point.
        if self.points is not None:
            found_points = self.points[self.boundary.contains(self.points)]
            within_dist = np.hypot(found_points[:, 0] - center[0], found_points[:, 1] - center[1]) <= radius
            found_points = found_points[within_dist]

        # if this node has children, search them too
        if self.divided:
            found_points = np.concatenate([found_points, self.nw_quad._query_circle(boundary, center, radius)], axis=0)
            found_points = np.concatenate([found_points, self.ne_quad._query_circle(boundary, center, radius)], axis=0)
            found_points = np.concatenate([found_points, self.sw_quad._query_circle(boundary, center, radius)], axis=0)
            found_points = np.concatenate([found_points, self.se_quad._query_circle(boundary, center, radius)], axis=0)
        return found_points

    def query_radius(self, center: tuple, radius: float):
        """
        Find the points in the quadtree that lie within radius of centre

        Parameters
        ----------
        center
            tuple(x,y) that represents the center of the circle
        radius
            radius of circle

        Returns
        -------
        list

        """

        # First find the square that bounds the search circle as a Rect object.
        boundary = Rect(center[0], center[1], 2 * radius, 2 * radius)
        return self._query_circle(boundary, center, radius)

    def draw(self, ax: plt.Subplot):
        """
        draw a representation of the quadtree on Matplotlib Axes ax

        Parameters
        ----------
        ax
            subplot to draw on
        """

        self.boundary.draw(ax)
        if self.divided:
            self.nw_quad.draw(ax)
            self.ne_quad.draw(ax)
            self.sw_quad.draw(ax)
            self.se_quad.draw(ax)


if __name__ == '__main__':
    # base run with 10000 pts and Point class: end 2.95

    from time import perf_counter

    strtt = perf_counter()
    print('start')

    # test out the quadtree and plotting functionality
    DPI = 72
    np.random.seed(60)

    width, height = 600, 400

    N = 1000
    max_points = 5

    # generate points, spread them out a little for better vizualization
    coords = np.random.randn(N, 2) * height / 3 + (width / 2, height / 2)
    # build some random depths in the 20-50 meter range
    dpths = np.expand_dims(np.random.uniform(20, 50, size=N), axis=1)
    xyz = np.concatenate((coords, dpths), axis=1)

    domain = Rect(width / 2, height / 2, width, height)
    qtree = QuadTree(domain, max_points)
    qtree.insert(xyz)

    print('Number of points in the domain =', len(qtree))

    fig = plt.figure(figsize=(700 / DPI, 500 / DPI), dpi=DPI)
    ax = plt.subplot()
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    qtree.draw(ax)

    ax.scatter(xyz[:,0], xyz[:, 1])
    ax.set_xticks([])
    ax.set_yticks([])

    region = Rect(140, 190, 150, 150)
    found_points = np.array(qtree.query(region))
    print('Number of found points =', len(found_points))

    ax.scatter(found_points[:,0], found_points[:,1], facecolors='none', edgecolors='r', s=32)

    region.draw(ax, c='r')

    ax.invert_yaxis()
    plt.tight_layout()

    endt = perf_counter()
    print('end {}'.format(endt - strtt))

    plt.show()
