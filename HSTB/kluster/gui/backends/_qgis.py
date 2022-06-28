import logging
import os, sys, re
import numpy as np
from typing import Union
from pyproj import CRS
from osgeo import gdal

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, QtXml, Signal, qgis_enabled, found_path
if not qgis_enabled:
    raise EnvironmentError('Unable to find qgis directory in {}'.format(found_path))
from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui
from HSTB.kluster import __file__ as klusterdir

from HSTB.kluster.gdal_helpers import gdal_raster_create, VectorLayer, gdal_output_file_exists, ogr_output_file_exists, get_raster_bands
from HSTB.kluster import kluster_variables

from HSTB.shared import RegistryHelpers


acceptedlayernames = ['hillshade', 'depth', 'elevation', 'density', 'vertical_uncertainty', 'horizontal_uncertainty', 'total_uncertainty',
                      'hypothesis_count', 'hypothesis_ratio']
invert_colormap_layernames = ['vertical_uncertainty', 'horizontal_uncertainty', 'total_uncertainty', 'hypothesis_count',
                              'hypothesis_ratio']


class CompassRoseItem(qgis_gui.QgsMapCanvasItem):
    def __init__(self, canvas):
        super().__init__(canvas)
        self.canvas = canvas
        self.center = qgis_core.QgsPoint(0, 0)
        self.size = 100

    def setCenter(self, center):
        self.center = center

    def center(self):
        return self.center

    def setSize(self, size):
        self.size = size

    def size(self):
        return self.size

    def boundingRect(self):
        return QtCore.QRectF(self.center.x() - self.size / 2,
                             self.center.y() - self.size / 2,
                             self.center.x() + self.size / 2,
                             self.center.y() + self.size / 2)

    def paint(self, painter, option, widget):
        curwidth, curheight = self.canvas.width(), self.canvas.height()
        newcenter = QtCore.QPointF(int(curwidth - (curwidth / 11)), int(curheight / 10))
        self.setCenter(newcenter)
        self.setSize(int(curwidth / 20))

        fontSize = int(18 * self.size / 120)
        painter.setFont(QtGui.QFont("Times", pointSize=fontSize, weight=75))
        metrics = painter.fontMetrics()
        labelSize = metrics.height()
        margin = 5

        x = self.center.x()
        y = self.center.y()
        size = self.size - labelSize - margin

        path = QtGui.QPainterPath()
        path.moveTo(x, y - size * 0.23)
        path.lineTo(x - size * 0.45, y - size * 0.45)
        path.lineTo(x - size * 0.23, y)
        path.lineTo(x - size * 0.45, y + size * 0.45)
        path.lineTo(x, y + size * 0.23)
        path.lineTo(x + size * 0.45, y + size * 0.45)
        path.lineTo(x + size * 0.23, y)
        path.lineTo(x + size * 0.45, y - size * 0.45)
        path.closeSubpath()
        painter.fillPath(path, QtGui.QColor("light gray"))

        path = QtGui.QPainterPath()
        path.moveTo(x, y - size)
        path.lineTo(x - size * 0.18, y - size * 0.18)
        path.lineTo(x - size, y)
        path.lineTo(x - size * 0.18, y + size * 0.18)
        path.lineTo(x, y + size)
        path.lineTo(x + size * 0.18, y + size * 0.18)
        path.lineTo(x + size, y)
        path.lineTo(x + size * 0.18, y - size * 0.18)
        path.closeSubpath()
        painter.fillPath(path, QtGui.QColor("black"))

        labelX = x - metrics.width("N") / 2
        labelY = y - self.size + labelSize - metrics.descent()
        painter.drawText(QtCore.QPoint(labelX, labelY), "N")
        labelX = x - metrics.width("S") / 2
        labelY = y + self.size - labelSize + metrics.ascent()
        painter.drawText(QtCore.QPoint(labelX, labelY), "S")

        labelX = x - self.size + labelSize / 2 - metrics.width("E") / 2
        labelY = y - metrics.height() / 2 + metrics.ascent()
        painter.drawText(QtCore.QPoint(labelX, labelY), "E")
        labelX = x + self.size - labelSize / 2 - metrics.width("W") / 2
        labelY = y - metrics.height() / 2 + metrics.ascent()
        painter.drawText(QtCore.QPoint(labelX, labelY), "W")


class DistanceTool(qgis_gui.QgsMapTool):
    """
    Render a green line and give distance from start to end point using the WGS84 ellipsoid curvature.  Each click
    resets the map tool.  Distance is given in meters (if the tool finds a different unit is being provided, it raises
    an exception as I think that might be indicative of an issue with the ellipsoid set.
    """

    def __init__(self, canvas):
        self.canvas = canvas
        qgis_gui.QgsMapToolEmitPoint.__init__(self, self.canvas)
        self.rubberBand = qgis_gui.QgsRubberBand(self.canvas, True)
        self.rubberBand.setColor(QtCore.Qt.darkGreen)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
        self.rubberBand.setWidth(4)
        
        self.start_point = None
        self.end_point = None
        self.reset()

    def reset(self):
        """
        Clear the line
        """
        self.start_point = None
        self.end_point = None
        self.rubberBand.reset(qgis_core.QgsWkbTypes.LineGeometry)

    def canvasPressEvent(self, e):
        """
        Start a new line
        """
        self.start_point = self.toMapCoordinates(e.pos())
        self.end_point = self.start_point
        self.showLine(self.start_point, self.end_point)

    def canvasReleaseEvent(self, e):
        """
        Finish the line on releasing the mouse.  If the start and end point are the same, it just resets.  Otherwise
        prints the distance in meters.
        """
        l = self.line()
        if l is not None:
            distance = qgis_core.QgsDistanceArea()
            distance.setEllipsoid('WGS84')
            m = distance.measureLine(self.start_point, self.end_point)
            units_enum = distance.lengthUnits()
            if units_enum != 0:
                raise ValueError('Something wrong with the distance units, got {} instead of 0=meters'.format(units_enum))
            print('******************************************************')
            print('Distance of {} meters'.format(round(m, 3)))
            print('******************************************************')
            self.start_point = None
        else:
            self.reset()

    def canvasMoveEvent(self, e):
        """
        Mouse movement resets and shows the new line where the end point is the current mouse position
        """
        if self.start_point is None:
            return
        self.end_point = self.toMapCoordinates(e.pos())
        self.showLine(self.start_point, self.end_point)

    def showLine(self, start_point: qgis_core.QgsPoint, end_point: qgis_core.QgsPoint):
        """
        Show the rubberband object from the provided start point to the end point.

        Parameters
        ----------
        start_point
            QgsPoint for the start of the line
        end_point
            QgsPoint for the end of the line
        """

        self.rubberBand.reset(qgis_core.QgsWkbTypes.LineGeometry)
        if start_point.x() == end_point.x() or start_point.y() == end_point.y():
            return

        point1 = qgis_core.QgsPointXY(start_point.x(), start_point.y())
        point2 = qgis_core.QgsPointXY(end_point.x(), end_point.y())

        self.rubberBand.addPoint(point1, False)
        self.rubberBand.addPoint(point2, True)
        self.rubberBand.show()

    def line(self):
        """
        Return the linestring if the start and end points are valid
        """
        if self.start_point is None or self.end_point is None:
            return None
        elif self.start_point.x() == self.end_point.x() or self.start_point.y() == self.end_point.y():
            return None
        return qgis_core.QgsLineString(self.start_point, self.end_point)

    def deactivate(self):
        """
        Turn the tool off, make sure to clear the rubberband as well
        """
        self.reset()
        qgis_gui.QgsMapTool.deactivate(self)
        self.deactivated.emit()


class QueryTool(qgis_gui.QgsMapTool):
    """
    Get the value for all raster layers loaded at the mouse position.  We filter out vector layers and any loaded
    WMS background layers.  Should just get surface layers
    """

    def __init__(self, parent):
        self.parent = parent
        qgis_gui.QgsMapTool.__init__(self, self.parent.canvas)

    def canvasPressEvent(self, e):
        """
        On press we print out the tooltip text to the stdout
        """

        text = self._get_cursor_data(e)
        print('******************************************************')
        print(text)
        print('******************************************************')

    def canvasMoveEvent(self, e):
        """
        On moving the mouse, we get the new raster information at mouse position and show a new tooltip
        """

        text = self._get_cursor_data(e)
        QtWidgets.QToolTip.showText(self.parent.canvas.mapToGlobal(self.parent.canvas.mouseLastXY()), text,
                                    self.parent.canvas, QtCore.QRect(), 1000000)

    def deactivate(self):
        """
        Deactivate the tool
        """

        qgis_gui.QgsMapTool.deactivate(self)
        self.deactivated.emit()

    def _get_cursor_data(self, e):
        """
        Get the mouse position, transform it to the map coordinates, build the text that feeds the tooltip and the
        print on mouseclick event.  Only query non-WMS raster layers.  WMS is background stuff, we don't care about those
        values.  If the raster layer is a virtual file system object (vsimem) we trim that part of the path off for display.
        """
        x = e.pos().x()
        y = e.pos().y()
        point = self.parent.canvas.getCoordinateTransform().toMapCoordinates(x, y)
        text = 'Latitude: {}, Longitude: {}'.format(round(point.y(), 7), round(point.x(), 7))

        datum_layers = []
        for name, layer in self.parent.project.mapLayers().items():
            if layer.type() in [qgis_core.QgsMapLayerType.RasterLayer, qgis_core.QgsMapLayerType.MeshLayer]:
                try:
                    datum = layer.crs().description()
                except:
                    datum = 'unknown'
                if datum not in datum_layers and datum:
                    try:
                        layerpt = self.parent.map_point_to_layer_point(layer, point)
                        layerpt = (layerpt.y(), layerpt.x())
                    except:
                        layerpt = ('unknown', 'unknown')
                    text += f'\n({datum}) Northing: {layerpt[0]} Easting: {layerpt[1]}'
                    datum_layers.append(datum)
                if layer.dataProvider().name() != 'wms':
                    if layer.name() in self.parent.layer_manager.shown_layer_names:
                        try:
                            layer_point = self.parent.map_point_to_layer_point(layer, point)
                            if layer.type() == qgis_core.QgsMapLayerType.RasterLayer:
                                ident = layer.dataProvider().identify(layer_point, qgis_core.QgsRaster.IdentifyFormatValue)
                                if ident:
                                    lname = layer.name()
                                    if lname[0:8] == '/vsimem/':
                                        lname = lname[8:]
                                    bands_under_cursor = ident.results()
                                    band_exists = False
                                    for ky, val in bands_under_cursor.items():
                                        band_name, band_value = layer.bandName(ky), round(val, 3)
                                        if not band_exists and band_name:
                                            text += '\n\n{}'.format(lname)
                                        text += '\n{}: {}'.format(band_name, band_value)
                            else:  # mesh layers don't have the identify method, you have to use datasetValue
                                mval = layer.datasetValue(qgis_core.QgsMeshDatasetIndex(0, 0), point).scalar()
                                lname = layer.name().split('____')[1]
                                text += '\n\n{}'.format(lname)
                                text += '\n{}: {}'.format(lname, mval)
                        except:  # point is outside of the transform
                            pass
        return text


class SelectTool(qgis_gui.QgsMapToolEmitPoint):
    """
    Allow the user to drag select a box and this tool will emit the corner coordinates using the select Signal.  We use
    this in Kluster to select lines.
    """
    # minlat, maxlat, minlon, maxlon in Map coordinates (WGS84 for Kluster)
    select = Signal(float, float, float, float)

    def __init__(self, canvas):
        self.canvas = canvas
        qgis_gui.QgsMapToolEmitPoint.__init__(self, self.canvas)
        self.rubberBand = qgis_gui.QgsRubberBand(self.canvas, True)
        self.rubberBand.setColor(QtCore.Qt.transparent)
        self.rubberBand.setFillColor(QtGui.QColor(0, 0, 255, 50))

        self.start_point = None
        self.end_point = None
        self.reset()

    def reset(self):
        """
        Clear the rubberband obj and points
        """

        self.start_point = None
        self.end_point = None
        self.isEmittingPoint = False
        self.rubberBand.reset(qgis_core.QgsWkbTypes.PolygonGeometry)

    def canvasPressEvent(self, e):
        """
        Set the start position of the rectangle on click
        """

        self.start_point = self.toMapCoordinates(e.pos())
        self.end_point = self.start_point
        self.isEmittingPoint = True
        self.showRect(self.start_point, self.end_point)

    def canvasReleaseEvent(self, e):
        """
        Finish the rectangle and emit the corner coordinates in map coordinate system
        """

        self.isEmittingPoint = False
        r = self.rectangle()
        if r is not None:
            self.select.emit(r.yMinimum(), r.yMaximum(), r.xMinimum(), r.xMaximum())
        self.reset()

    def canvasMoveEvent(self, e):
        """
        On move update the rectangle
        """

        if not self.isEmittingPoint:
            return
        self.end_point = self.toMapCoordinates(e.pos())
        self.showRect(self.start_point, self.end_point)

    def showRect(self, start_point: qgis_core.QgsPoint, end_point: qgis_core.QgsPoint):
        """
        Show the rubberband object from the provided start point to the end point.  Clear out any existing rect.

        Parameters
        ----------
        start_point
            QgsPoint for the start of the rect
        end_point
            QgsPoint for the end of the rect
        """

        self.rubberBand.reset(qgis_core.QgsWkbTypes.PolygonGeometry)
        if start_point.x() == end_point.x() or start_point.y() == end_point.y():
            return

        point1 = qgis_core.QgsPointXY(start_point.x(), start_point.y())
        point2 = qgis_core.QgsPointXY(start_point.x(), end_point.y())
        point3 = qgis_core.QgsPointXY(end_point.x(), end_point.y())
        point4 = qgis_core.QgsPointXY(end_point.x(), start_point.y())

        self.rubberBand.addPoint(point1, False)
        self.rubberBand.addPoint(point2, False)
        self.rubberBand.addPoint(point3, False)
        self.rubberBand.addPoint(point4, True)  # true to update canvas
        self.rubberBand.show()

    def rectangle(self):
        """
        Return the QgsRectangle object for the drawn start/end points
        """

        if self.start_point is None or self.end_point is None:
            return None
        elif self.start_point.x() == self.end_point.x() or self.start_point.y() == self.end_point.y():
            return None
        return qgis_core.QgsRectangle(self.start_point, self.end_point)

    def deactivate(self):
        """
        Turn off the tool
        """
        qgis_gui.QgsMapTool.deactivate(self)
        self.deactivated.emit()


class RectangleMapTool(qgis_gui.QgsMapToolEmitPoint):
    """
    Draw a persistent black rectangle on the screen and emit the coordinates for the rect in map coordinate system.
    """
    # minlat, maxlat, minlon, maxlon in Map coordinates (WGS84 for Kluster)
    select = Signal(object, float)
    clear_box = Signal(bool)

    def __init__(self, canvas, show_direction: bool = True):
        self.base_color = QtCore.Qt.black
        self.canvas = canvas
        qgis_gui.QgsMapToolEmitPoint.__init__(self, self.canvas)
        self.rubberBand = qgis_gui.QgsRubberBand(self.canvas, True)
        self.rubberBand.setColor(self.base_color)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
        self.rubberBand.setWidth(3)

        if show_direction:
            self.direction_arrow = qgis_gui.QgsRubberBand(self.canvas, False)
            self.direction_arrow.setColor(self.base_color)
            self.direction_arrow.setWidth(4)
        else:
            self.direction_arrow = None

        self.isEmittingPoint = False
        self.enable_rotation = False
        self.start_point = None
        self.end_point = None
        self.final_start_point = None
        self.final_end_point = None
        self.start_azimuth = 0
        self.azimuth = 0

        self.first_click = True
        self.second_click = False
        self.reset()

    def reset(self):
        """
        Clear the rectangle
        """
        self.rubberBand.setColor(self.base_color)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
        if self.direction_arrow:
            self.direction_arrow.setColor(self.base_color)
        self.start_point = None
        self.end_point = None
        self.final_start_point = None
        self.final_end_point = None
        self.azimuth = 0
        self.start_azimuth = 0
        self.first_click = False
        self.second_click = False
        self.isEmittingPoint = False
        self.enable_rotation = False
        self.rubberBand.reset(qgis_core.QgsWkbTypes.PolygonGeometry)
        if self.direction_arrow:
            self.direction_arrow.reset(qgis_core.QgsWkbTypes.LineGeometry)
        self.clear_box.emit(True)

    def keyPressEvent(self, e):
        ctrl_pressed = e.key() == 16777249
        if ctrl_pressed:
            self.enable_rotation = True

    def keyReleaseEvent(self, e):
        ctrl_released = e.key() == 16777249
        if ctrl_released:
            self.enable_rotation = False

    def return_azimuth(self, start_x, start_y, end_x, end_y):
        """
        build a new azimuth in radians from the given start/end points
        """
        centerx = end_x - start_x
        centery = end_y - start_y
        az = np.arctan2(centerx, centery)
        return az

    def canvasPressEvent(self, e):
        """
        Lay down the start point of the rectangle and reset the end point to the start point.
        """

        left_click = e.button() == 1
        right_click = e.button() == 2
        if left_click:  # first click sets the origin of the rectangle
            if not self.first_click and not self.second_click:
                self.reset()
                self.first_click = True
                self.second_click = False
                self.start_point = self.toMapCoordinates(e.pos())
                self.end_point = self.start_point
                self.isEmittingPoint = True
                self.showRect(self.start_point, self.end_point)
            elif self.first_click:  # second click sets the end point and fixes the rectangle in place
                self.final_start_point = self.toCanvasCoordinates(self.start_point)
                self.final_end_point = e.pos()
                self.first_click = False
                self.second_click = True
                self.isEmittingPoint = False
            elif self.second_click:  # third click loads
                self.first_click = False
                self.second_click = False
                self.rubberBand.setColor(QtCore.Qt.darkYellow)
                self.rubberBand.setFillColor(QtCore.Qt.transparent)
                self.rubberBand.update()
                if self.direction_arrow:
                    self.direction_arrow.setColor(QtCore.Qt.darkYellow)
                    self.direction_arrow.update()
                poly, az = self.rectangle()
                if poly is not None:
                    self.select.emit(poly, az)
        if right_click:  # clear the rectangle
            self.reset()

    def canvasMoveEvent(self, e):
        """
        On moving the mouse cursor, the rectangle continuously updates
        """
        if (not self.isEmittingPoint and not self.enable_rotation) or (not self.first_click and not self.second_click):
            return
        self.end_point = self.toMapCoordinates(e.pos())
        self.showRect(self.start_point, self.end_point)
        e.accept()

    def showRect(self, start_point: qgis_core.QgsPoint, end_point: qgis_core.QgsPoint):
        """
        Show the rubberband object from the provided start point to the end point.  Clear out any existing rect.

        Parameters
        ----------
        start_point
            QgsPoint for the start of the rect
        end_point
            QgsPoint for the end of the rect
        """

        if start_point.x() == end_point.x() or start_point.y() == end_point.y():
            return

        point1 = qgis_core.QgsPointXY(start_point.x(), start_point.y())
        point2 = qgis_core.QgsPointXY(start_point.x(), end_point.y())
        point3 = qgis_core.QgsPointXY(end_point.x(), end_point.y())
        point4 = qgis_core.QgsPointXY(end_point.x(), start_point.y())

        if self.enable_rotation and self.second_click:

            point1 = qgis_core.QgsPointXY(self.final_start_point.x(), self.final_start_point.y())
            point2 = qgis_core.QgsPointXY(self.final_start_point.x(), self.final_end_point.y())
            point3 = qgis_core.QgsPointXY(self.final_end_point.x(), self.final_end_point.y())
            point4 = qgis_core.QgsPointXY(self.final_end_point.x(), self.final_start_point.y())
            center_pixel = qgis_core.QgsPointXY(((point3.x() - point1.x()) / 2) + point1.x(),
                                                ((point3.y() - point1.y()) / 2) + point1.y())
            arryposition = point1.y() + (point2.y() - point1.y()) / 2
            arrpoint1 = qgis_core.QgsPointXY(int(point2.x()), int(arryposition))
            arrpoint2 = qgis_core.QgsPointXY(int(point2.x() - 15), int(arryposition))
            arrpoint3 = qgis_core.QgsPointXY(int(point2.x() - 10), int(arryposition - 5))
            arrpoint4 = qgis_core.QgsPointXY(int(point2.x() - 10), int(arryposition + 5))
            arrpoint5 = qgis_core.QgsPointXY(int(point2.x() - 15), int(arryposition))

            az = self.return_azimuth(start_point.x(), start_point.y(), end_point.x(), end_point.y())
            if not self.start_azimuth:
                self.start_azimuth = az
            self.azimuth = az - self.start_azimuth

            cos_az = np.cos(self.azimuth)
            sin_az = np.sin(self.azimuth)

            point1 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (point1.x() - center_pixel.x()) - sin_az * (point1.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (point1.x() - center_pixel.x()) + cos_az * (point1.y() - center_pixel.y()))
            point2 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (point2.x() - center_pixel.x()) - sin_az * (point2.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (point2.x() - center_pixel.x()) + cos_az * (point2.y() - center_pixel.y()))
            point3 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (point3.x() - center_pixel.x()) - sin_az * (point3.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (point3.x() - center_pixel.x()) + cos_az * (point3.y() - center_pixel.y()))
            point4 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (point4.x() - center_pixel.x()) - sin_az * (point4.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (point4.x() - center_pixel.x()) + cos_az * (point4.y() - center_pixel.y()))
            arrpoint1 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (arrpoint1.x() - center_pixel.x()) - sin_az * (arrpoint1.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (arrpoint1.x() - center_pixel.x()) + cos_az * (arrpoint1.y() - center_pixel.y()))
            arrpoint2 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (arrpoint2.x() - center_pixel.x()) - sin_az * (arrpoint2.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (arrpoint2.x() - center_pixel.x()) + cos_az * (arrpoint2.y() - center_pixel.y()))
            arrpoint3 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (arrpoint3.x() - center_pixel.x()) - sin_az * (arrpoint3.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (arrpoint3.x() - center_pixel.x()) + cos_az * (arrpoint3.y() - center_pixel.y()))
            arrpoint4 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (arrpoint4.x() - center_pixel.x()) - sin_az * (arrpoint4.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (arrpoint4.x() - center_pixel.x()) + cos_az * (arrpoint4.y() - center_pixel.y()))
            arrpoint5 = qgis_core.QgsPoint(
                center_pixel.x() + cos_az * (arrpoint5.x() - center_pixel.x()) - sin_az * (arrpoint5.y() - center_pixel.y()),
                center_pixel.y() + sin_az * (arrpoint5.x() - center_pixel.x()) + cos_az * (arrpoint5.y() - center_pixel.y()))

            mappoint1 = self.toMapCoordinates(QtCore.QPoint(int(point1.x()), int(point1.y())))
            mappoint2 = self.toMapCoordinates(QtCore.QPoint(int(point2.x()), int(point2.y())))
            mappoint3 = self.toMapCoordinates(QtCore.QPoint(int(point3.x()), int(point3.y())))
            mappoint4 = self.toMapCoordinates(QtCore.QPoint(int(point4.x()), int(point4.y())))

            arrpoint1 = self.toMapCoordinates(QtCore.QPoint(int(arrpoint1.x()), int(arrpoint1.y())))
            arrpoint2 = self.toMapCoordinates(QtCore.QPoint(int(arrpoint2.x()), int(arrpoint2.y())))
            arrpoint3 = self.toMapCoordinates(QtCore.QPoint(int(arrpoint3.x()), int(arrpoint3.y())))
            arrpoint4 = self.toMapCoordinates(QtCore.QPoint(int(arrpoint4.x()), int(arrpoint4.y())))
            arrpoint5 = self.toMapCoordinates(QtCore.QPoint(int(arrpoint5.x()), int(arrpoint5.y())))
        else:
            mappoint1 = point1
            mappoint2 = point2
            mappoint3 = point3
            mappoint4 = point4

            canvaspoint1 = self.toCanvasCoordinates(point1)
            canvaspoint2 = self.toCanvasCoordinates(point2)
            canvaspoint3 = self.toCanvasCoordinates(point3)
            canvaspoint4 = self.toCanvasCoordinates(point4)
            arryposition = canvaspoint1.y() + (canvaspoint2.y() - canvaspoint1.y()) / 2

            arrpoint1 = self.toMapCoordinates(QtCore.QPoint(int(canvaspoint2.x()), int(arryposition)))
            arrpoint2 = self.toMapCoordinates(QtCore.QPoint(int(canvaspoint2.x() - 15), int(arryposition)))
            arrpoint3 = self.toMapCoordinates(QtCore.QPoint(int(canvaspoint2.x() - 10), int(arryposition - 5)))
            arrpoint4 = self.toMapCoordinates(QtCore.QPoint(int(canvaspoint2.x() - 10), int(arryposition + 5)))
            arrpoint5 = self.toMapCoordinates(QtCore.QPoint(int(canvaspoint2.x() - 15), int(arryposition)))

        if self.direction_arrow:
            self.direction_arrow.reset(qgis_core.QgsWkbTypes.LineGeometry)
            self.direction_arrow.addPoint(arrpoint1, False)
            self.direction_arrow.addPoint(arrpoint2, False)
            self.direction_arrow.addPoint(arrpoint3, False)
            self.direction_arrow.addPoint(arrpoint4, False)
            self.direction_arrow.addPoint(arrpoint5, True)
            self.direction_arrow.show()

        self.rubberBand.reset(qgis_core.QgsWkbTypes.PolygonGeometry)
        self.rubberBand.addPoint(mappoint1, False)
        self.rubberBand.addPoint(mappoint2, False)
        self.rubberBand.addPoint(mappoint3, False)
        self.rubberBand.addPoint(mappoint4, True)  # true to update canvas

        self.rubberBand.show()

    def rectangle(self):
        """
        Return the points of the rectangle drawn.  Requires all four points to get the rotated polygon that we use
        later to find points inside the polygon.  Get the azimuth as well if you need to do some calcs later.
        """

        point1 = self.rubberBand.getPoint(0, 0)
        point2 = self.rubberBand.getPoint(0, 1)
        point3 = self.rubberBand.getPoint(0, 2)
        point4 = self.rubberBand.getPoint(0, 3)

        if point1 is None or point2 is None or point3 is None or point4 is None:
            return None, 0
        polygon = np.vstack([point1, point2, point3, point4])
        # here we build the azimuth manually instead of using self.azimuth.  self.azimuth is the change from origin of
        # box to the mouse cursor.  We want just the azimuth of the box, which we derive here using the bottom leg of the box
        az = self.return_azimuth(point2.x(), point2.y(), point3.x(), point3.y()) - (np.pi / 2)
        return polygon, az

    def finalize(self):
        """
        After receiving notice that the points are fully loaded, we turn the tool Green to signify completion
        """
        self.rubberBand.setColor(QtCore.Qt.green)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
        self.rubberBand.update()
        if self.direction_arrow:
            self.direction_arrow.setColor(QtCore.Qt.green)
            self.direction_arrow.update()

    def deactivate(self):
        """
        Deactivate the map tool
        """
        # self.reset()  # we want to leave the rectangle on screen on deactivation (when user clicks another map tool)
        qgis_gui.QgsMapTool.deactivate(self)
        self.deactivated.emit()


class RasterShader(qgis_core.QgsRasterShader):
    """
    Configure a new raster shader, controls the appearance of the raster layers in the 2dview.  Add a new tuple
    attribute (see redtoblue) if you want to have a new colorramp options.
    """
    def __init__(self, layer_min: float, layer_max: float, color_scheme: str = 'redtoblue'):
        super().__init__()
        self.layer_min = layer_min
        self.layer_max = layer_max
        self.color_scheme = color_scheme

        self.redtoblue = ((255, 0, 0), (255, 165, 0), (255, 255, 0), (0, 128, 0), (0, 0, 255), (75, 0, 130), (238, 130, 238))
        self.bluetored = ((238, 130, 238), (75, 0, 130), (0, 0, 255), (0, 128, 0), (255, 255, 0), (255, 165, 0), (255, 0, 0))
        self._set_scheme()

    def _set_scheme(self):
        lyrcolors = getattr(self, self.color_scheme)
        fcn = qgis_core.QgsColorRampShader()
        fcn.setColorRampType(qgis_core.QgsColorRampShader.Interpolated)
        diff = (self.layer_max - self.layer_min) / len(lyrcolors)
        lyrlst = []
        for cnt, lyrcolor in enumerate(lyrcolors):
            lyrlst.append(qgis_core.QgsColorRampShader.ColorRampItem(self.layer_min + (cnt * diff), QtGui.QColor(lyrcolor[0], lyrcolor[1], lyrcolor[2])))
        fcn.setColorRampItemList(lyrlst)
        self.setRasterShaderFunction(fcn)
        self.setMinimumValue(self.layer_min)
        self.setMaximumValue(self.layer_max)


class LayerManager:
    """
    Manage the layer order and what layers are currently loaded in QgsMapCanvas.  As layers are added, the order is
    maintained in the names in order attribute.  The layers currently shown are maintained in the shown layers index.
    """
    def __init__(self, parent):
        self.parent = parent
        self.layer_data_lookup = {}
        self.layer_type_lookup = {}
        self.layer_settings_lookup = {}
        self.names_in_order = []
        self.shown_layers_index = []

    @property
    def shown_layers(self):
        """
        Sort layers and return the list of QgsMapLayer objects.  We want surfaces on top of lines on top of background
        layers.
        """
        bground_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                             self.layer_type_lookup[self.names_in_order[t]] == 'background']
        line_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                          self.layer_type_lookup[self.names_in_order[t]] == 'line']
        surf_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                          self.layer_type_lookup[self.names_in_order[t]] == 'surface']
        genraster_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                               self.layer_type_lookup[self.names_in_order[t]] == 'raster']
        genvector_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                               self.layer_type_lookup[self.names_in_order[t]] == 'vector']
        mesh_lyr_names = [self.names_in_order[t] for t in self.shown_layers_index if
                          self.layer_type_lookup[self.names_in_order[t]] == 'mesh']
        # always render surfaces on top of lines on top of background layers
        lyr_names = mesh_lyr_names + genvector_lyr_names + genraster_lyr_names + surf_lyr_names + line_lyr_names + bground_lyr_names
        return [self.layer_data_lookup[lyr] for lyr in lyr_names]

    @property
    def shown_layer_names(self):
        """
        Return the names of all shown layers
        """
        lyr_names = [self.names_in_order[t] for t in self.shown_layers_index]
        return lyr_names

    @property
    def all_layers(self):
        """
        return the list of QgsMapLayer objects for all layers, regardless of whether or not they are shown.
        """
        return [self.layer_data_lookup[lyr] for lyr in self.names_in_order]

    @property
    def line_layers(self):
        """
        return the list of QgsMapLayer objects for all line layers
        """
        return self.return_layers_by_type('line')

    @property
    def surface_layers(self):
        """
        return the list of QgsMapLayer objects for all surface layers
        """

        return self.return_layers_by_type('surface')

    @property
    def raster_layers(self):
        """
        return the list of QgsMapLayer objects for all general raster layers
        """

        return self.return_layers_by_type('raster')

    @property
    def vector_layers(self):
        """
        return the list of QgsMapLayer objects for all general vector layers
        """

        return self.return_layers_by_type('vector')

    @property
    def mesh_layers(self):
        """
        return the list of QgsMapLayer objects for all mesh layers
        """

        return self.return_layers_by_type('mesh')

    @property
    def background_layers(self):
        """
        return the list of QgsMapLayer objects for all background layers
        """

        return self.return_layers_by_type('background')

    @property
    def line_layer_names(self):
        """
        return the list of layer names for all line layers
        """

        return self.return_layer_names_by_type('line')

    @property
    def surface_layer_names(self):
        """
        return the list of layer names for all surface layers
        """

        return self.return_layer_names_by_type('surface')

    @property
    def background_layer_names(self):
        """
        return the list of layer names for all background layers
        """

        return self.return_layer_names_by_type('background')

    @property
    def raster_layer_names(self):
        """
        return the list of layer names for all raster layers
        """

        return self.return_layer_names_by_type('raster')

    @property
    def vector_layer_names(self):
        """
        return the list of layer names for all vector layers
        """

        return self.return_layer_names_by_type('vector')

    @property
    def mesh_layer_names(self):
        """
        return the list of layer names for all mesh layers
        """

        return self.return_layer_names_by_type('mesh')

    def surface_layer_names_by_type(self, layertype: str):
        """
        Return all layer names that match a certain type, i.e. 'depth' or 'vertical_uncertainty'

        Parameters
        ----------
        layertype
            string identifier for the layer

        Returns
        -------
        list
            list of all surface layer names that match this layertype
        """

        return [lname for lname in self.surface_layer_names if re.findall(r'_{}_[0-9]*_[0-9]'.format(layertype), lname)]

    def surface_layers_by_type(self, layertype: str):
        """
        Return all layers that match a certain type, i.e. 'depth' or 'vertical_uncertainty'

        Parameters
        ----------
        layertype
            string identifier for the layer

        Returns
        -------
        list
            list of all qgis_core.QgsRasterLayer that match this layertype
        """

        return [self.layer_data_lookup[lname] for lname in self.surface_layer_names if re.findall(r'_{}_[0-9]*_[0-9]'.format(layertype), lname)]

    def add_layer(self, layername: str, bandname: str, providertype: str, color: QtGui.QColor,
                  layertype: str, layerdata: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer],
                  opacity: float, renderer):
        """
        Add new layer to the layer manager class.  We populate the lookups for data/type/name.
        
        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path, ex: '/vsimem/tj_patch_test_710_20220624_002103_depth_1_8.0.tif'
        bandname
            layer name to use from the source data, ex: 'depth_1'
        providertype
            one of ['gdal', 'wms', 'ogr']
        color
            optional, only used for vector layers, will set the color of that layer to the provided, can just be None
        layertype
            one of line, background, surface, raster, vector, mesh
        layerdata
            the qgs layer object for that layer
        opacity
            opacity as percentage from 0 to 1
        renderer
            qgis._core.QgsRasterRenderer object for the layer
        """
        
        if layername not in self.names_in_order:
            self.layer_data_lookup[layername] = layerdata
            self.layer_type_lookup[layername] = layertype
            self.layer_settings_lookup[layername] = {'color': color, 'bandname': bandname, 'opacity': opacity,
                                                     'providertype': providertype, 'renderer': renderer}
            self.names_in_order.append(layername)
        else:
            self.parent.print('Cant add layer {}, already in layer manager'.format(layername), logging.ERROR)

    def show_layer(self, layername: str):
        """
        This doesn't actually render the layer, but it does add the layer to the shown layer lookup.  
        
        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """

        if layername in self.names_in_order:
            layer_idx = self.names_in_order.index(layername)
            if layer_idx not in self.shown_layers_index:
                self.shown_layers_index.append(layer_idx)
            else:
                self.parent.print('show_layer: layer already in shown layers', logging.WARNING)
        else:
            self.parent.print('show layer: layer not loaded', logging.WARNING)

    def show_all_layers(self):
        """
        Set all layers as shown, but adding the names in order index to the shown layers index.
        """

        self.shown_layers_index = np.arange(len(self.names_in_order)).tolist()

    def hide_all_layers(self):
        """
        hide all layers by clearing out the shown layers index
        """

        self.shown_layers_index = []

    def hide_layer(self, layername: str):
        """
        hide layer corresponding to the provided layername if it has been added.  Hiding retains the data in the lookup
        dicts, but removes the shown layer index

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """

        if layername in self.names_in_order:
            layer_idx = self.names_in_order.index(layername)
            if layer_idx in self.shown_layers_index:
                self.shown_layers_index.remove(layer_idx)
            else:
                self.parent.print('hide_layer: layer not in shown layers', logging.ERROR)
        else:
            self.parent.print('hide layer: layer not loaded', logging.WARNING)

    def remove_layer(self, layername: str):
        """
        remove layer corresponding to the provided layername if it has been added.  Removing the layer clears all data
        associated with the layername

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """
        if layername in self.layer_data_lookup:
            layer_idx = self.names_in_order.index(layername)
            if layer_idx in self.shown_layers_index:
                self.shown_layers_index.remove(layer_idx)
            # shown layers index is based on the index of the item in the names in order list
            # since we remove the layer from the names in order list, we have to subtract one from all indices after this layer index
            for cnt, idx in enumerate(self.shown_layers_index):
                if idx > layer_idx:
                    self.shown_layers_index[cnt] = self.shown_layers_index[cnt] - 1
            self.layer_data_lookup.pop(layername)
            self.layer_type_lookup.pop(layername)
            self.layer_settings_lookup.pop(layername)
            self.names_in_order.remove(layername)
        else:
            self.parent.print('remove_layer: layer not loaded', logging.ERROR)

    def return_layers_by_type(self, search_type: str):
        """
        Return all qgs layer objects for the provided layer type

        Parameters
        ----------
        search_type
            one of line, background, surface, raster, vector, mesh

        Returns
        -------
        list
            list of Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer] for the provided type
        """
        lyrs = []
        for layername, layertype in self.layer_type_lookup.items():
            if layertype == search_type:
                lyrs.append(self.layer_data_lookup[layername])
        return lyrs

    def return_layer_names_by_type(self, search_type: str):
        """
        Return all layer names for the provided layer type

        Parameters
        ----------
        search_type
            one of line, background, surface, raster, vector, mesh

        Returns
        -------
        list
            list of layer names (str) for the provided type
        """
        lyrs = []
        for layername, layertype in self.layer_type_lookup.items():
            if layertype == search_type:
                lyrs.append(layername)
        return lyrs

    def set_layer_renderer(self, layername: str, renderer=None, color=None, opacity=None):
        """
        Set the renderer properties of the layer, override the settings lookup as well so you can examine them later

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        renderer
            optional, qgis renderer object if you want to override the existing
        color
            optional, only used for vector layers, will set the color of that layer to the provided
        opacity
            optional, transparency from 0 to 1

        Returns
        -------

        """
        if layername not in self.layer_data_lookup:
            print(f'ERROR: {layername} not found in layer manager')
            return
        layerdata = self.layer_data_lookup[layername]
        if renderer is None:
            renderer = layerdata.renderer()
        else:
            layerdata.setRenderer(renderer)
            renderer = layerdata.renderer()
        if color is not None:
            renderer.symbol().setColor(color)
        else:
            try:
                renderer.symbol().getColor()
            except:  # not a vector
                color = None
        if opacity is not None:
            try:  # raster
                renderer.setOpacity(opacity)
            except:  # vector
                layerdata.setOpacity(opacity)
        else:
            try:
                opacity = renderer.opacity()
            except:  # a vector layer
                opacity = None
        layerdata.triggerRepaint()
        self.layer_settings_lookup[layername]['renderer'] = renderer
        self.layer_settings_lookup[layername]['color'] = color
        self.layer_settings_lookup[layername]['opacity'] = opacity


class MapView(QtWidgets.QMainWindow):
    """
    The main window holding the QgsMapCanvas object (self.canvas) that renders all data.  We build a QMainWindow so that
    we have access to toolbars to control the QgsMapTools (zoom, distance, etc.).  The map canvas will by default be in
    WGS84 and all imported data will be transformed to this coordinate system.

    Kluster2dView will use this widget if Kluster uses the _qgs backend
    """

    box_select = Signal(float, float, float, float)
    lines_select = Signal(object)
    box_points = Signal(object, float)
    turn_off_pointsview = Signal(bool)

    def __init__(self, parent=None, settings=None, epsg: int = kluster_variables.qgis_epsg):
        super().__init__(parent=parent)
        self.epsg = epsg
        self.vdatum_directory = None
        self.layer_background = 'Default'
        self.layer_transparency = 0.5
        self.surface_transparency = 0
        self.background_data = {}
        self.band_minmax = {}
        self.force_band_minmax = {}

        self._init_settings(settings)

        self.crs = qgis_core.QgsCoordinateReferenceSystem('EPSG:{}'.format(self.epsg))
        self.canvas = qgis_gui.QgsMapCanvas()
        self.canvas.setCanvasColor(QtCore.Qt.white)
        self.canvas.setDestinationCrs(self.crs)
        self.canvas.enableAntiAliasing(True)

        self.settings = self.canvas.mapSettings()
        self.project = qgis_core.QgsProject.instance()

        self.setCentralWidget(self.canvas)

        self.init_toolbar()
        # start off with the pan tool activated
        self.pan()
        self.layer_manager = LayerManager(self)

        # initialize the background layer with the default options (or the ones we have been provided in the optional settings kwarg)
        self.set_background(self.layer_background, self.layer_transparency, self.surface_transparency)

        self.toolSelect.select.connect(self._area_selected)
        self.toolPoints.select.connect(self._points_selected)
        self.set_extent(90, -90, 180, -180, buffer=False)

    def print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            if self.parent().parent() is not None:  # widget is docked, kluster_main is the parent of the dock
                self.parent().parent().print(msg, loglevel)
            else:  # widget is undocked, kluster_main is the parent
                self.parent().print(msg, loglevel)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger, when debug is enabled

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            if self.parent().parent() is not None:  # widget is docked, kluster_main is the parent of the dock
                self.parent().parent().debug_print(msg, loglevel)
            else:  # widget is undocked, kluster_main is the parent
                self.parent().debug_print(msg, loglevel)
        else:
            print(msg)

    def init_toolbar(self):
        """
        Build the MapView toolbar with all of our QgsMapTool objects
        """
        self.actionZoomIn = QtWidgets.QAction("Zoom in", self)
        self.actionZoomIn.setToolTip('Left click and drag to select an area, zoom in to that area.')
        self.actionZoomOut = QtWidgets.QAction("Zoom out", self)
        self.actionZoomOut.setToolTip('Left click and drag to select an area, center on that area and zoom out.')
        self.actionPan = QtWidgets.QAction("Pan", self)
        self.actionPan.setToolTip('Left click and drag to move the camera around.')
        self.actionSelect = QtWidgets.QAction("Select", self)
        self.actionSelect.setToolTip('Left click and drag to select multibeam lines.  Display line attributes in the Explorer tab.')
        self.actionQuery = QtWidgets.QAction("Query", self)
        self.actionQuery.setToolTip('Left click to get map coordinates and data values (surfaces) at the mouse position.')
        self.actionDistance = QtWidgets.QAction("Distance", self)
        self.actionDistance.setToolTip('Left click to set the origin, left click again to measure distance, see Output tab for results.')
        rectangle_instructions = 'Right click - cancel selection at any point in this process\n\n'
        rectangle_instructions += 'First left click - set origin of rectangle, drag to change the size of the area\n'
        rectangle_instructions += 'Second left click - set end point of the rectangle, freezes selection area\n'
        rectangle_instructions += ' - After the second click, hold down CTRL and move the mouse to rotate the selection\n'
        rectangle_instructions += 'Third left click - load the data in Points View (if georeferenced soundings exist)'
        self.actionPoints = QtWidgets.QAction("Points")
        self.actionPoints.setToolTip('Select georeferenced points within an area to view in Points View tab.\n\n' + rectangle_instructions)
        self.actionScreenshot = QtWidgets.QAction("Screenshot")
        self.actionScreenshot.setToolTip('Take a screenshot of the current map view')

        self.actionZoomIn.setCheckable(True)
        self.actionZoomOut.setCheckable(True)
        self.actionPan.setCheckable(True)
        self.actionSelect.setCheckable(True)
        self.actionQuery.setCheckable(True)
        self.actionDistance.setCheckable(True)
        self.actionPoints.setCheckable(True)

        self.actionZoomIn.triggered.connect(self.zoomIn)
        self.actionZoomOut.triggered.connect(self.zoomOut)
        self.actionPan.triggered.connect(self.pan)
        self.actionSelect.triggered.connect(self.selectBox)
        self.actionQuery.triggered.connect(self.query)
        self.actionDistance.triggered.connect(self.distance)
        self.actionPoints.triggered.connect(self.selectPoints)
        self.actionScreenshot.triggered.connect(self.take_screenshot)

        self.toolbar = self.addToolBar("Canvas actions")
        self.toolbar.addAction(self.actionZoomIn)
        self.toolbar.addAction(self.actionZoomOut)
        self.toolbar.addAction(self.actionPan)
        self.toolbar.addAction(self.actionSelect)
        self.toolbar.addAction(self.actionQuery)
        self.toolbar.addAction(self.actionDistance)
        self.toolbar.addAction(self.actionPoints)
        self.toolbar.addAction(self.actionScreenshot)

        # create the map tools
        self.toolPan = qgis_gui.QgsMapToolPan(self.canvas)
        self.toolPan.setAction(self.actionPan)
        self.toolZoomIn = qgis_gui.QgsMapToolZoom(self.canvas, False)  # false = in
        self.toolZoomIn.setAction(self.actionZoomIn)
        self.toolZoomOut = qgis_gui.QgsMapToolZoom(self.canvas, True)  # true = out
        self.toolZoomOut.setAction(self.actionZoomOut)
        self.toolSelect = SelectTool(self.canvas)
        self.toolSelect.setAction(self.actionSelect)
        self.toolQuery = QueryTool(self)
        self.toolQuery.setAction(self.actionQuery)
        self.toolDistance = DistanceTool(self.canvas)
        self.toolDistance.setAction(self.actionDistance)
        self.toolPoints = RectangleMapTool(self.canvas)
        self.toolPoints.setAction(self.actionPoints)
        self.toolPoints.clear_box.connect(self.clear_points)

    def wms_openstreetmap_url(self):
        """
        Build the URL for the openstreetmap wms

        Returns
        -------
        str
            openstreetmap wms url
        """
        url = 'https://a.tile.openstreetmap.org/%7Bz%7D/%7Bx%7D/%7By%7D.png'
        typ = 'xyz'
        zmax = 19
        zmin = 0
        crs = 'EPSG:{}'.format(self.epsg)
        url_with_params = 'type={}&url={}&zmax={}&zmin={}&crs={}'.format(typ, url, zmax, zmin, crs)
        return url_with_params

    def wms_satellite_url(self):
        """
        Build the URL for the google satellite data wms

        Returns
        -------
        str
            satellite wms url
        """
        url = 'http://mt0.google.com/vt/lyrs%3Ds%26hl%3Den%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D'
        typ = 'xyz'
        zmax = 18
        zmin = 0
        crs = 'EPSG:{}'.format(self.epsg)
        url_with_params = 'type={}&url={}&zmax={}&zmin={}&crs={}'.format(typ, url, zmax, zmin, crs)
        return url_with_params

    def wms_noaa_rnc(self):
        """
        Build the URL for the NOAA raster chart wms

        Returns
        -------
        str
            noaa rnc wms url
        """
        url = 'https://seamlessrnc.nauticalcharts.noaa.gov/arcgis/services/RNC/NOAA_RNC/ImageServer/WMSServer'
        lyrs = 0
        fmat = 'image/png'
        dpi = 7
        crs = 'EPSG:{}'.format(self.epsg)
        url_with_params = 'crs={}&dpiMode={}&format={}&layers={}&styles&url={}'.format(crs, dpi, fmat, lyrs, url)
        return url_with_params

    def wms_noaa_enc(self):
        urls = []
        lyrs = [4, 5, 7, 3, 1, 9, 0, 6, 2, 0]  # leave out 12, the overscale warning, 8 data quality
        for lyr in lyrs:
            url = 'https://gis.charttools.noaa.gov/arcgis/rest/services/MCS/ENCOnline/MapServer/exts/MaritimeChartService/WMSServer'
            fmat = 'image/png'
            dpi = 7
            crs = 'EPSG:{}'.format(self.epsg)
            urls.append('crs={}&dpiMode={}&format={}&layers={}&styles&url={}'.format(crs, dpi, fmat, lyr, url))
        return urls

    def wms_noaa_chartdisplay(self):
        urls = []
        lyrs = [4, 5, 7, 3, 1, 9, 0, 6, 2, 0]  # leave out 12, the overscale warning, 8 data quality
        for lyr in lyrs:
            url = 'https://gis.charttools.noaa.gov/arcgis/rest/services/MCS/NOAAChartDisplay/MapServer/exts/MaritimeChartService/WMSServer'
            fmat = 'image/png'
            dpi = 7
            crs = 'EPSG:{}'.format(self.epsg)
            urls.append('crs={}&dpiMode={}&format={}&layers={}&styles&url={}'.format(crs, dpi, fmat, lyr, url))
        return urls

    def wms_gebco(self):
        """
        Build the URL for the Gebco latest grid shaded relief wms

        Returns
        -------
        str
            gebco wms url
        """
        url = 'https://www.gebco.net/data_and_products/gebco_web_services/web_map_service/mapserv'
        lyrs = 'GEBCO_LATEST'
        fmat = 'image/png'
        dpi = 7
        crs = 'EPSG:{}'.format(self.epsg)
        url_with_params = 'crs={}&dpiMode={}&format={}&layers={}&styles&url={}'.format(crs, dpi, fmat, lyrs, url)
        return url_with_params

    def wms_emodnet(self):
        """
        Build the URL for the EMODNET Bathymetry service

        Returns
        -------
        str
            url for the EMODNET service
        """
        url = 'https://ows.emodnet-bathymetry.eu/wms'
        lyrs = 'emodnet:mean_rainbowcolour'
        fmat = 'image/png'
        dpi = 7
        crs = 'EPSG:{}'.format(self.epsg)
        url_with_params = 'crs={}&dpiMode={}&format={}&layers={}&styles&url={}'.format(crs, dpi, fmat, lyrs, url)
        return url_with_params

    def _init_settings(self, settings: dict = None):
        """
        if settings are provided, we use them over the default ones

        Parameters
        ----------
        settings
            dict of settings
        """

        if settings:
            self.layer_background = settings['layer_background']
            self.layer_transparency = float(settings['layer_transparency'])
            self.surface_transparency = float(settings['surface_transparency'])
            self.vdatum_directory = settings['vdatum_directory']

    def _area_selected(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float):
        """
        emit box select signal when a QgsMapTool is used to select a box

        Parameters
        ----------
        min_lat
            minimum latitude in map coordinates (generally wgs84 latitude)
        max_lat
            maximum latitude in map coordinates (generally wgs84 latitude)
        min_lon
            minimum longitude in map coordinates (generally wgs84 longitude)
        max_lon
            maximum longitude in map coordinates (generally wgs84 longitude)
        """

        area_of_interest = qgis_core.QgsRectangle(min_lon, min_lat, max_lon, max_lat)
        request = qgis_core.QgsFeatureRequest().setFilterRect(area_of_interest).setFlags(qgis_core.QgsFeatureRequest.ExactIntersect)
        selected_line_names = []
        for line_layer in self.layer_manager.line_layers:
            for feature in line_layer.getFeatures(request):
                selected_line_names.append(line_layer.name())
        self.lines_select.emit(selected_line_names)

    def _points_selected(self, polygon: np.ndarray, azimuth: float):
        """
        emit box_points signal when the Rectbox select tool is used, displays the points within the boundary in
        3d viewer.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (longitude, latitude) in degrees
        azimuth
            azimuth of the selection polygon in radians
        """

        self.box_points.emit(polygon, azimuth)

    def _init_none(self):
        """
        Clear all background layers
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)

    def _init_default_layers(self):
        """
        Set the default background layers, which are GSHHS shapefiles I've downloaded and included with Kluster
        """

        self._init_none()
        background_dir = os.path.join(os.path.dirname(klusterdir), 'background')
        # order here is important, will build layers in the correct sequence to show on top of each other
        bground_layers = ['WDBII_border_L2.shp', 'WDBII_border_L1.shp', 'WDBII_river_L04.shp', 'WDBII_river_L03.shp',
                          'WDBII_river_L02.shp', 'WDBII_river_L01.shp', 'GSHHS_L2.shp', 'GSHHS_L1.shp']
        bground_colors = [QtGui.QColor('red'), QtGui.QColor('red'), QtGui.QColor('blue'), QtGui.QColor('blue'),
                          QtGui.QColor('blue'), QtGui.QColor('blue'), QtGui.QColor.fromRgb(0, 0, 255, 100),
                          QtGui.QColor.fromRgb(125, 100, 45, 150)]
        bground_name = ['WDBII_L2', 'WDBII_L1', 'WDBII_L4_river', 'WDBII_L3_river', 'WDBII_L2_river', 'WDBII_L1_river',
                        'GSHHS_L2', 'GSHHS_L1']
        for blayer, bcolor, bname in zip(bground_layers, bground_colors, bground_name):
            bpath = os.path.join(background_dir, blayer)
            if os.path.exists(bpath):
                lyr = self.add_layer(bpath, bname, 'ogr', bcolor)
                if lyr:
                    self.layer_manager.set_layer_renderer(bpath, opacity=1 - self.layer_transparency)
                else:
                    self.print('QGIS Initialize: Unable to find background layer: {}'.format(bpath), logging.WARNING)
            else:
                self.print('QGIS Initialize: Unable to find background layer: {}'.format(bpath), logging.WARNING)

    def _init_vdatum_extents(self):
        """
        Set the background to the vdatum kml files that signify the extents of vdatum coverage
        """
        if not self.vdatum_directory:
            self.print('Unable to find vdatum directory, please make sure you set the path in File - Settings'.format(self.vdatum_directory), logging.WARNING)
        else:
            self._init_none()
            background_dir = os.path.join(os.path.dirname(klusterdir), 'background')
            bpath = os.path.join(background_dir, 'GSHHS_L1.shp')
            if os.path.exists(bpath):
                lyr = self.add_layer(bpath, 'GSHHS_L1', 'ogr', QtGui.QColor.fromRgb(125, 100, 45, 150))
                if lyr:
                    self.layer_manager.set_layer_renderer(bpath, opacity=1 - self.layer_transparency)
                else:
                    self.print('_init_vdatum_extents: Unable to find background layer: {}'.format(bpath), logging.WARNING)
            else:
                self.print('_init_vdatum_extents: Unable to find background layer: {}'.format(bpath), logging.WARNING)

            kmlfiles = []
            lnames = []
            for root, dirs, files in os.walk(self.vdatum_directory):
                for f in files:
                    fname, exte = os.path.splitext(f)
                    if exte == '.kml':
                        kmlfiles.append(os.path.join(root, f))
                        lnames.append(fname)
            for kmlf, lname in zip(kmlfiles, lnames):
                lyr = self.add_layer(kmlf, lname, 'ogr')
                if lyr:  # change default symbol to line from fill, that way we just get the outline
                    symb = qgis_core.QgsSimpleFillSymbolLayer.create({'color': QtGui.QColor(0, 0, 255, 120)})
                    lyr.renderer().symbol().changeSymbolLayer(0, symb)
                    self.layer_manager.set_layer_renderer(kmlf, opacity=1 - self.layer_transparency)
                    # now hide all features that represent the extents of the kml file, so that we don't have a bunch
                    #  of rectangles obscuring the actual coverage features.
                    # also hide the masked out areas
                    strsel = "NOT (description = 'GTX Coverage') AND NOT (description = 'masked out areas, code 2')"
                    lyr.setSubsetString(strsel)
                else:
                    self.print('_init_vdatum_extents: Unable to find background layer: {}'.format(kmlf), logging.WARNING)
            self.print('Loaded {} VDatum kml files'.format(len(kmlfiles)), logging.INFO)
            if not len(kmlfiles):
                self.print('_init_vdatum_extents: Unable to find any kml files in all subdirectories at {}'.format(self.vdatum_directory), logging.ERROR)

    def _init_openstreetmap(self):
        """
        Set the background to the WMS openstreetmap service
        """

        self._init_none()
        url_with_params = self.wms_openstreetmap_url()
        lyr = self.add_layer(url_with_params, 'OpenStreetMap', 'wms')
        if lyr:
            self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
        else:
            self.print('_init_openstreetmap: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_satellite(self):
        """
        Set the background to the google provied satellite imagery
        """
        self._init_none()
        url_with_params = self.wms_satellite_url()
        lyr = self.add_layer(url_with_params, 'Satellite', 'wms')
        if lyr:
            self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
        else:
            self.print('_init_satellite: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_noaa_rnc(self):
        """
        Set the background to the NOAA RNC WMS service
        """
        self._init_none()
        url_with_params = self.wms_noaa_rnc()
        lyr = self.add_layer(url_with_params, 'NOAA_RNC', 'wms')
        if lyr:
            self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
        else:
            self.print('_init_noaa_rnc: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_noaa_enc(self):
        """
        Set the background to the NOAA ENC service
        """
        self._init_none()
        urls = self.wms_noaa_enc()
        for cnt, url_with_params in enumerate(urls):
            lyr = self.add_layer(url_with_params, 'NOAA_ENC_{}'.format(cnt), 'wms')
            if lyr:
                self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
            else:
                self.print('_init_noaa_enc: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_noaa_chartdisplay(self):
        """
        Set the background to the NOAA Chart Display service
        """
        self._init_none()
        urls = self.wms_noaa_chartdisplay()
        for cnt, url_with_params in enumerate(urls):
            lyr = self.add_layer(url_with_params, 'NOAA_CHARTDISPLAY_{}'.format(cnt), 'wms')
            if lyr:
                self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
            else:
                self.print('_init_noaa_chartdisplay: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_gebco(self):
        """
        Set the background to the Gebco latest Grid Shaded Relief WMS
        """
        self._init_none()
        url_with_params = self.wms_gebco()
        lyr = self.add_layer(url_with_params, 'GEBCO', 'wms')
        if lyr:
            self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
        else:
            self.print('_init_gebco: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _init_emodnet(self):
        """
        Set the background to the Gebco latest Grid Shaded Relief WMS
        """
        self._init_none()
        url_with_params = self.wms_emodnet()
        lyr = self.add_layer(url_with_params, 'EMODNET', 'wms')
        if lyr:
            self.layer_manager.set_layer_renderer(url_with_params, opacity=1 - self.layer_transparency)
        else:
            self.print('_init_emodnet: Unable to find background layer: {}'.format(url_with_params), logging.ERROR)

    def _manager_add_layer(self, layerpath: str, bandname: str, providertype: str, color: QtGui.QColor,
                           layertype: str, layerdata: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer],
                           opacity: float, renderer):
        """
        Add the layer to the layer manager, set the layer to 'shown' in the layer manager, and add the layer to the
        shown layers in the QgsMapCanvas (see setLayers)

        Parameters
        ----------
        layerpath
            name of the layer, generally a file path or vsimem path, ex: '/vsimem/tj_patch_test_710_20220624_002103_depth_1_8.0.tif'
        bandname
            layer name to use from the source data, ex: 'depth_1'
        providertype
            one of ['gdal', 'wms', 'ogr']
        color
            optional, only used for vector layers, will set the color of that layer to the provided, can just be None
        layertype
            one of line, background, surface, raster, vector, mesh
        layerdata
            the qgs layer object for that layer
        opacity
            opacity as percentage from 0 to 1
        renderer
            qgis._core.QgsRasterRenderer object for the layer
        """

        if layerpath not in self.layer_manager.layer_data_lookup:
            self.layer_manager.add_layer(layerpath, bandname, providertype, color, layertype, layerdata, opacity, renderer)
        self._manager_show_layer(layerpath)

    def _manager_show_layer(self, layername: str):
        """
        Set the layer to 'shown' in the layer manager and add the layer to the shown layers in the QgsMapCanvas (see setLayers)

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """

        if layername not in self.layer_manager.shown_layer_names:
            self.layer_manager.show_layer(layername)
        self.canvas.setLayers(self.layer_manager.shown_layers)

    def _manager_show_all_layers(self):
        """
        Show all currently loaded layers
        """
        self.layer_manager.show_all_layers()
        self.canvas.setLayers(self.layer_manager.shown_layers)

    def _manager_hide_layer(self, layername):
        """
        Set the layer to hidden in the layer manager and remove the layer from the shown layers in the QgsMapCanvas (see setLayers)

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """
        if layername in self.layer_manager.shown_layer_names:
            self.layer_manager.hide_layer(layername)
        self.canvas.setLayers(self.layer_manager.shown_layers)

    def _manager_remove_layer(self, layername):
        """
        Remove the layer from the layer manager and remove the layer from the shown layers in the QgsMapCanvas (see setLayers)

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        """
        if layername in self.layer_manager.layer_data_lookup:
            self.layer_manager.remove_layer(layername)
        self.canvas.setLayers(self.layer_manager.shown_layers)
        # if the layer is a virtual file system object, unlink the layer to prevent mem leaks
        if layername[0:7] == r'/vsimem':
            gdal.Unlink(layername)

    def build_line_source(self, linename: str):
        """
        Build the vsimem path for the multibeam line provided

        Parameters
        ----------
        linename
            name of the multibeam file

        Returns
        -------
        str
            generated vsimem path for the line
        """

        return '/vsimem/{}.shp'.format(linename)

    def build_surface_source(self, surfname: str, lyrname: str, resolution: float):
        """
        Build the vsimem path for the surface/layer provided

        Parameters
        ----------
        surfname
            path to the surface
        lyrname
            name of the surface layer you want to show
        resolution
            resolution in meters for the surface

        Returns
        -------
        str
            generated vsimem path for the surface/layer
        """
        if surfname[-4:].lower() in ['.tif', '.bag']:
            surfname = surfname[:-4]
        newname = '{}_{}_{}.tif'.format(surfname, lyrname, resolution)
        source = '/vsimem/{}'.format(newname)
        return source

    def set_background(self, layername: str, transparency: float, surf_transparency: float):
        """
        Set the background layer(s) based on the provided layername.  See the various _init for details on how these
        background layer(s) are constructed.

        Parameters
        ----------
        layername
            one of 'Default', 'OpenStreetMap (internet required)', etc.
        transparency
            the transparency of the layer as a percentage
        surf_transparency
            the transparency of all surfaces as a percentage
        """

        self.print('Initializing {} with transparency of {}%'.format(layername, int(transparency * 100)), logging.INFO)
        self.layer_background = layername
        self.layer_transparency = transparency
        self.surface_transparency = surf_transparency
        if self.layer_background == 'None':
            self._init_none()
        elif self.layer_background == 'Default':
            self._init_default_layers()
        elif self.layer_background == 'VDatum Coverage (VDatum required)':
            self._init_vdatum_extents()
        elif self.layer_background == 'OpenStreetMap (internet required)':
            self._init_openstreetmap()
        elif self.layer_background == 'Satellite (internet required)':
            self._init_satellite()
        elif self.layer_background == 'NOAA RNC (internet required)':
            self._init_noaa_rnc()
        elif self.layer_background == 'NOAA ENC (internet required)':
            self._init_noaa_enc()
        elif self.layer_background == 'NOAA Chart Display Service (internet required)':
            self._init_noaa_chartdisplay()
        elif self.layer_background == 'GEBCO Grid (internet required)':
            self._init_gebco()
        elif self.layer_background == 'EMODnet Bathymetry (internet required)':
            self._init_emodnet()
        else:
            self.print(f'Unable to enable layer "{self.layer_background}"', logging.ERROR)

        for lyr in self.layer_manager.surface_layers:
            self.layer_manager.set_layer_renderer(lyr.source(), opacity=1 - self.surface_transparency)

    def set_extent(self, max_lat: float, min_lat: float, max_lon: float, min_lon: float, buffer: bool = True):
        """
        Set the extent of the 2d window

        Parameters
        ----------
        max_lat
            set the maximum latitude of the displayed map
        min_lat
            set the minimum latitude of the displayed map
        max_lon
            set the maximum longitude of the displayed map
        min_lon
            set the minimum longitude of the displayed map
        buffer
            if True, will extend the extents by half the current width/height
        """
        if buffer:
            lat_buffer = np.max([(max_lat - min_lat) * 0.5, 0.5])
            lon_buffer = np.max([(max_lon - min_lon) * 0.5, 0.5])
        else:
            lat_buffer = 0
            lon_buffer = 0

        min_lon = np.clip(min_lon - lon_buffer, -179.999999999, 179.999999999)
        max_lon = np.clip(max_lon + lon_buffer, -179.999999999, 179.999999999)
        min_lat = np.clip(min_lat - lat_buffer, -90, 90)
        max_lat = np.clip(max_lat + lat_buffer, -90, 90)
        self.debug_print(f'2dview Setting extent: {min_lat},{min_lon} - {max_lat},{max_lon}', logging.INFO)
        self.canvas.setExtent(qgis_core.QgsRectangle(qgis_core.QgsPointXY(min_lon, min_lat),
                                                     qgis_core.QgsPointXY(max_lon, max_lat)))

    def add_line(self, line_name: str, lats: np.ndarray, lons: np.ndarray, refresh: bool = False, color: str = 'blue'):
        """
        Draw a new multibeam trackline on the mapcanvas, unless it is already there

        Parameters
        ----------
        line_name
            name of the multibeam line
        lats
            numpy array of latitude values to plot
        lons
            numpy array of longitude values to plot
        refresh
            set to True if you want to show the line after adding here, kluster will redraw the screen after adding
            lines itself
        color
            color of the line
        """
        self.debug_print(f'2dview Adding Line {line_name}: color={color}', logging.INFO)
        source = self.build_line_source(line_name)
        if ogr_output_file_exists(source):
            # raise ValueError('Line {} already exists in this map view session'.format(line_name))
            return
        try:
            vl = VectorLayer(source, 'ESRI Shapefile', self.epsg, False)
            vl.write_to_layer(line_name, np.stack([lons, lats], axis=1), 2)  # ogr.wkbLineString
            vl.close()
            lyr = self.add_layer(source, line_name, 'ogr', QtGui.QColor(color), layertype='line')
            if refresh:
                lyr.reload()
        except:
            self.print('ERROR: Unable to build navigation from line {}'.format(line_name), logging.ERROR)

    def remove_line(self, line_name: str, refresh: bool = False):
        """
        Remove a multibeam line from the mapcanvas

        Parameters
        ----------
        line_name
            name of the multibeam line
        refresh
            optional screen refresh, True most of the time, unless you want to remove multiple lines and then refresh
            at the end
        """
        self.debug_print(f'2dview Removing Line {line_name}', logging.INFO)
        source = self.build_line_source(line_name)
        remlyr = ogr_output_file_exists(source)
        if remlyr:
            self.remove_layer(source)
        if refresh:
            self.layer_by_name(source).reload()

    def hide_line(self, line_name: str, refresh: bool = False):
        """
        Hide the line so that it is not displayed, but keep the data in the layer_manager for showing later

        Parameters
        ----------
        line_name
            name of the multibeam line
        refresh
            optional screen refresh, True most of the time, unless you want to remove multiple lines and then refresh
            at the end
        """
        self.debug_print(f'2dview Hiding Line {line_name}', logging.INFO)
        source = self.build_line_source(line_name)
        hidelyr = ogr_output_file_exists(source)
        if hidelyr:
            self.hide_layer(source)
        if refresh:
            self.layer_by_name(source).reload()

    def show_line(self, line_name, refresh=False, color: str = None):
        """
        Show the line so that it is displayed, if it was hidden

        Parameters
        ----------
        line_name
            name of the multibeam line
        refresh
            optional screen refresh, True most of the time, unless you want to remove multiple lines and then refresh
            at the end
        color
            color of the line
        """
        self.debug_print(f'2dview Showing Line {line_name}', logging.INFO)
        source = self.build_line_source(line_name)
        showlyr = ogr_output_file_exists(source)
        if showlyr:  # line is added already but hidden, we need to reconfigure
            if color:
                color = QtGui.QColor(color)
                self.layer_manager.set_layer_renderer(source, color=color)
            self.show_layer(source)
        if refresh:
            self.layer_by_name(source).reload()
        return showlyr

    def _return_all_surface_tiles(self, surfname: str, lyrname: str, resolution: float):
        """
        We add surfaces in tiles, which would have matching surface/layer/resolutions.  In order to find all loaded tiles
        that match the given parameters, we need to look for the surface names excluding the tile index.

        tile index is attached to the layer name as seen below
        ex: surfname=srgrid_mean_auto_depth, lyrname='depth_1', resolution=0.5

        source = '/vsimem/srgrid_mean_auto_depth_0.5.tif'
        search_string = '/vsimem/srgrid_mean_auto_depth'
        matching_layer_names = ['/vsimem/srgrid_mean_auto_depth_1_0.5.tif', '/vsimem/srgrid_mean_auto_depth_1_1.0.tif'...]
        match_resolution = ['/vsimem/srgrid_mean_auto_depth_1_0.5.tif', '/vsimem/srgrid_mean_auto_depth_2_0.5.tif'...]

        Parameters
        ----------
        surfname
            surface name, name of the parent folder
        lyrname
            layer name with tile index
        resolution
            resolution of the grid

        Returns
        -------
        list
            list of all vsimem file names that match the given parameters
        """

        # surface layers can be added in chunks, i.e. 'depth_1', 'depth_2', etc., but they should all use the same
        #  extents and global stats.  Figure out which category the layer fits into here.
        formatted_layername = [aln for aln in acceptedlayernames if lyrname.find(aln) > -1][0]

        source = self.build_surface_source(surfname, formatted_layername, resolution)
        search_string = os.path.splitext(source)[0].rstrip('_{}'.format(resolution))
        matching_layer_names = [lyr for lyr in self.layer_manager.names_in_order if lyr.find(search_string) != -1]
        match_resolution = [lyr for lyr in matching_layer_names if lyr.find('_{}.tif'.format(resolution)) != -1]
        return match_resolution

    def add_surface(self, surfname: str, lyrname: str, data: list, geo_transform: list, crs: Union[CRS, int], resolution: float):
        """
        Add a new surface/layer with the provided data

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        data
            list of either [2d array of depth] or [2d array of depth, 2d array of vert uncertainty]
        geo_transform
            [x origin, x pixel size, x rotation, y origin, y rotation, -y pixel size]
        crs
            pyproj CRS or an integer epsg code
        resolution
            resolution in meters for the surface
        """

        source = self.build_surface_source(surfname, lyrname, resolution)
        showlyr = gdal_output_file_exists(source)
        self.debug_print(f'2dview add_surface {surfname}, {lyrname}, {resolution} = {source}', logging.INFO)

        if not showlyr:
            if all([lyrname.find(lyr) == -1 for lyr in ['density', 'hypothesis_count']]):
                gdal_raster_create(source, data, geo_transform, crs, np.nan, (lyrname,))
            else:
                gdal_raster_create(source, data, geo_transform, crs, 0, (lyrname,))
            self.add_layer(source, lyrname, 'gdal', layertype='surface')
        else:
            self.show_surface(surfname, lyrname, resolution)

    def hide_surface(self, surfname: str, lyrname: str, resolution: float):
        """
        Hide the surface layer that corresponds to the given names.

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        resolution
            resolution in meters for the surface
        """

        hidelyrs = self._return_all_surface_tiles(surfname, lyrname, resolution)
        for lyr in hidelyrs:
            self.hide_layer(lyr)

    def show_surface(self, surfname: str, lyrname: str, resolution: float):
        """
        Show the surface layer that corresponds to the given names, if it was hidden

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        resolution
            resolution in meters for the surface
        """

        showlyrs = self._return_all_surface_tiles(surfname, lyrname, resolution)
        if showlyrs:
            for lyr in showlyrs:
                self.show_layer(lyr)
            return True
        return False

    def remove_surface(self, surfname: str, resolution: float):
        """
        Remove a surface from the mapcanvas/layer_manager

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        resolution
            resolution in meters for the surface
        """

        for lyrname in acceptedlayernames:
            remlyrs = self._return_all_surface_tiles(surfname, lyrname, resolution)
            if remlyrs:
                for lyr in remlyrs:
                    self.remove_layer(lyr)
                self.band_minmax.pop(lyrname)
                self._update_global_layer_minmax(lyrname)
                self.update_layer_minmax(lyrname)

    def _generate_combined_source(self, file_source: str, layername: str):
        if os.path.splitext(file_source)[1] in ['.000', '.s57']:
            # a quirk of the s57 driver, you need to pipe in the layername in the source
            return file_source + '|layername=' + layername + '____' + layername
        else:
            return file_source + '____' + layername

    def _parse_combined_source(self, combined_source: str):
        src, layername = combined_source.split('____')
        return src, layername

    def add_raster(self, raster_source: str, layername: str):
        """
        Add a new raster from the given source

        Parameters
        ----------
        raster_source
            filepath to the raster
        layername
            band name, ex: depth
        """

        self.debug_print(f'2dview add_raster {raster_source}, {layername}', logging.INFO)
        self.add_layer(raster_source, layername, 'gdal', layertype='raster')

    def show_raster(self, raster_source: str, layername: str):
        """
        Show the raster layer that corresponds to the given names, if it was hidden

        Parameters
        ----------
        raster_source
            filepath to the raster
        layername
            band name, ex: depth
        """

        combined_src = self._generate_combined_source(raster_source, layername)
        found_raster = [rlyr for rlyr in self.layer_manager.raster_layers if os.path.normpath(rlyr.name()) == os.path.normpath(combined_src)]
        if found_raster:
            self.show_layer(combined_src)
            return True
        return False

    def hide_raster(self, raster_source: str, layername: str):
        """
        Hide the raster layer that corresponds to the given names

        Parameters
        ----------
        raster_source
            filepath to the raster
        layername
            band name, ex: depth
        """
        combined_src = self._generate_combined_source(raster_source, layername)
        self.hide_layer(combined_src)

    def remove_raster(self, raster_source: str, layername: str = None):
        """
        Remove a raster from the mapcanvas/layer_manager

        Parameters
        ----------
        raster_source
            filepath to the raster
        layername
            band name, ex: depth
        """
        if layername:
            combined_src = self._generate_combined_source(raster_source, layername)
            self.remove_layer(combined_src)
        else:
            for rlyr in self.layer_manager.raster_layers:
                if rlyr.name().find(raster_source) > -1:
                    self.remove_layer(rlyr.name())

    def add_vector(self, vector_source: str, layername: str):
        """
        Add a new vector from the given source

        Parameters
        ----------
        vector_source
            filepath to the vector
        layername
            feature layer
        """

        self.debug_print(f'2dview add_vector {vector_source}, {layername}', logging.INFO)
        self.add_layer(vector_source, layername, 'ogr', color=QtGui.QColor('blue'), layertype='vector')

    def show_vector(self, vector_source: str, layername: str):
        """
        Show the vector layer that corresponds to the given names, if it was hidden

        Parameters
        ----------
        vector_source
            filepath to the vector
        layername
            feature layer
        """

        combined_src = self._generate_combined_source(vector_source, layername)
        found_vector = [rlyr for rlyr in self.layer_manager.vector_layers if os.path.normpath(rlyr.name()) == os.path.normpath(combined_src)]
        if found_vector:
            self.show_layer(combined_src)
            return True
        return False

    def hide_vector(self, vector_source: str, layername: str):
        """
        Hide the vector layer that corresponds to the given names

        Parameters
        ----------
        vector_source
            filepath to the vector
        layername
            band name, ex: depth
        """
        combined_src = self._generate_combined_source(vector_source, layername)
        self.hide_layer(combined_src)

    def remove_vector(self, vector_source: str, layername: str = None):
        """
        Remove a vector from the mapcanvas/layer_manager

        Parameters
        ----------
        vector_source
            filepath to the vector
        layername
            band name, ex: depth
        """
        if layername:
            combined_src = self._generate_combined_source(vector_source, layername)
            self.remove_layer(combined_src)
        else:
            for rlyr in self.layer_manager.vector_layers:
                if rlyr.name().find(vector_source) > -1:
                    self.remove_layer(rlyr.name())

    def add_mesh(self, mesh_source: str, layername: str):
        """
        Add a new mesh from the given source

        Parameters
        ----------
        mesh_source
            filepath to the mesh
        layername
            feature layer
        """

        self.debug_print(f'2dview add_mesh {mesh_source}, {layername}', logging.INFO)
        self.add_layer(mesh_source, layername, 'mdal', layertype='mesh')

    def show_mesh(self, mesh_source: str, layername: str):
        """
        Show the mesh layer that corresponds to the given names, if it was hidden

        Parameters
        ----------
        mesh_source
            filepath to the mesh
        layername
            feature layer
        """

        combined_src = self._generate_combined_source(mesh_source, layername)
        found_mesh = [rlyr for rlyr in self.layer_manager.mesh_layers if os.path.normpath(rlyr.name()) == os.path.normpath(combined_src)]
        if found_mesh:
            self.show_layer(combined_src)
            return True
        return False

    def hide_mesh(self, mesh_source: str, layername: str):
        """
        Hide the mesh layer that corresponds to the given names

        Parameters
        ----------
        mesh_source
            filepath to the mesh
        layername
            band name, ex: depth
        """
        combined_src = self._generate_combined_source(mesh_source, layername)
        self.hide_layer(combined_src)

    def remove_mesh(self, mesh_source: str, layername: str = None):
        """
        Remove a mesh from the mapcanvas/layer_manager

        Parameters
        ----------
        mesh_source
            filepath to the mesh
        layername
            band name, ex: depth
        """
        if layername:
            combined_src = self._generate_combined_source(mesh_source, layername)
            self.remove_layer(combined_src)
        else:
            for rlyr in self.layer_manager.mesh_layers:
                if rlyr.name().find(mesh_source) > -1:
                    self.remove_layer(rlyr.name())

    def remove_all_surfaces(self):
        """
        Remove all surfaces from the display and layer manager
        """
        remlyrs = self.layer_manager.surface_layer_names
        if remlyrs:
            for lyr in remlyrs:
                self.remove_layer(lyr)
            self.band_minmax = {}
            self.force_band_minmax = {}

    def remove_all_lines(self):
        """
        Remove all tracklines from the display and layer manager
        """
        remlyrs = self.layer_manager.line_layer_names
        if remlyrs:
            for lyr in remlyrs:
                self.remove_layer(lyr)

    def layer_point_to_map_point(self, layer: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer],
                                 point: qgis_core.QgsPoint):
        """
        Transform the provided point in layer coordinates to map coordinates.  Layer is provided to get the CRS for
        the transformation

        Parameters
        ----------
        layer
            layer the point comes from
        point
            the point to transform

        Returns
        -------
        qgis_core.QgsPoint
                the transformed point
        """
        crs_src = layer.crs()
        crs_dest = self.crs
        transform_context = self.project.transformContext()
        xform = qgis_core.QgsCoordinateTransform(crs_src, crs_dest, transform_context)
        newpoint = xform.transform(point)
        self.debug_print(f'2dview layer_point_to_map_point: layerpoint={point} mappoint={newpoint}', logging.INFO)
        return newpoint

    def layer_extents_to_map_extents(self, layer: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer]):
        """
        Transform the provided layer's extents to the map extents, and return the extents

        Parameters
        ----------
        layer
            layer the extents come from

        Returns
        -------
        qgis_core.QgsRectangle
            the transformed extents
        """
        extnt = layer.extent()
        newmin = self.layer_point_to_map_point(layer, qgis_core.QgsPointXY(extnt.xMinimum(), extnt.yMinimum()))
        newmax = self.layer_point_to_map_point(layer, qgis_core.QgsPointXY(extnt.xMaximum(), extnt.yMaximum()))
        extnt = qgis_core.QgsRectangle(newmin, newmax)
        return extnt

    def map_point_to_layer_point(self, layer: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer],
                                 point: qgis_core.QgsPoint):
        """
        Transform the provided point in map coordinates to layer coordinates.  Layer is provided to get the CRS for
        the transformation

        Parameters
        ----------
        layer
            layer the point comes from
        point
            the point to transform

        Returns
        -------
        qgis_core.QgsPoint
                the transformed point
        """
        crs_src = self.crs
        crs_dest = layer.crs()
        transform_context = self.project.transformContext()
        xform = qgis_core.QgsCoordinateTransform(crs_src, crs_dest, transform_context)
        newpoint = xform.transform(point)
        self.debug_print(f'2dview map_point_to_layer_point: mappoint={point} layerpoint={newpoint}', logging.INFO)
        return newpoint

    def add_layer(self, source: str, layername: str, providertype: str, color: QtGui.QColor = None,
                  layertype: str = 'background'):
        """
        Generate the Qgs layer.  provider type specifies the driver to use to open the data.

        Parameters
        ----------
        source
            source str, generally a file path to the object/file
        layername
            layer name to use from the source data
        providertype
            one of ['gdal', 'wms', 'ogr']
        color
            optional, only used for vector layers, will set the color of that layer to the provided
        layertype
            corresponding to the layer_manager categories: line, background, surface, raster, vector, mesh.  Used to sort the layer draw order.

        Returns
        -------
        Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer]
            the created layer
        """
        self.debug_print(f'2dview add_layer: source={source} layername={layername} providertype={providertype} color={color}, layertype={layertype}', logging.INFO)

        if providertype in ['gdal', 'wms']:
            source, lyr, opacity, renderer = self._add_raster_layer(source, layername, providertype, layertype)
        elif providertype in ['ogr']:
            source, lyr, opacity, renderer = self._add_vector_layer(source, layername, providertype, color, layertype)
        elif providertype in ['mdal']:
            source, lyr, opacity, renderer = self._add_mesh_layer(source, layername, providertype, layertype)
        else:
            raise NotImplementedError('Only currently supporting gdal and ogr formats, found {}'.format(providertype))
        self._manager_add_layer(source, layername, providertype, color, layertype, lyr, opacity, renderer)
        return lyr

    def _update_global_layer_minmax(self, layername: str):
        """
        Update the global band_minmax attribute with the min and max of all layers that match the provided layername.

        Used when we remove a layer to get the new global minmax after that layer is removed.

        Parameters
        ----------
        layername
            layer name to use from the source data
        """

        for rlayer in self.layer_manager.surface_layers_by_type(layername):
            stats = rlayer.dataProvider().bandStatistics(1)
            minval = stats.minimumValue
            maxval = stats.maximumValue

            if layername in self.band_minmax:
                self.band_minmax[layername][0] = min(minval, self.band_minmax[layername][0])
                self.band_minmax[layername][1] = max(maxval, self.band_minmax[layername][1])
            else:
                self.band_minmax[layername] = [minval, maxval]

    def update_layer_minmax(self, layername: str):
        """
        Using the global band_minmax attribute, set the min/max of the band value for all raster layers that have the
        provided layername

        Parameters
        ----------
        layername
            layer name to use from the source data
        """

        if layername in self.force_band_minmax:
            minl, maxl = self.force_band_minmax[layername]
        elif layername in self.band_minmax:
            minl, maxl = self.band_minmax[layername]
        else:
            return
        for lname in self.layer_manager.surface_layer_names_by_type(layername):
            if layername == 'hillshade':
                continue
            old_lyr = self.layer_manager.layer_data_lookup[lname]
            # this would be ideal, but does not seem to work.  The triggerRepaint method does not acknowledge the change
            # renderer.shader().setMinimumValue(minl)
            # renderer.shader().setMaximumValue(maxl)
            old_lyr.renderer().setShader(RasterShader(minl, maxl, old_lyr.renderer().shader().color_scheme))
            self.layer_manager.set_layer_renderer(lname)  # no arguments, just to update the renderer we currently have cached

    def _create_raster_renderer(self, raster_layer: qgis_core.QgsRasterLayer, layername: str):
        """
        Build a new Renderer object to apply to a raster layer.  Allow for overriding the actual min/max of the layer
        with the globally set band_minmax, and use specific shaders/renderers depending on the layer name of the raster.

        Parameters
        ----------
        raster_layer
            qgis raster layer
        layername
            layer name to use from the source data

        Returns
        -------
        qgis._core.QgsRasterRenderer
            new raster renderer object
        str
            layername from the acceptedlayernames lookup
        """

        stats = raster_layer.dataProvider().bandStatistics(1)
        minval = stats.minimumValue
        maxval = stats.maximumValue
        # surface layers can be added in chunks, i.e. 'depth_1', 'depth_2', etc., but they should all use the same
        #  extents and global stats.  Figure out which category the layer fits into here.
        try:
            fixed_layername = layername.lower().replace(' ', '_')
            formatted_layername = [aln for aln in acceptedlayernames if fixed_layername.find(aln) > -1][0]
        except IndexError:
            formatted_layername = layername
        if formatted_layername in self.band_minmax:
            self.band_minmax[formatted_layername][0] = min(minval, self.band_minmax[formatted_layername][0])
            self.band_minmax[formatted_layername][1] = max(maxval, self.band_minmax[formatted_layername][1])
        else:
            self.band_minmax[formatted_layername] = [minval, maxval]
        if formatted_layername in invert_colormap_layernames:
            shadercolor = 'bluetored'
        else:
            shadercolor = 'redtoblue'
        if formatted_layername == 'hillshade':
            renderer = qgis_core.QgsHillshadeRenderer(raster_layer.dataProvider(), 1, 315, 45)
        else:
            renderer = qgis_core.QgsSingleBandPseudoColorRenderer(raster_layer.dataProvider(), 1,
                                                                  RasterShader(self.band_minmax[formatted_layername][0],
                                                                               self.band_minmax[formatted_layername][1],
                                                                               shadercolor))
        return renderer, formatted_layername

    def _add_raster_layer(self, source: str, layername: str, providertype: str, layertype: str):
        """
        Build the QgsRasterLayer for the provided source/layer.  We do some specific things based on surface layer
        names, for instance the 'vertical_uncertainty' layer gets an inverted shader.

        source needs to be either a path to a gdal supported file, a vsimem path, or an URI for wms data

        Parameters
        ----------
        source
            source str, generally a file path to the object/file
        layername
            layer name to use from the source data
        providertype
            one of ['gdal', 'wms']
        layertype
            corresponding to the layer_manager categories: line, background, surface, raster, vector, mesh.

        Returns
        -------
        str
            source str, modified for the raster driver to include layer name
        qgis_core.QgsRasterLayer
            raster layer
        float
            opacity
        qgis._core.QgsRasterRenderer
            renderer object
        """

        rlayer = qgis_core.QgsRasterLayer(source, layername, providertype)
        if rlayer.error().message():
            self.print("{} Layer failed to load!".format(layername), logging.ERROR)
            self.print(rlayer.error().message(), logging.ERROR)
            return None, None, None, None
        if providertype == 'gdal':
            try:
                renderer, formatted_layername = self._create_raster_renderer(rlayer, layername)
                rlayer.setRenderer(renderer)
                opacity = 1 - self.surface_transparency
                rlayer.renderer().setOpacity(opacity)
                self.update_layer_minmax(formatted_layername)
            except:
                self.print(f'2dview: Unable to generate custom renderer for {source}', logging.WARNING)
                opacity = 1
                renderer = rlayer.renderer()
        else:
            opacity = None
            renderer = None

        if layertype == 'raster':
            # we encode the source and layername together to find it easier later for raster, surface data uses the virtual raster
            #    which has this already in the source, ex: '/vsimem/tj_patch_test_2040_20220624_002634_depth_1_8.0.tif'
            source = self._generate_combined_source(source, layername)
        rlayer.setName(source)
        self.project.addMapLayer(rlayer, True)
        return source, rlayer, opacity, renderer

    def _add_vector_layer(self, source: str, layername: str, providertype: str, color: QtGui.QColor = None, layertype: str = 'line'):
        """
        Build the QgsVectorLayer from the source/layername.

        source needs to be either a path to an ogr supported file or a vsimem path

        Parameters
        ----------
        source
            source str, generally a file path to the object/file
        layername
            layer name to use from the source data
        providertype
            one of ['ogr']
        color
            optional, only used for vector layers, will set the color of that layer to the provided
        layertype
            corresponding to the layer_manager categories: line, background, surface, raster, vector, mesh.

        Returns
        -------
        qgis_core.QgsVectorLayer
        """

        if os.path.splitext(source)[1] in ['.000', '.s57']:
            # a quirk of the s57 driver, you need to pipe in the layername in the source
            source = source + '|layername=' + layername
        vlayer = qgis_core.QgsVectorLayer(source, layername, providertype)
        if vlayer.error().message():
            self.print("{} Layer failed to load!".format(source), logging.ERROR)
            self.print(vlayer.error().message(), logging.ERROR)
            return None, None, None, None
        renderer = None
        opacity = None
        if color:
            if vlayer.renderer():  # can only set color if there is data
                vlayer.renderer().symbol().setColor(color)
                renderer = vlayer.renderer()
            else:
                self.print('{} unable to set color'.format(source), logging.ERROR)
        if layertype == 'vector':
            # we encode the source and layername together to find it easier later for vector, lines use the virtual raster
            #    which has this already in the source
            source = self._generate_combined_source(source, layername)
            vlayer.setName(source)
        self.project.addMapLayer(vlayer, True)
        return source, vlayer, opacity, renderer

    def _add_mesh_layer(self, source: str, layername: str, providertype: str, layertype: str):
        """
        Build the QgsMeshLayer for the provided source/layer.

        source needs to be either a path to a mdal supported file, a vsimem path, or an URI for wms data

        Parameters
        ----------
        source
            source str, generally a file path to the object/file
        layername
            layer name to use from the source data
        providertype
            one of ['mdal']
        layertype
            corresponding to the layer_manager categories: line, background, surface, raster, vector, mesh.

        Returns
        -------
        str
            source str, modified for the raster driver to include layer name
        qgis_core.QgsMeshLayer
            mesh layer
        float
            opacity
        qgis._core.QgsMeshRenderer
            renderer object
        """

        mlayer = qgis_core.QgsMeshLayer(source, layername, providertype)
        if mlayer.error().message():
            self.print("{} Layer failed to load!".format(layername), logging.ERROR)
            self.print(mlayer.error().message(), logging.ERROR)
            return None, None, None, None
        opacity = None
        renderer = None

        source = self._generate_combined_source(source, layername)
        mlayer.setName(source)
        self.project.addMapLayer(mlayer, True)
        return source, mlayer, opacity, renderer

    def hide_layer(self, source: str):
        """
        hides the layer, see _manager_hide_layer
        """
        self._manager_hide_layer(source)

    def show_layer(self, source: str):
        """
        shows the layer, see _manager_show_layer
        """
        self._manager_show_layer(source)

    def remove_layer(self, source: str):
        """
        removes the layer, see _manager_remove_layer.  Also removes the data from the QgsProject/QgsMapCanvas
        """
        lyr = self.layer_by_name(source)
        self._manager_remove_layer(source)
        self.project.removeMapLayer(lyr)

    def layer_by_name(self, layername: str, silent: bool = False):
        """
        Returns the QgsMapLayer that corresponds to the provided layername

        Parameters
        ----------
        layername
            name of the layer
        silent
            if True, will not print a message

        Returns
        -------
        Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer]
            the layer found
        """
        if layername in self.layer_manager.layer_data_lookup:
            layer = self.layer_manager.layer_data_lookup[layername]
        else:
            layer = None
            if not silent:
                self.print('layer_by_name: Unable to find layer {}'.format(layername), logging.ERROR)
        return layer

    def change_line_colors(self, line_names: list, color: str):
        """
        Change the provided line names to the provided color

        Parameters
        ----------
        line_names
            list of line names to use as keys in the line objects dict
        color
            string color identifier, ex: 'r' or 'red'
        """

        newcolor = QtGui.QColor(color)
        lyrnames = self.layer_manager.line_layer_names
        for lyrname in lyrnames:
            lyr = self.layer_manager.layer_data_lookup[lyrname]
            if lyr.name() in line_names:
                self.layer_manager.set_layer_renderer(lyrname, color=newcolor)

    def reset_line_colors(self):
        """
        Reset all lines back to the default color
        """

        lyrnames = self.layer_manager.line_layer_names
        for lyrname in lyrnames:
            lyr = self.layer_manager.layer_data_lookup[lyrname]
            if lyr.renderer().symbol().color().name() == '#ff0000':  # red
                newcolor = QtGui.QColor('blue')
                self.layer_manager.set_layer_renderer(lyrname, color=newcolor)

    def set_extents_from_lines(self, subset_lines: list = None):
        """
        Set the maximum extent based on the line layer extents
        """
        lyrs = self.layer_manager.line_layers
        if subset_lines:
            lyrs = [l for l in lyrs if l.name() in subset_lines]
        total_extent = None
        for lyr in lyrs:
            extent = lyr.extent()
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
        self.debug_print(f'2dview set_extents_from_lines: lines={lyrs} extent={total_extent}', logging.INFO)
        if total_extent:
            self.canvas.zoomToFeatureExtent(total_extent)

    def set_extents_from_surfaces(self, subset_surf: str = None, resolution: float = None):
        """
        Set the maximum extent based on the surface layer extents
        """

        if subset_surf:
            for lyrname in acceptedlayernames:  # find the first loaded layer
                lyrs = self._return_all_surface_tiles(subset_surf, lyrname, resolution)  # get all tiles
                lyrs = [self.layer_by_name(lyr, silent=True) for lyr in lyrs]  # get the actual layer data for each tile layer
                if lyrs:
                    break
            if not lyrs:
                self.print('No layer loaded for {}'.format(subset_surf), logging.ERROR)
                return
        else:
            lyrs = self.layer_manager.surface_layers
        total_extent = None
        for lyr in lyrs:
            extent = self.layer_extents_to_map_extents(lyr)
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
        self.debug_print(f'2dview set_extents_from_surfaces: surfaces={lyrs} extent={total_extent}', logging.INFO)
        self.canvas.zoomToFeatureExtent(total_extent)

    def set_extents_from_rasters(self, subset_raster: str = None, layername: str = None):
        if subset_raster:
            lyr = None
            if layername:
                combname = self._generate_combined_source(subset_raster, layername)
                lyr = self.layer_by_name(combname, silent=True)
            else:
                for rlyr in self.layer_manager.raster_layers:
                    if rlyr.name().find(subset_raster) > -1:
                        lyr = rlyr
                        break
            if not lyr:
                self.print(f'No layer loaded for {subset_raster}', logging.ERROR)
                return
            lyrs = [lyr]
        else:
            lyrs = self.layer_manager.raster_layers
        total_extent = None
        for lyr in lyrs:
            extent = self.layer_extents_to_map_extents(lyr)
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
        self.debug_print(f'2dview set_extents_from_rasters: rasters={lyrs} extent={total_extent}', logging.INFO)
        self.canvas.zoomToFeatureExtent(total_extent)

    def set_extents_from_vectors(self, subset_vector: str = None, layername: str = None):
        if subset_vector:
            lyr = None
            if layername:
                combname = self._generate_combined_source(subset_vector, layername)
                lyr = self.layer_by_name(combname, silent=True)
            else:
                for rlyr in self.layer_manager.vector_layers:
                    if rlyr.name().find(subset_vector) > -1:
                        lyr = rlyr
                        break
            if not lyr:
                self.print(f'No layer loaded for {subset_vector}', logging.ERROR)
                return
            lyrs = [lyr]
        else:
            lyrs = self.layer_manager.vector_layers
        total_extent = None
        for lyr in lyrs:
            extent = self.layer_extents_to_map_extents(lyr)
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
        self.debug_print(f'2dview set_extents_from_vectors: vectors={lyrs} extent={total_extent}', logging.INFO)
        self.canvas.zoomToFeatureExtent(total_extent)

    def set_extents_from_meshes(self, subset_mesh: str = None, layername: str = None):
        if subset_mesh:
            lyr = None
            if layername:
                combname = self._generate_combined_source(subset_mesh, layername)
                lyr = self.layer_by_name(combname, silent=True)
            else:
                for rlyr in self.layer_manager.mesh_layers:
                    if rlyr.name().find(subset_mesh) > -1:
                        lyr = rlyr
                        break
            if not lyr:
                self.print(f'No layer loaded for {subset_mesh}', logging.ERROR)
                return
            lyrs = [lyr]
        else:
            lyrs = self.layer_manager.mesh_layers
        total_extent = None
        for lyr in lyrs:
            extent = self.layer_extents_to_map_extents(lyr)
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
        self.debug_print(f'2dview set_extents_from_meshes: meshs={lyrs} extent={total_extent}', logging.INFO)
        self.canvas.zoomToFeatureExtent(total_extent)

    def refresh_screen(self):
        """
        Refresh the screen by redrawing the canvas
        """
        self.canvas.refresh()

    def zoom_to_layer(self, layername: str):
        """
        Set the canvas extents to the provided layer extents

        Parameters
        ----------
        layername
            name of the layer
        """

        layer = self.layer_by_name(layername)
        if layer:
            extents = self.layer_extents_to_map_extents(layer)
            self.debug_print(f'2dview zoom_to_layer: layer={layername} extent={extents}', logging.INFO)
            self.canvas.zoomToFeatureExtent(extents)

    def clear(self):
        """
        Clears all data (except background data) from the Map
        """
        self.remove_all_surfaces()
        for layername, layertype in self.layer_manager.layer_type_lookup.items():
            if layertype == 'line':
                self.remove_line(layername)
        self.set_extent(90, -90, 180, -180, buffer=False)

    def zoomIn(self):
        """
        Activate the zoom in tool
        """
        self.canvas.setMapTool(self.toolZoomIn)

    def zoomOut(self):
        """
        Activate the zoom out tool
        """
        self.canvas.setMapTool(self.toolZoomOut)

    def pan(self):
        """
        Activate the pan tool
        """
        self.canvas.setMapTool(self.toolPan)

    def selectBox(self):
        """
        Activate the select tool
        """
        self.canvas.setMapTool(self.toolSelect)

    def selectPoints(self):
        """
        Activate the point select tool
        """
        self.clear_points()
        self.canvas.setMapTool(self.toolPoints)

    def finalize_points_tool(self):
        self.toolPoints.finalize()

    def query(self):
        """
        Activate the query tool
        """
        self.canvas.setMapTool(self.toolQuery)

    def distance(self):
        """
        Activate the distance tool
        """
        self.canvas.setMapTool(self.toolDistance)

    def clear_points(self):
        """
        switching off the 2d/3d points view tool or clearing the box with the tool will clear the already loaded data in the 3d view
        """
        self.turn_off_pointsview.emit(True)

    def take_screenshot(self):
        """
        Take a screenshot of the current map view, based off of
        https://www.geodose.com/2022/02/pyqgis-tutorial-automating-map-layout.html
        and
        https://github.com/MarcoDuiker/QGIS_QuickPrint/blob/d1c946a7b6187553c92ffad7a0cc23d39a1bc593/quick_print3.py
        """

        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='Save a new screenshot (supports png, pdf, jpeg)',
                                                         AppName='klusterproj', bMulti=False, bSave=True, DefaultFile="screenshot.png",
                                                         fFilter='png (*.png);;pdf (*.pdf);;jpeg (*.jpeg);;jpg (*.jpg)')
        if msg:
            self.print(f'Generating screenshot {fil}', logging.INFO)
            supported_extensions = ['.pdf', '.png', '.jpeg', '.jpg']
            fil_ext = os.path.splitext(fil)[1]
            if fil_ext not in supported_extensions:
                self.print(f'take_screenshot - {fil} file type not supported, must be one of {supported_extensions}', logging.ERROR)
                return

            project = self.project
            layout = qgis_core.QgsPrintLayout(project)
            layout.initializeDefaults()

            # start with the kluster template.  This allows us to layout the rough chart elements, in the proper position
            #  easily, so that we can then customize them
            templatefile = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'misc', 'kluster_qgis_print_template.qpt')
            with open(templatefile) as f:
                template_content = f.read()
            doc = QtXml.QDomDocument()
            doc.setContent(template_content)
            items, chk = layout.loadFromTemplate(doc, qgis_core.QgsReadWriteContext(), False)
            if not chk:
                self.print(f'Error loading from template file {templatefile}', logging.ERROR)
                return

            # get the layout map from the template load.  we need to setLayers, so that our loaded lines/grids show up.
            #  we also need to find the grids, which are in projected crs, and reproject to geographic so that they line
            #  up with the geographic lines/background.  Have to do this manually, we could use the qgis processing algorithms,
            #  but they are a pain in the ass to setup, and seem to cause issues on import.
            mymap = [itm for itm in items if isinstance(itm, qgis_core.QgsLayoutItemMap)]
            if not mymap or len(mymap) > 1:
                self.print(f'Unexpected item map found {mymap}', logging.ERROR)
                return
            mymap = mymap[0]
            drop_layers = []
            orig_layers = self.project.mapThemeCollection().masterVisibleLayers()  # should only include layers that are actually visible in the current zoom/extents
            for cnt, lyr in enumerate(orig_layers):  # get all the grid layers, warp to 4326
                if lyr.name().find('vsimem') != -1 and lyr.crs() != qgis_core.QgsCoordinateReferenceSystem(kluster_variables.qgis_epsg) and lyr in self.layer_manager.shown_layers:
                    newsrc = f'/vsimem/newsrc_{cnt}'
                    ds = gdal.Warp(newsrc, lyr.source(), format='GTiff', dstSRS=f"EPSG:{kluster_variables.qgis_epsg}")
                    newlyr = qgis_core.QgsRasterLayer(newsrc, '', 'gdal')
                    current_settings = self.layer_manager.layer_settings_lookup[lyr.source()]
                    if current_settings['renderer'] is not None:
                        newlyr.setRenderer(current_settings['renderer'])
                    if lyr.renderer() is not None:
                        newlyr.setRenderer(lyr.renderer())
                    formatted_layername = [aln for aln in acceptedlayernames if lyr.name().find(aln) > -1][0]
                    ds = None
                    self.project.addMapLayer(newlyr, True)
                    drop_layers.append([newlyr, formatted_layername])

            # now we build the layers to use in the screenshot.  First sort them in shown_layers order, keeping only visible layers
            final_layers = [x for x in self.layer_manager.shown_layers if x in self.project.mapThemeCollection().masterVisibleLayers()]
            # now take out the old surface layers (the non unprojected ones)
            final_layers = [lyr for lyr in final_layers if lyr not in self.layer_manager.surface_layers]
            # add in the new geographic versions, put them on top of course
            final_layers = [dlyr[0] for dlyr in drop_layers] + final_layers
            mymap.setLayers(final_layers)

            # screen might not have the same proportions as the paper, need to ensure that the trimming/growing of the screen
            #   to fit paper results in the image centered on the same spot, so we do it manually
            paper_proportion = layout.width() / layout.height()
            canvrec = self.canvas.extent()
            cancenter = canvrec.center()
            if canvrec.width() > canvrec.height():
                desired_canvas_height = canvrec.width() / paper_proportion
                desired_canvas_rec = qgis_core.QgsRectangle(qgis_core.QgsPointXY(canvrec.xMinimum(), cancenter.y() - desired_canvas_height / 2),
                                                            qgis_core.QgsPointXY(canvrec.xMaximum(), cancenter.y() + desired_canvas_height / 2))
            else:
                desired_canvas_width = canvrec.height() * paper_proportion
                desired_canvas_rec = qgis_core.QgsRectangle(qgis_core.QgsPointXY(cancenter.x() - desired_canvas_width / 2, canvrec.yMinimum()),
                                                            qgis_core.QgsPointXY(cancenter.x() + desired_canvas_width / 2, canvrec.yMaximum()))
            mymap.setExtent(desired_canvas_rec)

            # if we have grids, we need to use the legend
            mylegend = [itm for itm in items if isinstance(itm, qgis_core.QgsLayoutItemLegend)]
            if not mylegend or len(mylegend) > 1:
                self.print(f'Unexpected item legend found {mylegend}', logging.ERROR)
                return
            mylegend = mylegend[0]
            if drop_layers:  # ideally all layers have the same legend (min max being the same)
                droplayer, dropname = drop_layers[0]
                root = qgis_core.QgsLayerTree()  # override legend items with only the reprojected rasters
                root.addLayer(droplayer)
                # this is the only way I could figure out how to rename the band name
                root.children()[0].setCustomProperty("legend/label-0", dropname)
                mylegend.model().setRootGroup(root)
                # set the height of the color bar
                mylegend.setSymbolHeight(130.0)
                mylegend.updateLegend()
                # want the colorbar to go from shallow on top, to deep on the bottom.  Feel like this should do it, but it doesnt, strangely.
                if dropname == 'depth':
                    layer_node = root.findLayer(droplayer)
                    newsetts = qgis_core.QgsColorRampLegendNodeSettings()
                    newsetts.setDirection(qgis_core.QgsColorRampLegendNodeSettings.Direction.MaximumToMinimum)
                    qgis_core.QgsMapLayerLegendUtils.setLegendNodeColorRampSettings(layer_node, 0, newsetts)
                    mylegend.updateLegend()
            else:
                layout.removeLayoutItem(mylegend)

            pics = [itm for itm in items if isinstance(itm, qgis_core.QgsLayoutItemPicture)]
            # I like this arrow more than any of the prebuilt ones in qgis
            myarrow = [itm for itm in pics if itm.mode() == qgis_core.QgsLayoutItemPicture.FormatRaster]
            if not myarrow or len(myarrow) > 1:
                self.print(f'Unexpected item northarrow found {myarrow}', logging.ERROR)
                return
            myarrow = myarrow[0]
            myarrow.setPicturePath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'images', 'NorthArrow.png'))

            # fix logo path, since it depends on environment
            # mylogo = [itm for itm in pics if itm.mode() == qgis_core.QgsLayoutItemPicture.FormatRaster]
            # if not mylogo or len(mylogo) > 1:
            #     print(f'Unexpected item northarrow found {mylogo}')
            #     return
            # mylogo = mylogo[0]
            # mylogo.setPicturePath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'images', 'kluster_img.png'))

            # scale bar is a pain.  If we were to show it in degrees, it would be no issue.  To show in nautical miles, we need
            #  to calculate the screenshot width, convert units, and then build scale bar units accordingly
            myscale = [itm for itm in items if isinstance(itm, qgis_core.QgsLayoutItemScaleBar)]
            if not myscale or len(myscale) > 1:
                self.print(f'Unexpected item scalebar found {myscale}', logging.ERROR)
                return
            myscale = myscale[0]
            myextent = mymap.extent()
            distance = qgis_core.QgsDistanceArea()
            distance.setEllipsoid('WGS84')
            degree_width_meters = distance.measureLine(qgis_core.QgsPointXY(myextent.xMinimum(), myextent.yMinimum()), qgis_core.QgsPointXY(myextent.xMinimum() + 1, myextent.yMinimum()))
            screenshot_width_meters = distance.measureLine(qgis_core.QgsPointXY(myextent.xMinimum(), myextent.yMinimum()), qgis_core.QgsPointXY(myextent.xMaximum(), myextent.yMaximum()))
            meters_in_nm = 1852
            screenshot_width_in_nm = screenshot_width_meters / meters_in_nm
            # pick some nautical mile numbers, nearest one is used in deriving units/length of scale bar
            scale_sizes = [1000, 750, 500, 250, 100, 75, 50, 25, 10, 7.5, 5, 2.5, 1, 0.75, 0.5, 0.25, 0.1, 0.075, 0.05, 0.025, 0.01, 0.0075, 0.005, 0.0025, 0.001]
            found = False
            for scalesize in scale_sizes:
                if screenshot_width_in_nm > scalesize:
                    myscale.setMapUnitsPerScaleBarUnit((scalesize / 10) * meters_in_nm * (1 / degree_width_meters) / (scalesize / 10))
                    myscale.setUnitsPerSegment((scalesize / 10) * meters_in_nm * (1 / degree_width_meters))
                    myscale.setUnitLabel("NM")
                    found = True
                    break
            if not found:
                self.print(f'Warning: Unable to generate scale bar for screen width of {screenshot_width_in_nm} NM', logging.WARNING)

            if os.path.exists(fil):
                os.remove(fil)
            exporter = qgis_core.QgsLayoutExporter(layout)
            if fil_ext == '.pdf':
                exporter.exportToPdf(fil, qgis_core.QgsLayoutExporter.PdfExportSettings())
            else:
                exporter.exportToImage(fil, qgis_core.QgsLayoutExporter.ImageExportSettings())
            self.print(f'Screenshot saved to {fil}', logging.INFO)
            exporter = None
            mymap.setLayers([])
            mymap = None
            layout = None
            # make sure we get rid of our temporary warped surface layer
            for dlyr, dname in drop_layers:
                gdal.Unlink(dlyr.source())
                self.project.removeMapLayer(dlyr)


if __name__ == '__main__':
    app = qgis_core.QgsApplication([], True)
    app.initQgis()
    tst = MapView()
    tst.show()
    exitcode = app.exec_()
    app.exitQgis()
    sys.exit(exitcode)
