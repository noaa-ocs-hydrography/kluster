import os, sys
import numpy as np
from typing import Union
from pyproj import CRS
from osgeo import gdal

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled, found_path
if not qgis_enabled:
    raise EnvironmentError('Unable to find qgis directory in {}'.format(found_path))
from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui
from HSTB.kluster import __file__ as klusterdir

from HSTB.kluster.gdal_helpers import gdal_raster_create, VectorLayer, gdal_output_file_exists, ogr_output_file_exists
from HSTB.kluster import kluster_variables


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
        for name, layer in self.parent.project.mapLayers().items():
            if layer.type() == qgis_core.QgsMapLayerType.RasterLayer:
                if layer.dataProvider().name() != 'wms':
                    try:
                        layer_point = self.parent.map_point_to_layer_point(layer, point)
                        ident = layer.dataProvider().identify(layer_point, qgis_core.QgsRaster.IdentifyFormatValue)
                        if ident:
                            lname = layer.name()
                            if lname[0:8] == '/vsimem/':
                                lname = lname[8:]
                            text += '\n\n{}'.format(lname)
                            for ky, val in ident.results().items():
                                text += '\n{}: {}'.format(layer.bandName(ky), round(val, 3))
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

    def __init__(self, canvas):
        self.canvas = canvas
        qgis_gui.QgsMapToolEmitPoint.__init__(self, self.canvas)
        self.rubberBand = qgis_gui.QgsRubberBand(self.canvas, True)
        self.rubberBand.setColor(QtCore.Qt.black)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
        self.rubberBand.setWidth(1)

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
        self.third_click = False
        self.reset()

    def reset(self):
        """
        Clear the rectangle
        """
        self.rubberBand.setColor(QtCore.Qt.black)
        self.rubberBand.setFillColor(QtCore.Qt.transparent)
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

    def keyPressEvent(self, e):
        ctrl_pressed = e.key() == 16777249
        if ctrl_pressed:
            self.enable_rotation = True

    def keyReleaseEvent(self, e):
        ctrl_released = e.key() == 16777249
        if ctrl_released:
            self.enable_rotation = False

    def return_azimuth(self, start_x, start_y, end_x, end_y):
        delta_x = end_x - start_x
        az = np.arctan2((np.sin(delta_x) * np.cos(end_y)),
                        (np.cos(start_y) * np.sin(end_y) - np.sin(start_y) * np.cos(end_y) * np.cos(delta_x)))
        return az

    def canvasPressEvent(self, e):
        """
        Lay down the start point of the rectangle and reset the end point to the start point.
        """

        left_click = e.button() == 1
        right_click = e.button() == 2
        if left_click:  # first click sets the origin of the rectangle
            if not self.first_click and not self.second_click and not self.third_click:
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
                self.rubberBand.setColor(QtCore.Qt.green)
                self.rubberBand.setFillColor(QtCore.Qt.transparent)
                self.rubberBand.update()
                poly, az = self.rectangle()
                if poly is not None:
                    self.select.emit(poly, az)
        if right_click:  # clear the rectangle
            self.reset()

    def canvasMoveEvent(self, e):
        """
        On moving the mouse cursor, the rectangle continuously updates
        """
        if not self.isEmittingPoint and not self.enable_rotation:
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
            az = self.return_azimuth(np.deg2rad(start_point.x()), np.deg2rad(start_point.y()),
                                     np.deg2rad(end_point.x()), np.deg2rad(end_point.y()))
            if not self.start_azimuth:
                self.start_azimuth = az
            self.azimuth = az - self.start_azimuth

            point1 = qgis_core.QgsPointXY(self.final_start_point.x(), self.final_start_point.y())
            point2 = qgis_core.QgsPointXY(self.final_start_point.x(), self.final_end_point.y())
            point3 = qgis_core.QgsPointXY(self.final_end_point.x(), self.final_end_point.y())
            point4 = qgis_core.QgsPointXY(self.final_end_point.x(), self.final_start_point.y())
            center_pixel = qgis_core.QgsPointXY(((point3.x() - point1.x()) / 2) + point1.x(),
                                                ((point3.y() - point1.y()) / 2) + point1.y())
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
            point1 = self.toMapCoordinates(QtCore.QPoint(point1.x(), point1.y()))
            point2 = self.toMapCoordinates(QtCore.QPoint(point2.x(), point2.y()))
            point3 = self.toMapCoordinates(QtCore.QPoint(point3.x(), point3.y()))
            point4 = self.toMapCoordinates(QtCore.QPoint(point4.x(), point4.y()))

        self.rubberBand.reset(qgis_core.QgsWkbTypes.PolygonGeometry)
        self.rubberBand.addPoint(point1, False)
        self.rubberBand.addPoint(point2, False)
        self.rubberBand.addPoint(point3, False)
        self.rubberBand.addPoint(point4, True)  # true to update canvas

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
        az = self.azimuth
        return polygon, az

    def deactivate(self):
        """
        Deactivate the map tool
        """
        self.reset()
        qgis_gui.QgsMapTool.deactivate(self)
        self.deactivated.emit()


def raster_shader(lyrmin: float, lyrmax: float):
    """
    Use the provided minimum/maximum layer value to build a color ramp for rendering surface tifs.  We don't have the
    ability in Kluster to pick a color ramp, we just give them this one.

    Parameters
    ----------
    lyrmin
        minimum value for this band
    lyrmax
        maximum value for this band

    Returns
    -------
    qgis_core.QgsRasterShader
        Return a new raster shader with the built color ramp from band min to max
    """

    fcn = qgis_core.QgsColorRampShader()
    fcn.setColorRampType(qgis_core.QgsColorRampShader.Interpolated)
    diff = (lyrmax - lyrmin) / 6
    lst = [qgis_core.QgsColorRampShader.ColorRampItem(lyrmin, QtGui.QColor(255, 0, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + diff, QtGui.QColor(255, 165, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 2 * diff, QtGui.QColor(255, 255, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 3 * diff, QtGui.QColor(0, 128, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 4 * diff, QtGui.QColor(0, 0, 255)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 5 * diff, QtGui.QColor(75, 0, 130)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 6 * diff, QtGui.QColor(238, 130, 238))]
    fcn.setColorRampItemList(lst)
    shader = qgis_core.QgsRasterShader()
    shader.setRasterShaderFunction(fcn)
    return shader


def inv_raster_shader(lyrmin: float, lyrmax: float):
    """
    Use the provided minimum/maximum layer value to build a color ramp for rendering surface tifs.  The inverted
    shader is used in Kluster to visualize uncertainty, as we probably want red values to be high uncertainty (bad = hot)

    Parameters
    ----------
    lyrmin
        minimum value for this band
    lyrmax
        maximum value for this band

    Returns
    -------
    qgis_core.QgsRasterShader
        Return a new raster shader with the built color ramp from band min to max
    """
    fcn = qgis_core.QgsColorRampShader()
    fcn.setColorRampType(qgis_core.QgsColorRampShader.Interpolated)
    diff = (lyrmax - lyrmin) / 6
    lst = [qgis_core.QgsColorRampShader.ColorRampItem(lyrmin, QtGui.QColor(238, 130, 238)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + diff, QtGui.QColor(75, 0, 130)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 2 * diff, QtGui.QColor(0, 0, 255)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 3 * diff, QtGui.QColor(0, 128, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 4 * diff, QtGui.QColor(255, 255, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 5 * diff, QtGui.QColor(255, 165, 0)),
           qgis_core.QgsColorRampShader.ColorRampItem(lyrmin + 6 * diff, QtGui.QColor(255, 0, 0))]
    fcn.setColorRampItemList(lst)
    shader = qgis_core.QgsRasterShader()
    shader.setRasterShaderFunction(fcn)
    return shader


class LayerManager:
    """
    Manage the layer order and what layers are currently loaded in QgsMapCanvas.  As layers are added, the order is
    maintained in the names in order attribute.  The layers currently shown are maintained in the shown layers index.
    """
    def __init__(self):
        self.layer_data_lookup = {}
        self.layer_type_lookup = {}
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
        # always render surfaces on top of lines on top of background layers
        lyr_names = surf_lyr_names + line_lyr_names + bground_lyr_names
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

        return [lname for lname in self.surface_layer_names if lname.find('_{}_'.format(layertype)) != -1]

    def add_layer(self, layername: str, layerdata: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer], 
                  layertype: str):
        """
        Add new layer to the layer manager class.  We populate the lookups for data/type/name.
        
        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        layerdata
            the qgs layer object for that layer
        layertype
            one of line, background, surface
        """
        
        if layername not in self.names_in_order:
            self.layer_data_lookup[layername] = layerdata
            self.layer_type_lookup[layername] = layertype
            self.names_in_order.append(layername)
        else:
            print('Cant add layer {}, already in layer manager'.format(layername))

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
                print('show_layer: layer already in shown layers')
        else:
            print('show layer: layer not loaded')

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
                print('hide_layer: layer not in shown layers')
        else:
            print('hide layer: layer not loaded')

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
            self.names_in_order.remove(layername)
        else:
            print('remove_layer: layer not loaded')

    def return_layers_by_type(self, search_type: str):
        """
        Return all qgs layer objects for the provided layer type

        Parameters
        ----------
        search_type
            one of line, background, surface

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
            one of line, background, surface

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


class MapView(QtWidgets.QMainWindow):
    """
    The main window holding the QgsMapCanvas object (self.canvas) that renders all data.  We build a QMainWindow so that
    we have access to toolbars to control the QgsMapTools (zoom, distance, etc.).  The map canvas will by default be in
    WGS84 and all imported data will be transformed to this coordinate system.

    Kluster2dView will use this widget if Kluster uses the _qgs backend
    """

    box_select = Signal(float, float, float, float)
    box_3dpoints = Signal(object, float)
    box_swath = Signal(object, float)

    def __init__(self, parent=None, settings=None, epsg: int = 4326):
        super().__init__()
        self.epsg = epsg
        self.vdatum_directory = None
        self.layer_background = 'Default'
        self.layer_transparency = 0.5
        self.surface_transparency = 0
        self.background_data = {}
        self.band_minmax = {}
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
        self.layer_manager = LayerManager()

        # initialize the background layer with the default options (or the ones we have been provided in the optional settings kwarg)
        self.set_background(self.layer_background, self.layer_transparency, self.surface_transparency)

        self.toolSelect.select.connect(self._area_selected)
        self.toolPoints.select.connect(self._points_selected)
        self.toolSwath.select.connect(self._swath_selected)
        self.set_extent(90, -90, 180, -180, buffer=False)

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
        self.actionPoints = QtWidgets.QAction("3d Points")
        self.actionPoints.setToolTip('Select georeferenced points within an area to view in 3d using Points View tab.\n\n' + rectangle_instructions)
        self.actionSwath = QtWidgets.QAction("2d Points")
        self.actionSwath.setToolTip('Select georeferenced points within an area to view in 2d using Points View tab.\n\n' + rectangle_instructions)

        self.actionZoomIn.setCheckable(True)
        self.actionZoomOut.setCheckable(True)
        self.actionPan.setCheckable(True)
        self.actionSelect.setCheckable(True)
        self.actionQuery.setCheckable(True)
        self.actionDistance.setCheckable(True)
        self.actionPoints.setCheckable(True)
        self.actionSwath.setCheckable(True)

        self.actionZoomIn.triggered.connect(self.zoomIn)
        self.actionZoomOut.triggered.connect(self.zoomOut)
        self.actionPan.triggered.connect(self.pan)
        self.actionSelect.triggered.connect(self.selectBox)
        self.actionQuery.triggered.connect(self.query)
        self.actionDistance.triggered.connect(self.distance)
        self.actionPoints.triggered.connect(self.selectPoints)
        self.actionSwath.triggered.connect(self.selectSwath)

        self.toolbar = self.addToolBar("Canvas actions")
        self.toolbar.addAction(self.actionZoomIn)
        self.toolbar.addAction(self.actionZoomOut)
        self.toolbar.addAction(self.actionPan)
        self.toolbar.addAction(self.actionSelect)
        self.toolbar.addAction(self.actionQuery)
        self.toolbar.addAction(self.actionDistance)
        self.toolbar.addAction(self.actionPoints)
        self.toolbar.addAction(self.actionSwath)

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
        self.toolSwath = RectangleMapTool(self.canvas)
        self.toolSwath.setAction(self.actionSwath)

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
        """
        Build the urls for the layers of the NOAA electronic chart

        Returns
        -------
        list
            list of urls for each layer of the noaa enc
        """
        urls = []
        lyrs = [0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        for lyr in lyrs:
            url = 'https://gis.charttools.noaa.gov/arcgis/rest/services/MCS/ENCOnline/MapServer/exts/MaritimeChartService/WMSServer'
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

        self.box_select.emit(min_lat, max_lat, min_lon, max_lon)

    def _points_selected(self, polygon: np.ndarray, azimuth: float):
        """
        emit box_3dpoints signal when the Rectbox select tool is used, displays the points within the boundary in
        3d viewer.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (longitude, latitude) in degrees
        azimuth
            azimuth of the selection polygon in radians
        """

        self.box_3dpoints.emit(polygon, azimuth)

    def _swath_selected(self, polygon: np.ndarray, azimuth: float):
        """
        emit box_swath signal when the Rectbox select tool is used, displays the swaths within the boundary in
        3d viewer.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (longitude, latitude) in degrees
        azimuth
            azimuth of the selection polygon in radians
        """

        self.box_swath.emit(polygon, azimuth)

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

        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
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
                    lyr.setOpacity(1 - self.layer_transparency)
                else:
                    print('QGIS Initialize: Unable to find background layer: {}'.format(bpath))
            else:
                print('QGIS Initialize: Unable to find background layer: {}'.format(bpath))

    def _init_vdatum_extents(self):
        """
        Set the background to the vdatum kml files that signify the extents of vdatum coverage
        """
        if not self.vdatum_directory:
            print('Unable to find vdatum directory, please make sure you set the path in File - Settings'.format(self.vdatum_directory))
        else:
            for lname in self.layer_manager.background_layer_names:
                self.remove_layer(lname)
            background_dir = os.path.join(os.path.dirname(klusterdir), 'background')
            bpath = os.path.join(background_dir, 'GSHHS_L1.shp')
            if os.path.exists(bpath):
                lyr = self.add_layer(bpath, 'GSHHS_L1', 'ogr', QtGui.QColor.fromRgb(125, 100, 45, 150))
                if lyr:
                    lyr.setOpacity(1 - self.layer_transparency)
                else:
                    print('QGIS Initialize: Unable to find background layer: {}'.format(bpath))
            else:
                print('QGIS Initialize: Unable to find background layer: {}'.format(bpath))

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
                    symb = qgis_core.QgsSimpleLineSymbolLayer.create({'color': 'blue'})
                    lyr.renderer().symbol().changeSymbolLayer(0, symb)
                    lyr.setOpacity(1 - self.layer_transparency)
                else:
                    print('QGIS Initialize: Unable to find background layer: {}'.format(kmlf))
            print('Loaded {} VDatum kml files'.format(len(kmlfiles)))
            if not len(kmlfiles):
                print('Unable to find any kml files in all subdirectories at {}'.format(self.vdatum_directory))

    def _init_openstreetmap(self):
        """
        Set the background to the WMS openstreetmap service
        """

        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        url_with_params = self.wms_openstreetmap_url()
        lyr = self.add_layer(url_with_params, 'OpenStreetMap', 'wms')
        if lyr:
            lyr.renderer().setOpacity(1 - self.layer_transparency)
        else:
            print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _init_satellite(self):
        """
        Set the background to the google provied satellite imagery
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        url_with_params = self.wms_satellite_url()
        lyr = self.add_layer(url_with_params, 'Satellite', 'wms')
        if lyr:
            lyr.renderer().setOpacity(1 - self.layer_transparency)
        else:
            print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _init_noaa_rnc(self):
        """
        Set the background to the NOAA RNC WMS service
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        url_with_params = self.wms_noaa_rnc()
        lyr = self.add_layer(url_with_params, 'NOAA_RNC', 'wms')
        if lyr:
            lyr.renderer().setOpacity(1 - self.layer_transparency)
        else:
            print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _init_noaa_enc(self):
        """
        Set the background to the NOAA ENC service
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        urls = self.wms_noaa_enc()
        for cnt, url_with_params in enumerate(urls):
            lyr = self.add_layer(url_with_params, 'NOAA_ENC_{}'.format(cnt), 'wms')
            if lyr:
                lyr.renderer().setOpacity(1 - self.layer_transparency)
            else:
                print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _init_gebco(self):
        """
        Set the background to the Gebco latest Grid Shaded Relief WMS
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        url_with_params = self.wms_gebco()
        lyr = self.add_layer(url_with_params, 'GEBCO', 'wms')
        if lyr:
            lyr.renderer().setOpacity(1 - self.layer_transparency)
        else:
            print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _init_emodnet(self):
        """
        Set the background to the Gebco latest Grid Shaded Relief WMS
        """
        for lname in self.layer_manager.background_layer_names:
            self.remove_layer(lname)
        url_with_params = self.wms_emodnet()
        lyr = self.add_layer(url_with_params, 'EMODNET', 'wms')
        if lyr:
            lyr.renderer().setOpacity(1 - self.layer_transparency)
        else:
            print('QGIS Initialize: Unable to find background layer: {}'.format(url_with_params))

    def _manager_add_layer(self, layername: str, layerdata: Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer],
                           layertype: str):
        """
        Add the layer to the layer manager, set the layer to 'shown' in the layer manager, and add the layer to the
        shown layers in the QgsMapCanvas (see setLayers)

        Parameters
        ----------
        layername
            name of the layer, generally a file path or vsimem path
        layerdata
            the qgs layer object for that layer
        layertype
            one of line, background, surface
        """

        if layername not in self.layer_manager.layer_data_lookup:
            self.layer_manager.add_layer(layername, layerdata, layertype)
        self._manager_show_layer(layername)

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
        newname = '{}_{}_{}.tif'.format(os.path.splitext(surfname)[0], lyrname, resolution)
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

        print('Initializing {} with transparency of {}%'.format(layername, int(transparency * 100)))
        self.layer_background = layername
        self.layer_transparency = transparency
        self.surface_transparency = surf_transparency
        if self.layer_background == 'None':
            self._init_none()
        if self.layer_background == 'Default':
            self._init_default_layers()
        if self.layer_background == 'VDatum Coverage (VDatum required)':
            self._init_vdatum_extents()
        elif self.layer_background == 'OpenStreetMap (internet required)':
            self._init_openstreetmap()
        elif self.layer_background == 'Satellite (internet required)':
            self._init_satellite()
        elif self.layer_background == 'NOAA RNC (internet required)':
            self._init_noaa_rnc()
        elif self.layer_background == 'NOAA ENC (internet required)':
            self._init_noaa_enc()
        elif self.layer_background == 'GEBCO Grid (internet required)':
            self._init_gebco()
        elif self.layer_background == 'EMODnet Bathymetry (internet required)':
            self._init_emodnet()

        for lyr in self.layer_manager.surface_layers:
            lyr.renderer().setOpacity(1 - self.surface_transparency)

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
        self.canvas.setExtent(qgis_core.QgsRectangle(qgis_core.QgsPointXY(min_lon, min_lat),
                                                     qgis_core.QgsPointXY(max_lon, max_lat)))

    def add_line(self, line_name: str, lats: np.ndarray, lons: np.ndarray, refresh: bool = False):
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
        """
        source = self.build_line_source(line_name)
        if ogr_output_file_exists(source):
            # raise ValueError('Line {} already exists in this map view session'.format(line_name))
            return

        vl = VectorLayer(source, 'ESRI Shapefile', self.epsg, False)
        vl.write_to_layer(line_name, np.stack([lons, lats], axis=1), 2)  # ogr.wkbLineString
        vl.close()
        lyr = self.add_layer(source, line_name, 'ogr', QtGui.QColor('blue'), layertype='line')
        if refresh:
            lyr.reload()

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
        source = self.build_line_source(line_name)
        hidelyr = ogr_output_file_exists(source)
        if hidelyr:
            self.hide_layer(source)
        if refresh:
            self.layer_by_name(source).reload()

    def show_line(self, line_name, refresh=False):
        """
        Show the line so that it is displayed, if it was hidden

        Parameters
        ----------
        line_name
            name of the multibeam line
        refresh
            optional screen refresh, True most of the time, unless you want to remove multiple lines and then refresh
            at the end
        """
        source = self.build_line_source(line_name)
        showlyr = ogr_output_file_exists(source)
        if showlyr:
            self.show_layer(source)
        if refresh:
            self.layer_by_name(source).reload()

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

        if not showlyr:
            gdal_raster_create(source, data, geo_transform, crs, np.nan, (lyrname,))
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

        source = self.build_surface_source(surfname, lyrname, resolution)
        hidelyr = gdal_output_file_exists(source)
        if hidelyr:
            self.hide_layer(source)

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

        source = self.build_surface_source(surfname, lyrname, resolution)
        showlyr = gdal_output_file_exists(source)
        if showlyr:
            self.show_layer(source)

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

        possible_layers = ['depth', 'vertical_uncertainty']
        for lyr in possible_layers:
            source = self.build_surface_source(surfname, lyr, resolution)
            remlyr = gdal_output_file_exists(source)
            if remlyr:
                self.remove_layer(source)

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
            corresponding to the layer_manager categories: background, line, surface.  Used to sort the layer draw order.

        Returns
        -------
        Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer]
            the created layer
        """

        if providertype in ['gdal', 'wms']:
            lyr = self._add_raster_layer(source, layername, providertype)

        elif providertype in ['ogr']:
            lyr = self._add_vector_layer(source, layername, providertype, color)
        else:
            raise NotImplementedError('Only currently supporting gdal and ogr formats, found {}'.format(providertype))
        self._manager_add_layer(source, lyr, layertype)
        return lyr

    def _update_all_raster_layer_minmax(self, layername: str):
        for lname in self.layer_manager.surface_layer_names_by_type(layername):
            old_lyr = self.layer_manager.layer_data_lookup[lname]
            if layername == 'vertical_uncertainty':
                shader = inv_raster_shader(self.band_minmax[layername][0], self.band_minmax[layername][1])
            else:
                shader = raster_shader(self.band_minmax[layername][0], self.band_minmax[layername][1])
            old_lyr.renderer().setShader(shader)

    def _add_raster_layer(self, source: str, layername: str, providertype: str):
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

        Returns
        -------
        qgis_core.QgsRasterLayer
        """
        rlayer = qgis_core.QgsRasterLayer(source, layername, providertype)
        if rlayer.error().message():
            print("{} Layer failed to load!".format(layername))
            print(rlayer.error().message())
            return
        if providertype == 'gdal':
            stats = rlayer.dataProvider().bandStatistics(1)
            minval = stats.minimumValue
            maxval = stats.maximumValue
            if layername in self.band_minmax:
                self.band_minmax[layername][0] = min(minval, self.band_minmax[layername][0])
                self.band_minmax[layername][1] = max(maxval, self.band_minmax[layername][1])
            else:
                self.band_minmax[layername] = [minval, maxval]
            if layername == 'vertical_uncertainty':
                shader = inv_raster_shader
            else:
                shader = raster_shader
            self._update_all_raster_layer_minmax(layername)
            renderer = qgis_core.QgsSingleBandPseudoColorRenderer(rlayer.dataProvider(), 1, shader(self.band_minmax[layername][0],
                                                                                                   self.band_minmax[layername][1]))
            rlayer.setRenderer(renderer)
            rlayer.renderer().setOpacity(1 - self.surface_transparency)
        rlayer.setName(source)
        self.project.addMapLayer(rlayer, True)
        return rlayer

    def _add_vector_layer(self, source: str, layername: str, providertype: str, color: QtGui.QColor = None):
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

        Returns
        -------
        qgis_core.QgsVectorLayer
        """
        vlayer = qgis_core.QgsVectorLayer(source, layername, providertype)
        if vlayer.error().message():
            print("{} Layer failed to load!".format(source))
            print(vlayer.error().message())
            return
        if color:
            if vlayer.renderer():  # can only set color if there is data
                vlayer.renderer().symbol().setColor(color)
            else:
                print('{} unable to set color'.format(source))
        self.project.addMapLayer(vlayer, True)
        return vlayer

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

    def layer_by_name(self, layername: str):
        """
        Returns the QgsMapLayer that corresponds to the provided layername

        Parameters
        ----------
        layername
            name of the layer

        Returns
        -------
        Union[qgis_core.QgsRasterLayer, qgis_core.QgsVectorLayer]
            the layer found
        """
        if layername in self.layer_manager.layer_data_lookup:
            layer = self.layer_manager.layer_data_lookup[layername]
        else:
            layer = None
            print('layer_by_name: Unable to find layer {}'.format(layername))
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
        color = QtGui.QColor(color)
        lyrs = self.layer_manager.line_layers
        for lyr in lyrs:
            if lyr.name() in line_names:
                lyr.renderer().symbol().setColor(color)
                lyr.triggerRepaint()

    def reset_line_colors(self):
        """
        Reset all lines back to the default color
        """

        lyrs = self.layer_manager.line_layers
        for lyr in lyrs:
            lyr.renderer().symbol().setColor(QtGui.QColor('blue'))
            lyr.triggerRepaint()

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
        self.canvas.zoomToFeatureExtent(total_extent)

    def set_extents_from_surfaces(self, subset_surf: str = None):
        """
        Set the maximum extent based on the surface layer extents
        """

        if subset_surf:
            subset_surf = self.build_surface_source(subset_surf, 'depth')
            lyr = self.layer_by_name(subset_surf)
            if lyr is None:
                subset_surf = self.build_surface_source(subset_surf, 'vertical_uncertainty')
                lyr = self.layer_by_name(subset_surf)
                if lyr is None:
                    return
            lyrs = [lyr]
        else:
            lyrs = self.layer_manager.surface_layers
        total_extent = None
        for lyr in lyrs:
            extent = self.layer_extents_to_map_extents(lyr)
            if total_extent is None:
                total_extent = extent
            else:
                total_extent.combineExtentWith(extent)
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
            self.canvas.zoomToFeatureExtent(extents)

    def clear(self):
        """
        Clears all data (except background data) from the Map
        """
        for layername, layertype in self.layer_manager.layer_type_lookup.items():
            if layertype == 'line':
                self.remove_line(layername)
            elif layertype == 'surface':
                self.remove_surface(layername)
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
        self.canvas.setMapTool(self.toolPoints)

    def selectSwath(self):
        """
        Activate the point select tool
        """
        self.canvas.setMapTool(self.toolSwath)

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


if __name__ == '__main__':
    app = qgis_core.QgsApplication([], True)
    app.initQgis()
    tst = MapView()
    tst.show()
    exitcode = app.exec_()
    app.exitQgis()
    sys.exit(exitcode)
