import os
import unittest
import numpy as np
from osgeo import gdal, ogr
from osgeo.osr import SpatialReference

from HSTB.kluster.gdal_helpers import pyproj_crs_to_osgeo, crs_to_osgeo, return_gdal_version, ogr_output_file_exists, \
    gdal_output_file_exists, gdal_raster_create, VectorLayer
from pyproj import CRS

#TODO: ??
class TestGdalHelpers(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.vector_file = '/vsimem/tst.shp'

    def setUp(self) -> None:
        self.vl = VectorLayer(self.vector_file, 'ESRI Shapefile', 26917, True, False)

    def tearDown(self) -> None:
        self.vl.close()
        gdal.Unlink(self.vector_file)

    def get_geom(self, type: str, arr: np.array, geom_type: ogr):
        self.vl.write_to_layer(type, arr, geom_type)
        lyr = self.vl.get_layer_by_name(type)
        ft = lyr.GetFeature(0)
        return ft.GetGeometryRef()

    def test_pyproj_crs_to_osgeo(self):
        test_crs = 4326
        assert isinstance(pyproj_crs_to_osgeo(test_crs), SpatialReference)
        assert pyproj_crs_to_osgeo(test_crs).ExportToProj4() == '+proj=longlat +datum=WGS84 +no_defs'
        assert pyproj_crs_to_osgeo(test_crs).GetName() == pyproj_crs_to_osgeo(CRS.from_epsg(4326)).GetName()

    def test_crs_to_osgeo(self):
        test_crs = 4326
        assert isinstance(crs_to_osgeo(test_crs), SpatialReference)
        assert crs_to_osgeo(test_crs).ExportToProj4() == '+proj=longlat +datum=WGS84 +no_defs'
        assert crs_to_osgeo(test_crs).GetName() == crs_to_osgeo(CRS.from_epsg(4326)).GetName()
        assert crs_to_osgeo(test_crs).GetName() == crs_to_osgeo('4326').GetName()

    def test_return_gdal_version(self):
        # just make sure it works and you get a returned string, can't really test the content of the version number
        assert isinstance(return_gdal_version(), str)

    def test_gdal_output_file_exists(self):
        testshp = os.path.join(os.path.dirname(__file__), 'resources', 'test_shapefile.shp')
        testtif = os.path.join(os.path.dirname(__file__), 'resources', 'test_raster.tif')

        assert ogr_output_file_exists(testshp)
        assert not ogr_output_file_exists(testtif)
        assert not gdal_output_file_exists(testshp)
        assert gdal_output_file_exists(testtif)

    def test_gdal_raster_create(self):
        test_newtif = os.path.join(os.path.split(self.testtif)[0], 'newtif.tif')
        data = [np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]

        ds = gdal_raster_create(test_newtif, data, [0, 1, 0, 0, 0, -1], 26917, bandnames=('depth',))
        assert not ds
        assert os.path.exists(test_newtif)

        ds = gdal.Open(test_newtif)
        band = ds.GetRasterBand(1)
        assert band.GetDescription() == 'depth'
        assert np.array_equal(ds.GetRasterBand(1).ReadAsArray(), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]).T)
        os.remove(test_newtif)
        assert not os.path.exists(test_newtif)

    def test_vectorlayer_createpoint(self):
        geom = self.get_geom('pointtest', np.array([123.0, 456.0]), ogr.wkbPoint)
        assert geom.GetPoints() == [(123.0, 456.0)]
        assert ogr_output_file_exists(self.vector_file)

    def test_vectorlayer_createline(self):
        geom = self.get_geom('linetest', np.array([[123.0, 456.0], [124.0, 457.0], [125.0, 458.0]]), ogr.wkbLineString)
        assert geom.GetPoints() == [(123.0, 456.0), (124.0, 457.0), (125.0, 458.0)]
        assert ogr_output_file_exists(self.vector_file)

    def test_vectorlayer_createpolygon(self):
        geom = self.get_geom('polytest', np.array([[0.0, 20.0], [20.0, 20.0], [20.0, 0.0], [0.0, 0.0], [0.0, 20.0]]),
                             ogr.wkbPolygon)
        assert geom.ExportToWkt() == 'POLYGON ((0 20,20 20,20 0,0 0,0 20))'
        assert ogr_output_file_exists(self.vector_file)
