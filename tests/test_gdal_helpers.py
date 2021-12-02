import os
import shutil
import unittest
import numpy as np
from osgeo import gdal, ogr
from osgeo.osr import SpatialReference
import tempfile
from pyproj import CRS

from HSTB.kluster.gdal_helpers import pyproj_crs_to_osgeo, crs_to_osgeo, return_gdal_version, ogr_output_file_exists, \
    gdal_output_file_exists, gdal_raster_create, VectorLayer


class TestGdalHelpers(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.clsFolder = os.path.join(tempfile.tempdir, 'TestGdalHelpers')
        try:
            os.mkdir(cls.clsFolder)
        except FileExistsError:
            shutil.rmtree(cls.clsFolder)
            os.mkdir(cls.clsFolder)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.clsFolder)

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
        test_newtif = os.path.join(tempfile.mkdtemp(dir=self.clsFolder), 'newtif.tif')
        data = [np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]

        ds = gdal_raster_create(test_newtif, data, [0, 1, 0, 0, 0, -1], 26917, bandnames=('depth',))
        assert not ds

        ds = gdal.Open(test_newtif)
        band = ds.GetRasterBand(1)
        assert band.GetDescription() == 'depth'
        assert np.array_equal(ds.GetRasterBand(1).ReadAsArray(), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]).T)

    def test_vectorlayer_createpoint(self):
        vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
        vl.write_to_layer('pointtest', np.array([123.0, 456.0]), geom_type=ogr.wkbPoint)
        lyr = vl.get_layer_by_name('pointtest')
        ft = lyr.GetFeature(0)
        geom = ft.GetGeometryRef()

        assert geom.GetPoints() == [(123.0, 456.0)]
        assert ogr_output_file_exists('/vsimem/tst.shp')
        vl.close()
        gdal.Unlink('/vsimem/tst.shp')

    def test_vectorlayer_createline(self):
        vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
        vl.write_to_layer('linetest', np.array([[123.0, 456.0], [124.0, 457.0], [125.0, 458.0]]),
                          geom_type=ogr.wkbLineString)
        lyr = vl.get_layer_by_name('linetest')
        ft = lyr.GetFeature(0)
        geom = ft.GetGeometryRef()

        assert geom.GetPoints() == [(123.0, 456.0), (124.0, 457.0), (125.0, 458.0)]
        assert ogr_output_file_exists('/vsimem/tst.shp')
        vl.close()
        gdal.Unlink('/vsimem/tst.shp')

    def test_vectorlayer_createpolygon(self):
        vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
        vl.write_to_layer('polytest', np.array([[0.0, 20.0], [20.0, 20.0], [20.0, 0.0], [0.0, 0.0], [0.0, 20.0]]),
                          geom_type=ogr.wkbPolygon)
        lyr = vl.get_layer_by_name('polytest')
        ft = lyr.GetFeature(0)
        geom = ft.GetGeometryRef()

        assert geom.ExportToWkt() == 'POLYGON ((0 20,20 20,20 0,0 0,0 20))'
        assert ogr_output_file_exists('/vsimem/tst.shp')
        vl.close()
        gdal.Unlink('/vsimem/tst.shp')
