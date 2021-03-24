from HSTB.kluster.gdal_helpers import *


def get_testfile_paths():
    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    testshp = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'test_shapefile.shp')
    testtif = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'test_raster.tif')
    return testfile, testshp, testtif


def test_pyproj_crs_to_osgeo():
    test_crs = 4326
    test_crs_two = CRS.from_epsg(4326)

    assert isinstance(pyproj_crs_to_osgeo(test_crs), SpatialReference)
    assert pyproj_crs_to_osgeo(test_crs).ExportToProj4() == '+proj=longlat +datum=WGS84 +no_defs'
    assert pyproj_crs_to_osgeo(test_crs).GetName() == pyproj_crs_to_osgeo(test_crs_two).GetName()


def test_crs_to_osgeo():
    test_crs = 4326
    test_crs_two = CRS.from_epsg(4326)
    test_crs_three = '4326'

    assert isinstance(crs_to_osgeo(test_crs), SpatialReference)
    assert crs_to_osgeo(test_crs).ExportToProj4() == '+proj=longlat +datum=WGS84 +no_defs'
    assert crs_to_osgeo(test_crs).GetName() == crs_to_osgeo(test_crs_two).GetName()
    assert crs_to_osgeo(test_crs).GetName() == crs_to_osgeo(test_crs_three).GetName()


def test_return_gdal_version():
    # just make sure it works and you get a returned string, can't really test the content of the version number
    assert isinstance(return_gdal_version(), str)


def test_ogr_output_file_exists():
    testfile, testshp, testtif = get_testfile_paths()
    assert ogr_output_file_exists(testshp)
    assert not ogr_output_file_exists(testtif)


def test_gdal_output_file_exists():
    testfile, testshp, testtif = get_testfile_paths()
    assert not gdal_output_file_exists(testshp)
    assert gdal_output_file_exists(testtif)


def test_gdal_raster_create():
    testfile, testshp, testtif = get_testfile_paths()
    test_newtif = os.path.join(os.path.split(testtif)[0], 'newtif.tif')
    data = [np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
    geotransform = [0, 1, 0, 0, 0, -1]
    crs = 26917
    bandnames = ('depth',)

    ds = gdal_raster_create(test_newtif, data, geotransform, crs, bandnames=bandnames)
    assert not ds
    assert os.path.exists(test_newtif)

    ds = gdal.Open(test_newtif)
    band = ds.GetRasterBand(1)
    assert band.GetDescription() == 'depth'
    assert np.array_equal(ds.GetRasterBand(1).ReadAsArray(), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]).T)
    ds = None
    os.remove(test_newtif)
    assert not os.path.exists(test_newtif)


def test_vectorlayer_createpoint():
    vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
    vl.write_to_layer('pointtest', np.array([123.0, 456.0]), geom_type=ogr.wkbPoint)
    lyr = vl.get_layer_by_name('pointtest')
    ft = lyr.GetFeature(0)
    geom = ft.GetGeometryRef()

    assert geom.GetPoints() == [(123.0, 456.0)]
    geom = None
    ft = None
    lyr = None

    assert ogr_output_file_exists('/vsimem/tst.shp')
    vl.close()
    vl = None
    gdal.Unlink('/vsimem/tst.shp')


def test_vectorlayer_createline():
    vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
    vl.write_to_layer('linetest', np.array([[123.0, 456.0], [124.0, 457.0], [125.0, 458.0]]), geom_type=ogr.wkbLineString)
    lyr = vl.get_layer_by_name('linetest')
    ft = lyr.GetFeature(0)
    geom = ft.GetGeometryRef()

    assert geom.GetPoints() == [(123.0, 456.0), (124.0, 457.0), (125.0, 458.0)]
    geom = None
    ft = None
    lyr = None

    assert ogr_output_file_exists('/vsimem/tst.shp')
    vl.close()
    vl = None
    gdal.Unlink('/vsimem/tst.shp')


def test_vectorlayer_createpolygon():
    vl = VectorLayer('/vsimem/tst.shp', 'ESRI Shapefile', 26917, True, False)
    vl.write_to_layer('polytest', np.array([[0.0, 20.0], [20.0, 20.0], [20.0, 0.0], [0.0, 0.0], [0.0, 20.0]]), geom_type=ogr.wkbPolygon)
    lyr = vl.get_layer_by_name('polytest')
    ft = lyr.GetFeature(0)
    geom = ft.GetGeometryRef()

    assert geom.ExportToWkt() == 'POLYGON ((0 20,20 20,20 0,0 0,0 20))'
    geom = None
    ft = None
    lyr = None

    assert ogr_output_file_exists('/vsimem/tst.shp')
    vl.close()
    vl = None
    gdal.Unlink('/vsimem/tst.shp')
