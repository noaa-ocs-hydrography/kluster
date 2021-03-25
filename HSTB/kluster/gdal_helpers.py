from typing import Union
import numpy as np
import os

import osgeo
from osgeo import gdal, ogr
from osgeo.osr import SpatialReference
from pyproj.crs import CRS
from pyproj.enums import WktVersion


def pyproj_crs_to_osgeo(proj_crs: Union[CRS, int]):
    """
    Convert from the pyproj CRS object to osgeo SpatialReference

    See https://pyproj4.github.io/pyproj/stable/crs_compatibility.html

    Parameters
    ----------
    proj_crs
        pyproj CRS or an integer epsg code

    Returns
    -------
    SpatialReference
        converted SpatialReference
    """

    if isinstance(proj_crs, int):
        proj_crs = CRS.from_epsg(proj_crs)
    osr_crs = SpatialReference()
    if osgeo.version_info.major < 3:
        osr_crs.ImportFromWkt(proj_crs.to_wkt(WktVersion.WKT1_GDAL))
    else:
        osr_crs.ImportFromWkt(proj_crs.to_wkt())
    return osr_crs


def crs_to_osgeo(input_crs: Union[CRS, str, int]):
    """
    Take in a CRS in several formats and returns an osr SpatialReference for use with GDAL/OGR

    Supports pyproj CRS object, crs in Proj4 format, crs in Wkt format, epsg code as integer/string

    Parameters
    ----------
    input_crs
        input crs in one of the accepted forms

    Returns
    -------
    SpatialReference
        osr SpatialReference for the provided CRS
    """

    if isinstance(input_crs, CRS):
        crs = pyproj_crs_to_osgeo(input_crs)
    else:
        crs = SpatialReference()
        try:  # in case someone passes a str that is an epsg
            epsg = int(input_crs)
            err = crs.ImportFromEPSG(epsg)
            if err:
                raise ValueError('Error trying to ImportFromEPSG: {}'.format(epsg))
        except ValueError:  # a wkt or proj4 is provided
            err = crs.ImportFromWkt(input_crs)
            if err:
                err = crs.ImportFromProj4(input_crs)
                if err:
                    raise ValueError('{} is neither a valid Wkt or Proj4 string'.format(input_crs))
    return crs


def return_gdal_version():
    """
    Parse the gdal VersionInfo() output to make it make sense in terms of major.minor.hotfix convention

    '3000400' -> '3.0.4'

    Returns
    -------
    str
        gdal version
    """

    # vers = gdal.VersionInfo()
    # maj = vers[0:2]
    # if maj[1] == '0':
    #     maj = int(maj[0])
    # else:
    #     maj = int(maj)
    # min = vers[2:4]
    # if min[1] == '0':
    #     min = int(min[0])
    # else:
    #     min = int(min)
    # hfix = vers[4:8]
    # if hfix[2] == '0':
    #     if hfix[1] == '0':
    #         hfix = int(hfix[0])
    #     else:
    #         hfix = int(hfix[0:1])
    # else:
    #     hfix = int(hfix[0:2])
    # return '{}.{}.{}'.format(maj, min, hfix)

    # not sure how I got to the above answer, when you can just use gdal.__version__
    #  I think it was for an old version of GDAL?  Leaving it just in case
    return gdal.__version__


def ogr_output_file_exists(pth: str):
    """
    here we could just do os.path.exists, but we also support vsimem virtual file systems for gdal
    https://gdal.org/user/virtual_file_systems.html
    therefore we should just try a ogr open to see if the file path exists

    Parameters
    ----------
    pth
        path to the file you want to check

    Returns
    -------
    bool
        True if the file exists
    """

    openfil = ogr.Open(pth)
    if openfil is None:
        return False
    openfil = None
    return True


def gdal_output_file_exists(pth: str):
    """
    here we could just do os.path.exists, but we also support vsimem virtual file systems for gdal
    https://gdal.org/user/virtual_file_systems.html
    therefore we should just try a gdal open to see if the file path exists

    Parameters
    ----------
    pth
        path to the file you want to check

    Returns
    -------
    bool
        True if the file exists
    """
    openfil = gdal.Open(pth)
    if openfil is None:
        return False
    openfil = None
    return True


def gdal_raster_create(output_raster: str, data: list, geo_transform: list, crs: Union[CRS, int], nodatavalue: float = 1000000.0,
                       bandnames: tuple = (), driver: str = 'GTiff', transpose: bool = True, creation_options: list = []):
    """
    Build a gdal product from the provided data using the provided driver.  Can perform a Transpose on the provided
    data to align with GDAL/Image standards.

    Parameters
    ----------
    output_raster
        path to the output file we are writing here
    data
        list of numpy ndarrays, generally something like [2dim depth, 2dim uncertainty].  Can just be [2dim depth]
    geo_transform
        gdal geotransform for the raster [x origin, x pixel size, x rotation, y origin, y rotation, -y pixel size]
    crs
        pyproj CRS or an integer epsg code
    nodatavalue
        nodatavalue to use in raster
    bandnames
        list of string identifiers, should match the length of the data provided
    driver
        name of gdal driver to get, ex: 'GTiff'
    transpose
        if True, performs Transpose on the provided data
    creation_options
        list of gdal creation options, mostly used for BAG metadata
    """

    gdal_driver = gdal.GetDriverByName(driver)
    srs = pyproj_crs_to_osgeo(crs)

    if transpose:
        data = [d.T for d in data]
    rows, cols = data[0].shape
    no_bands = len(data)

    dataset = gdal_driver.Create(output_raster, cols, rows, no_bands, gdal.GDT_Float32, creation_options)
    dataset.SetGeoTransform(geo_transform)
    dataset.SetProjection(srs.ExportToWkt())

    for cnt, d in enumerate(data):
        rband = dataset.GetRasterBand(cnt + 1)
        if bandnames:
            rband.SetDescription(bandnames[cnt])
        rband.WriteArray(d)
        if driver != 'GTiff':
            rband.SetNoDataValue(nodatavalue)
    if driver == 'GTiff':  # gtiff driver wants one no data value for all bands
        dataset.GetRasterBand(1).SetNoDataValue(nodatavalue)
    if driver != 'MEM':  # MEM driver relies on you returning the dataset for use
        dataset = None
    return dataset


class VectorLayer:
    """
    Convert numpy arrays and metadata to OGR geometry to then generate OGR files.  Includes methods for the common
    geometry types like polygons, lines and points.  Would need to be expanded if there are types outside of these
    basic types.

    ex:

    | >>> vl = VectorLayer('C:\\collab\\dasktest\\data_dir\\SHAM_ERS\\tst.shp', 'ESRI Shapefile', 26917, False)
    | Creating new file C:\\collab\\dasktest\\data_dir\\SHAM_ERS\\tst.shp
    | >>> vl.write_to_layer('new_poly', np.array([[1116651.439379124, 637392.6969887456], [1188804.0108498496, 652655.7409537067], [1226730.3625203592, 637392.6969887456], [1188804.0108498496, 622467.6640211721]]), ogr.wkbPolygon)
    | Successfully generated 1 out of 1 features in layer new_poly
    | >>> vl.close()

    Or write to virtual file system object (using gpkg driver for variety, is not driver specific)
    vl = VectorLayer('/vsimem/tst.gpkg', 'GPKG', 26917, False)
    """

    def __init__(self, output_file: str, driver_name: str, input_crs: Union[CRS, str, int], update: bool, silent: bool = True):
        self.output_file = output_file
        self.driver_name = driver_name
        self.driver = ogr.GetDriverByName(driver_name)
        self.silent = silent
        if not self.driver:
            raise ValueError('Provided driver name {} is not a valid ogr driver'.format(driver_name))

        self.update = update
        output_file_exists = ogr_output_file_exists(output_file)

        self.ds = None
        if output_file_exists:
            self._print_with_silent('Opening existing file {}, update={}'.format(self.output_file, self.update))
            self.ds = self.driver.Open(output_file, int(self.update))
        else:
            self._print_with_silent('Creating new file {}'.format(self.output_file))
            self.ds = self.driver.CreateDataSource(output_file)
        if self.ds is None:
            raise ValueError('Unable to create data source for {} using {} driver'.format(output_file, driver_name))
        self.crs = crs_to_osgeo(input_crs)

        self.hidden_layers = {}

    @property
    def is_virtual(self):
        """
        Returns if the output_file is a gdal virtual file system path
        """
        return self.output_file.find('vsimem') != -1

    def _print_with_silent(self, msg):
        if not self.silent:
            print(msg)

    def _2dim_geom(self, geom_type: int, coords: np.ndarray):
        """
        Generate new geometry for 2dim data, add the points and return

        Parameters
        ----------
        geom_type
            integer for one of the geom enumerations, ogr.wkbLinearRing for example
        coords
            2dim array of coords, ex: array([1116651.439379124, 637392.6969887456], [1188804.0108498496, 652655.7409537067])

        Returns
        -------
        ogr.Geometry
            geometry for the provided data
        """

        if coords.ndim != 2:
            raise ValueError('Must have multiple coordinates for 2dim geom, dim='.format(coords.shape))
        geom = ogr.Geometry(geom_type)
        [geom.AddPoint_2D(float(coord[0]), float(coord[1])) for coord in coords]
        return geom

    def _create_point(self, coord: np.ndarray):
        """
        Build a new point Geometry

        Parameters
        ----------
        coord
            1dim array of coords, ex: array([1116651.439379124, 637392.6969887456])

        Returns
        -------
        ogr.Geometry
            geometry for the provided data
        """

        if coord.ndim != 1:
            raise ValueError('Coordinates for point must be one dimensional, dim='.format(coord.shape))
        geom = ogr.Geometry(ogr.wkbPoint)
        geom.AddPoint_2D(coord[0], coord[1])
        return geom

    def _create_linestring(self, coords: np.ndarray):
        """
        Build a new Linestring Geometry

        Parameters
        ----------
        coords
            2dim array of coords, ex: array([[1116651.439379124, 637392.6969887456], [1188804.0108498496, 652655.7409537067]])

        Returns
        -------
        ogr.Geometry
            geometry for the provided data
        """

        ls = self._2dim_geom(ogr.wkbLineString, coords)
        return ls

    def _create_polygon(self, coords: np.ndarray):
        """
        Build a new polygon Geometry

        Parameters
        ----------
        coords
            2dim array of coords, ex: array([1116651.439379124, 637392.6969887456], [1188804.0108498496, 652655.7409537067])

        Returns
        -------
        ogr.Geometry
            geometry for the provided data
        """

        ring = self._2dim_geom(ogr.wkbLinearRing, coords)
        # Create polygon
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly

    def get_layer_by_name(self, layer_name: str):
        lyr = self.ds.GetLayer(layer_name)
        return lyr

    def write_to_layer(self, layer_name: str, coords_dset: np.ndarray, geom_type: int):
        """
        Build the requested geometry and write to layer_name.  If that layer exists, will update as long as the
        update switch is on.

        Parameters
        ----------
        layer_name
            name of the dataset layer
        coords_dset
            either a numpy array of coords for the feature, or a list of numpy arrays if there are multiple features
            you are writing to the layer
        geom_type
            The geometry type of the layer
        """

        if isinstance(coords_dset, np.ndarray):
            coords_dset = [coords_dset]

        # seems as though you have to have a layer in there equal to the file name.  If you don't do this, you end up
        #   with a layer equal to the file name regardless of what you name you pass into createlayer.
        default_layer = os.path.splitext(os.path.split(self.output_file)[1])[0]
        # create this default layer if it is not in there already and you aren't trying to do it anyway
        if not self.ds.GetLayerByName(default_layer) and default_layer != layer_name:
            self._print_with_silent('Initializing new file...')
            lyr = self.ds.CreateLayer(default_layer, self.crs, geom_type)
            lyr = None

        lyr = self.ds.GetLayer(layer_name)
        if lyr is not None:
            if self.update and lyr.GetGeomType() != geom_type:  # found a layer of that name, which is fine with update, just make sure the geom matches
                raise ValueError('Provided geometry type {} does not match layer {} which is {}'.format(geom_type, layer_name, lyr.GetGeomType()))
            elif not self.update:
                raise ValueError('Layer {} exists already, update must be enabled to update the layer'.format(layer_name))
            self._print_with_silent('Updating layer {}'.format(layer_name))
        else:
            lyr = self.ds.CreateLayer(layer_name, self.crs, geom_type)
            self._print_with_silent('Creating new layer {}'.format(layer_name))

        success_count = 0
        for coords in coords_dset:
            feat = ogr.Feature(lyr.GetLayerDefn())
            if geom_type == ogr.wkbPolygon:
                geom = self._create_polygon(coords)
            elif geom_type == ogr.wkbLineString:
                geom = self._create_linestring(coords)
            elif geom_type == ogr.wkbPoint:
                geom = self._create_point(coords)
            else:
                raise ValueError('Unrecognized geom_type {}'.format(geom_type))
            feat.SetGeometry(geom)
            err = lyr.CreateFeature(feat)
            if not err:
                success_count += 1
            feat = None
        lyr = None
        self._print_with_silent('Successfully written {} out of {} features in layer {}'.format(len(coords_dset), success_count, layer_name))

    def delete_layer(self, layer_name: str):
        """
        Delete the layer from the dataset

        Parameters
        ----------
        layer_name
            name of the dataset layer
        """
        lyr = self.ds.GetLayer(layer_name)
        if lyr is not None:
            self.ds.DeleteLayer(layer_name)
        else:
            self._print_with_silent('{} does not exist in lines data, cannot remove line')

    def hide_layer(self, layer_name: str):
        if layer_name in self.hidden_layers:
            raise ValueError('hide_layer: layer is already in hidden layers: {}'.format(layer_name))
        lyr = self.ds.GetLayer(layer_name)
        if lyr is not None:
            self.hidden_layers[layer_name] = lyr
            self.ds.DeleteLayer(layer_name)
        else:
            self._print_with_silent('{} does not exist in lines data, cannot remove line')

    def show_layer(self, layer_name: str):
        if layer_name not in self.hidden_layers:
            raise ValueError('show_layer: layer is not in hidden layers: {}'.format(layer_name))
        lyr = self.hidden_layers[layer_name]
        self.ds.CopyLayer(layer_name, layer_name)

    def close(self):
        """
        Close the dataset
        """

        self.ds = None
