from typing import Union
import numpy as np

import osgeo
from osgeo import gdal
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


def return_gdal_version():
    vers = gdal.VersionInfo()
    maj = vers[0:2]
    if maj[1] == '0':
        maj = int(maj[0])
    else:
        maj = int(maj)
    min = vers[2:4]
    if min[1] == '0':
        min = int(min[0])
    else:
        min = int(min)
    hfix = vers[4:8]
    if hfix[2] == '0':
        if hfix[1] == '0':
            hfix = int(hfix[0])
        else:
            hfix = int(hfix[0:1])
    else:
        hfix = int(hfix[0:2])
    return '{}.{}.{}'.format(maj, min, hfix)


def gdal_create(output_raster: str, data: list, geo_transform: list, crs: CRS, nodatavalue: float = 1000000.0,
                bandnames: tuple = (), driver: str = 'GTiff', creation_options: list = []):
    gdal_driver = gdal.GetDriverByName(driver)
    srs = pyproj_crs_to_osgeo(crs)

    cols, rows = data[0].shape
    no_bands = len(data)
    dataset = gdal_driver.Create(output_raster, cols, rows, no_bands, gdal.GDT_Float32, creation_options)
    dataset.SetGeoTransform(geo_transform)
    dataset.SetProjection(srs.ExportToWkt())

    for cnt, d in enumerate(data):
        rband = dataset.GetRasterBand(cnt + 1)
        if bandnames:
            rband.SetDescription(bandnames[cnt])
        rband.WriteArray(d.T)
        if driver != 'GTiff':
            rband.SetNoDataValue(nodatavalue)
    if driver == 'GTiff':  # gtiff driver wants one no data value for all bands
        dataset.GetRasterBand(1).SetNoDataValue(nodatavalue)
    dataset = None
