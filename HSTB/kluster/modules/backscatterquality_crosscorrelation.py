from HSTB.kluster.fqpr_convenience import perform_all_processing, reload_data, generate_new_mosaic
import glob
import copy
import numpy as np
import xarray as xr
from scipy.signal import correlate2d
import cv2
from osgeo import gdal
import matplotlib.pyplot as plt



fle_dir = r'D:\Backscatter\sample_dataset'
def make_mosaiscs(fle_dir):
    fles = glob.glob(fle_dir + '\\**\\*.all', recursive=True)
    bs_gradient_half = np.linspace(-80,10,200)
    bs_gradient = np.concatenate((bs_gradient_half, bs_gradient_half[::-1]))
    # fg = perform_all_processing(fles, skip_dask=True)

    converted_data = reload_data(fle_dir + '\\', skip_dask=True)
    generate_new_mosaic(converted_data, resolution=2, export_path= r"D:\Backscatter\sample_dataset\converted", use_dask=False)


    converted_data_bs_gradient = copy.deepcopy(converted_data)
    new_bs = -np.absolute(np.rad2deg(converted_data_bs_gradient.multibeam.raw_ping[0]['corr_pointing_angle'].values))
    converted_data_bs_gradient.multibeam.raw_ping[0]['backscatter'].values = new_bs

    # for mfile in multibeamfiles:
    #     linedata = converted_data_bs_gradient.subset_variables_by_line(['x', 'y', 'corr_pointing_angle', 'backscatter'],
    #                                                          line_names=mfile, filter_by_detection=True)
    #     data = linedata[mfile]
    #     #re-writing BS with angular value in degrees. Magnitude probably doesn't matter
    #     # for the cross-correlation, but this keeps it within a comparable dynamic range
    #     # to the expected reflectivity
    #     new_bs = -np.absolute(np.rad2deg(data['corr_pointing_angle']))
    #     data['backscatter'] = new_bs

    generate_new_mosaic(converted_data_bs_gradient, gridding_algorithm='shoalest', process_backscatter=False, resolution=2, angle_varying_gain=False, export_path= r"D:\Backscatter\sample_dataset\converted", use_dask=False)

def read_tif(tif_fle):
    img_gdal = gdal.Open(tif_fle, gdal.GA_ReadOnly)
    raster = img_gdal.GetRasterBand(1)
    img = raster.ReadAsArray()
    return img

def image_correlate(tif_backscatter, tif_angle_mosaic):
    img_backscatter = read_tif(tif_backscatter)
    img_angle_mosaic = read_tif(tif_angle_mosaic)
    img_area_mask = np.zeros(np.shape(img_angle_mosaic))
    img_area_mask = img_area_mask + img_angle_mosaic
    img_area_mask[img_area_mask < 0] = -30.0
    # img_cc = correlate2d(img1, img2)
    img_cc_angle_mosaic = cv2.filter2D(img_backscatter, ddepth=-1, kernel=img_angle_mosaic)
    img_cc_area_mask = cv2.filter2D(img_backscatter, ddepth=-1, kernel=img_area_mask)
    return img_cc_angle_mosaic, img_cc_area_mask

def filter_tester(img, kernel):
    plt.figure()
    img_cc = cv2.filter2D(img, ddepth=-1, kernel=kernel)
    plt.imshow(img_cc)

def pseudo_contour(img, bins):
    inds = np.digitize(img, bins)-1
    img_psuedo_contour = bins[inds]
    return img_psuedo_contour




tif_backscatter = r"D:\Backscatter\sample_dataset\FMGT_SampleProject.fmproj\Output\SD\FMGT_2m_Mosaic_Output_floatingpoint.tiff"
tif_angle_mosaic = r"D:\Backscatter\sample_dataset\converted_20240701_211121_2.0_1.tif"
img_am = read_tif(tif_angle_mosaic)
bins = np.arange(-70,5,15)

img_am_ps = pseudo_contour(img_am, bins)
plt.imshow(img_am_ps)
img_cc_angle_mosaic, img_cc_area_mask = image_correlate(tif_backscatter, tif_angle_mosaic)
# contour_test = gdal.ContourGenerate(img_angle_mosaic, 5, 0, [], 0, 0, contour_shp, 0, 1)
print('done')