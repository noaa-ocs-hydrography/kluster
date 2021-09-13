################## TEST RUN THROUGH ##################################

from HSTB.kluster.fqpr_convenience import *
import os

datasets = [r'C:\collab\dasktest\data_dir\EM122_RonBrown', r'C:\collab\dasktest\data_dir\EM710_Rainier', r'C:\collab\dasktest\data_dir\EM2040_BHII',
            r'C:\collab\dasktest\data_dir\EM2040_Fairweather_SmallFile', r'C:\collab\dasktest\data_dir\EM2040c_NRT2',
            r'C:\collab\dasktest\data_dir\EM2040p_UNH_ASV', r'C:\collab\dasktest\data_dir\hassler_acceptance\refsurf',
            r'C:\collab\dasktest\data_dir\Hasslerdual', r'C:\collab\dasktest\data_dir\tj_sbet_test\MBES\2904_2019_KongsbergEM2040\2019-216',
            r'C:\collab\dasktest\data_dir\tjacceptance\reference_surface', r'C:\collab\dasktest\data_dir\tjmotionlatency\mbes',
            r"C:\collab\dasktest\data_dir\from_Lund\0000_20191009_083350_Narwhal.all", r"C:\collab\dasktest\data_dir\from_Lund\0006_20161122_190914_Antares.all",
            r"C:\collab\dasktest\data_dir\from_Lund\0011_20190715_070305_MALASPINA.all", r"C:\collab\dasktest\data_dir\from_Lund\0014_20170420_014958_ShipName.all",
            r"C:\collab\dasktest\data_dir\from_Lund\0034_20171114_104317_Astrolabio.all", r"C:\collab\dasktest\data_dir\from_Lund\0090_20201202_141721_Narwhal.all",
            r"C:\collab\dasktest\data_dir\from_Lund\0154_20190530_060059_Hesperides.all", r"C:\collab\dasktest\data_dir\from_Lund\0389_20170602_094427_Astrolabio.all",
            r"C:\collab\dasktest\data_dir\from_Lund\0681_20201102_191746_TOFINO.all", r"C:\collab\dasktest\data_dir\from_Lund\EM2040-0007-t16-20181206-095359.all",
            r"C:\collab\dasktest\data_dir\from_Lund\0063_20210612_092103_MAL_EM2040MKII.kmall", r"C:\collab\dasktest\data_dir\from_Lund\0011_20210304_094901_EM2040P.kmall"]

datasets_w_sbets = [r'C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035',
                    r'C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Raw\MBES\S222_2020_KongsbergEM710\2020-077',
                    r'C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Raw\MBES\S222_2020_KongsbergEM2040\2020-077']
sbetfils = [[r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\sbet\export_2020_035_2801_A.out", None,
             r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\sbet\export_2020_035_2801_A.log", None, None, None],
            [r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM710\2020-077\S222_2020_KongsbergEM710_2020-077_NAD83_SBET.out",
             r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM710\2020-077\S222_2020_KongsbergEM710_2020-077_NAD83_smrmsg.out",
             None, 2020, 12, 'NAD83'],
            [r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM2040\2020-077\S222_2020_KongsbergEM2040_2020-077_NAD83_SBET.out",
             r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM2040\2020-077\S222_2020_KongsbergEM2040_2020-077_NAD83_smrmsg.out",
             None, 2020, 12, 'NAD83'],
            [r"C:\collab\dasktest\data_dir\val_kmall_patch\sbet_Mission 1.out",
             r"C:\collab\dasktest\data_dir\val_kmall_patch\smrms_Mission 1.out",
             None, 2019, 15, 'NAD83']
            ]

outputdir = r'C:\collab\dasktest\data_dir\outputtest'

fldernames = ['EM122_RonBrown', 'EM710_Rainier', 'EM2040_BHII', 'EM2040_Fairweather_SmallFile', 'EM2040c_NRT2', 'EM2040p_UNH_ASV',
              'hassler_acceptance', 'Hasslerdual', 'tj_sbet_test', 'tjacceptance', 'tjmotionlatency', 'narwhal', 'antares',
              'malaspina', 'shipname', 'astrolabio', 'narwhal2', 'hesperides', 'astrolabio2', 'tofino', 'lund_em2040', 'lund_em2040_kmall',
              'lund_em2040p']
for cnt, dset in enumerate(datasets):
    fq = perform_all_processing(dset, outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='WGS84', vert_ref='waterline')

fldernames = ['ra_mbes', 'tj_patch_test_710', 'tj_patch_test_2040']
for cnt, dset in enumerate(datasets_w_sbets):
    sbet, smrmsg, logf, yr, wk, dat = sbetfils[cnt]
    fq = perform_all_processing(dset, navfiles=[sbet], outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='NAD83',
                                vert_ref='ellipse', errorfiles=[smrmsg], logfiles=[logf], weekstart_year=yr,
                                weekstart_week=wk, override_datum=dat)
    generate_new_surface(fq, resolution=8.0, output_path=os.path.join(outputdir, fldernames[cnt]))
    # fq.export_pings_to_file()

sbet = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet.out"
logf = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet_export.log"
errorfil = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_smrmsg.out"
fq = perform_all_processing(r'C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035',
                            navfiles=[sbet], errorfiles=[errorfil], logfiles=[logf], vert_ref='ellipse',
                            outfold=r"C:\collab\dasktest\data_dir\outputtest\rambes35sbet")
fq.export_pings_to_file()

# concat test
fq_outoforder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0634_20180711_142125.all",
                                       outfold=r'C:\collab\dasktest\data_dir\outputtest\concatoutoforder')
fq_outoforder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0653_20180711_152950.all",
                                       outfold=r'C:\collab\dasktest\data_dir\outputtest\concatoutoforder')
fq_outoforder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0650_20180711_151518.all",
                                       outfold=r'C:\collab\dasktest\data_dir\outputtest\concatoutoforder')

fq_inorder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0634_20180711_142125.all",
                                    outfold=r'C:\collab\dasktest\data_dir\outputtest\concatinorder')
fq_inorder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0650_20180711_151518.all",
                                    outfold=r'C:\collab\dasktest\data_dir\outputtest\concatinorder')
fq_inorder = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\0653_20180711_152950.all",
                                    outfold=r'C:\collab\dasktest\data_dir\outputtest\concatinorder')

assert np.array_equal(fq_inorder.multibeam.raw_ping[0].z, fq_outoforder.multibeam.raw_ping[0].z)

###################### PATCH TEST GENERATOR ##########################

from fqpr_convenience import *
from fqpr_generation import *
from xarray_conversion import *
fq = reload_data(r"C:\collab\dasktest\data_dir\EM2040\converted")
xyzrph = fq.multibeam.xyzrph
subset_time = [[fq.multibeam.raw_ping[0].time.values[0], fq.multibeam.raw_ping[0].time.values[10]],
               [fq.multibeam.raw_ping[0].time.values[50], fq.multibeam.raw_ping[0].time.values[60]]]

fq, soundings = reprocess_sounding_selection(fq, new_xyzrph=xyzrph, subset_time=subset_time, turn_off_dask=True)
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.scatter3D(soundings[0], soundings[1], soundings[2])

##########################################################################################

from HSTB.drivers.kmall import *
import matplotlib.pyplot as plt

km = kmall(r"C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1\0000_20190411_175243_ShipName.kmall")
km.index_file()
SKMOffsets = [x for x, y in zip(km.msgoffset, km.msgtype) if y == "b'#SKM'"]
km.FID.seek(SKMOffsets[0])
dg = km.read_EMdgmSKM()
tme = dg['sample']['KMdefault']['dgtime']
roll = dg['sample']['KMdefault']['roll_deg']
km.closeFile()
plt.plot(tme)

###################################### iterate through finding the right offset ###############################

from fqpr_generation import *
from fqpr_convenience import *
for i in [-0.100,-0.030, -0.010, -0.005, 0.005, 0.010, 0.030, 0.100]:
    fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\nfwobble", skip_dask=True)
    fq.motion_latency = i
    fq.subset_by_time(1599981219.9734, 1599981274.5968)
    fq = process_multibeam(fq)
    fq.export_pings_to_file()

from copy import deepcopy
for i in [1, -1]:
    fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\nfwobble", skip_dask=True)
    workingdata = deepcopy(fq.multibeam.xyzrph)
    workingdata['tx_x']['1599980693'] = str(float(workingdata['tx_x']['1599980693']) + i)
    workingdata['rx_x']['1599980693'] += str(float(workingdata['rx_x']['1599980693']) + i)

    fq.multibeam.xyzrph = workingdata
    fq.subset_by_time(1599981219.9734, 1599981274.5968)
    fq = process_multibeam(fq, vert_ref='ellipse')
    fq.export_pings_to_file()

#################################### build github environment ######################################

# conda create -n kluster_test -y python=3.8.2
# conda activate kluster_test
# conda install -c conda-forge -y qgis=3.18.0 vispy=0.6.6 pyside2=5.13.2 gdal=3.2.1
# pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster

########################################### kluster intel test ##############################################

from HSTB.kluster.fqpr_project import *
from HSTB.kluster.fqpr_intelligence import *

if os.path.exists(r'C:\collab\dasktest\data_dir\new_project\kluster_project.json'):
    proj = open_project(r'C:\collab\dasktest\data_dir\new_project\kluster_project.json')
else:
    proj = create_new_project(r'C:\collab\dasktest\data_dir\new_project')
st = FqprIntel(proj)

st.add_file(r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM2040\2020-077\S222_2020_KongsbergEM2040_2020-077_NAD83_SBET.log")
st.add_file(r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM2040\2020-077\S222_2020_KongsbergEM2040_2020-077_NAD83_SBET.out")
st.add_file(r"C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Processed\SBET\S222_2020_KongsbergEM2040\2020-077\S222_2020_KongsbergEM2040_2020-077_NAD83_smrmsg.out")

st.start_folder_monitor(r'C:\collab\dasktest\data_dir\tj_patch_test', is_recursive=True)

####################################################################################
from HSTB.kluster.fqpr_convenience import *
from HSTB.kluster.gdal_helpers import *
from HSTB.kluster.fqpr_surface_v3 import *

surf = reload_surface(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710\grid")

output_raster = r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710\export.bag"
z_positive_up = True
individual_name = 'unknown'
organizational_name = 'unknown'
position_name = 'unknown'
attr_date = ''
vert_crs = ''
abstract = ''
process_step_description = ''
attr_datetime = ''
restriction_code = 'otherRestrictions'
other_constraints = 'unknown'
classification = 'unclassified'
security_user_note = 'none'

if not attr_date:
    attr_date = datetime.now().strftime('%Y-%m-%d')
if not attr_datetime:
    attr_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
if not process_step_description:
    process_step_description = 'Generated By GDAL {} and Kluster {}'.format(return_gdal_version(), kluster_version)
if not vert_crs:
    vert_crs = 'VERT_CS["unknown", VERT_DATUM["unknown", 2000]]'

bag_options = ['VAR_INDIVIDUAL_NAME=' + individual_name, 'VAR_ORGANISATION_NAME=' + organizational_name,
               'VAR_POSITION_NAME=' + position_name, 'VAR_DATE=' + attr_date, 'VAR_VERT_WKT=' + vert_crs,
               'VAR_ABSTRACT=' + abstract, 'VAR_PROCESS_STEP_DESCRIPTION=' + process_step_description,
               'VAR_DATETIME=' + attr_datetime, 'VAR_RESTRICTION_CODE=' + restriction_code,
               'VAR_OTHER_CONSTRAINTS=' + other_constraints, 'VAR_CLASSIFICATION=' + classification,
               'VAR_SECURITY_USER_NOTE=' + security_user_note]
nodatavalue = 1000000.0
data, geo_transform, bandnames = surf._gdal_preprocessing(nodatavalue=nodatavalue, z_positive_up=z_positive_up)
driver = 'BAG'
crs = surf.crs

driver = gdal.GetDriverByName(driver)
srs = pyproj_crs_to_osgeo(crs)

cols, rows = data[0].shape
no_bands = len(data)
dataset = driver.Create(output_raster, cols, rows, no_bands, gdal.GDT_Float32, bag_options)
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

#########################################################################################
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLandrew02_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif"
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLapalach01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLGAeastbays31_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLGAeastshelf41_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLjoseph03_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLpensac02_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLsoicw01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLsouth12_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLwest01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.tif" -co APPEND_SUBDATASET=YES

# gdal_translate "C:\vdatum_all_20201203\vdatum\FLwest01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif"
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLsouth12_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLsoicw01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLpensac02_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLjoseph03_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLGAeastshelf41_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLGAeastbays31_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLapalach01_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES
# gdal_translate "C:\vdatum_all_20201203\vdatum\FLandrew02_8301\mllw.gtx" "C:\vdatum_all_20201203\vdatum\chart_datum_depth_rev.tif" -co APPEND_SUBDATASET=YES

# gdalbuildvrt -input_file_list "C:\vdatum_all_20201203\vdatum\vrt_file_list.txt" "C:\vdatum_all_20201203\vdatum\chart_datum_depth.vrt"

###############################################################################################

from HSTB.kluster.fqpr_generation import *
from HSTB.kluster.fqpr_convenience import reload_data
from HSTB.kluster.modules.georeference import *


fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\EM2040_BHII")
self = fq
subset_time: list = None
prefer_pp_nav: bool = True
dump_data: bool = True
delete_futs: bool = True
vdatum_directory: str = None

self._validate_georef_xyz(subset_time, dump_data)
self.logger.info('****Georeferencing sound velocity corrected beam offsets****\n')
starttime = perf_counter()

self.logger.info('Using pyproj CRS: {}'.format(self.horizontal_crs.to_string()))

skip_dask = False
if self.client is None:  # small datasets benefit from just running it without dask distributed
    skip_dask = True

systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)

for s_cnt, system in enumerate(systems):
    ra = self.multibeam.raw_ping[s_cnt]
    sys_ident = ra.system_identifier
    self.logger.info('Operating on system serial number = {}'.format(sys_ident))
    self.initialize_intermediate_data(sys_ident, 'xyz')
    pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

    for applicable_index, timestmp, prefixes in system:
        self.logger.info('using installation params {}'.format(timestmp))
        z_offset = float(self.multibeam.xyzrph[prefixes[0] + '_z'][timestmp])
        idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
        data_for_workers = self._generate_chunks_georef(ra, idx_by_chunk, applicable_index, prefixes,
                                                        timestmp, z_offset, prefer_pp_nav, vdatum_directory)
        break
    break

sv_corr, alt, lon, lat, hdng, heave, wline, vert_ref, input_crs, horizontal_crs, z_offset, vdatum_directory = self.client.gather(data_for_workers[0])

#################################################################
import os
from HSTB.kluster.fqpr_convenience import reload_data, generate_new_surface
fqone = reload_data(r"C:\collab\dasktest\data_dir\new_project\em710_241_03_17_2020")
fqtwo = reload_data(r"C:\collab\dasktest\data_dir\new_project\em2040_40072_03_17_2020")
bg = generate_new_surface([fqone, fqtwo], resolution=16.0)

bg.remove_points('em710_241_03_17_2020_0')
bg.grid(resolution=16.0)
bg.plot()


################################################################
from HSTB.kluster.fqpr_convenience import reload_data
import numpy as np

fq = reload_data(r"C:\Pydro21_Dev\NOAA\site-packages\Python38\git_repos\hstb_kluster\test_data\em2040_40111_05_23_2017")
polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                    [-122.47771027, 47.78949665]])
x, y, z, tvu, rejected, pointtime, beam = fq.return_soundings_in_polygon(polygon)

###################################################################

from HSTB.kluster.modules.sat import accuracy_test
accuracy_test(r"C:\collab\dasktest\data_dir\EM2040_Fairweather_SmallFile\srgrid_mean_auto",
              r"C:\collab\dasktest\data_dir\EM2040_Fairweather_SmallFile\em2040_40111_05_23_2017",
              output_directory=r"C:\collab\dasktest\data_dir\EM2040_Fairweather_SmallFile\accuracy_test", show_plots=True)

######################################################################
import sys
import numpy as np

from vispy import scene, app

canvas = scene.SceneCanvas(keys='interactive', size=(600, 600), show=True)
grid = canvas.central_widget.add_grid(margin=10)
grid.spacing = 0

title = scene.Label("Plot Title", color='white')
title.height_max = 40
grid.add_widget(title, row=0, col=0, col_span=2)

yaxis = scene.AxisWidget(orientation='left',
                         axis_label='Y Axis',
                         axis_font_size=12,
                         axis_label_margin=50,
                         tick_label_margin=5)
yaxis.width_max = 80
grid.add_widget(yaxis, row=1, col=0)

xaxis = scene.AxisWidget(orientation='bottom',
                         axis_label='X Axis',
                         axis_font_size=12,
                         axis_label_margin=50,
                         tick_label_margin=5)

xaxis.height_max = 80
grid.add_widget(xaxis, row=2, col=1)

right_padding = grid.add_widget(row=1, col=2, row_span=1)
right_padding.width_max = 50

view = grid.add_view(row=1, col=1, border_color='white')
data = np.random.normal(size=(1000, 2))
data[0] = -10, -10
data[1] = 10, -10
data[2] = 10, 10
data[3] = -10, 10
data[4] = -10, -10
plot = scene.Line(data, parent=view.scene)
view.camera = 'panzoom'

xaxis.link_view(view)
yaxis.link_view(view)


if __name__ == '__main__' and sys.flags.interactive == 0:
    app.run()

#################################################

# trying out geohashes

from HSTB.kluster.modules.subset import filter_subset_by_polygon
from HSTB.kluster.fqpr_convenience import reload_data
import numpy as np

fq = reload_data(r"D:\falkor\fk005b_geohashtest\em710_225_09_17_2012")
test_poly = np.array([[-96.14630717,  27.85118748], [-96.10496133,  27.83500867], [-96.1036131 ,  27.83860396], [-96.14495893,  27.85478277]])
fq.return_soundings_in_polygon(test_poly)



from HSTB.kluster.fqpr_convenience import reload_data, process_multibeam
fq = reload_data(r"C:\collab\dasktest\data_dir\EM2040c_NRT2\em2045_20098_07_11_2018")
fq = process_multibeam(fq, only_this_line='0650_20180711_151518.all')