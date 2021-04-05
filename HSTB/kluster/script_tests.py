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
            r"C:\collab\dasktest\data_dir\from_Lund\0681_20201102_191746_TOFINO.all", r"C:\collab\dasktest\data_dir\from_Lund\EM2040-0007-t16-20181206-095359.all"]

datasets_w_sbets = [r'C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035',
                    r'C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Raw\MBES\S222_2020_KongsbergEM710\2020-077',
                    r'C:\collab\dasktest\data_dir\tj_patch_test\S222_PatchTest_DN077\Raw\MBES\S222_2020_KongsbergEM2040\2020-077',
                    r'C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1']
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
              'malaspina', 'shipname', 'astrolabio', 'narwhal2', 'hesperides', 'astrolabio2', 'tofino', 'lund_em2040']
for cnt, dset in enumerate(datasets):
    fq = perform_all_processing(dset, outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='WGS84', vert_ref='waterline')
    # generate_new_surface(fq, resolution=2.0, output_path=os.path.join(outputdir, fldernames[cnt], 'surf.npz'))
    fq.export_pings_to_file(export_by_identifiers=False)

fldernames = ['ra_mbes', 'tj_patch_test_710', 'tj_patch_test_2040', 'val_kmall_patch']
for cnt, dset in enumerate(datasets_w_sbets):
    sbet, smrmsg, logf, yr, wk, dat = sbetfils[cnt]
    fq = perform_all_processing(dset, navfiles=[sbet], outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='NAD83',
                                vert_ref='ellipse', errorfiles=[smrmsg], logfiles=[logf], weekstart_year=yr,
                                weekstart_week=wk, override_datum=dat)
    # generate_new_surface(fq, resolution=2.0, output_path=os.path.join(outputdir, fldernames[cnt], 'surf.npz'))
    fq.export_pings_to_file()

sbet = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet.out"
logf = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet_export.log"
errorfil = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_smrmsg.out"
fq = perform_all_processing(r'C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035',
                            navfiles=[sbet], errorfiles=[errorfil], logfiles=[logf], vert_ref='ellipse',
                            outfold=r"C:\collab\dasktest\data_dir\outputtest\rambes35sbet")


sbet = r"C:\collab\dasktest\data_dir\val_kmall_patch\sbet_Mission 1.out"
smrmsg = r"C:\collab\dasktest\data_dir\val_kmall_patch\smrms_Mission 1.out"
fq = perform_all_processing(r'C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1',
                            navfiles=[sbet], vert_ref='waterline', errorfiles=[smrmsg], weekstart_year=2019,
                            weekstart_week=15, override_datum='WGS84')

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

plt.plot(tme)

############################## accuracy tests ###################################
# from fqpr_sat import accuracy_test
# output_directory = r"C:\collab\dasktest\data_dir\outputtest\acc_test_screengrabs"
# reference_surface = r"C:\collab\dasktest\data_dir\outputtest\hassler_refsurf.npz"
# linepairs = [r"C:\collab\dasktest\data_dir\outputtest\hassler_400_long_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_400_short_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_400_long_cw",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_400_short_cw",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_300_long_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_300_short_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_300_long_cw",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_300_short_cw",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_200_long_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_200_short_fm",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_200_long_cw",
#              r"C:\collab\dasktest\data_dir\outputtest\hassler_200_short_cw"]
# accuracy_test(reference_surface, linepairs, vert_ref='waterline', output_directory=output_directory)

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
# conda install -c conda-forge -y vispy=0.6.4 pyside2=5.13.2 gdal=3.2.1 qgis=3.18.0
# pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster
# pip install git+https://github.com/noaa-ocs-hydrography/drivers.git#egg=hstb.drivers
# pip install git+https://github.com/noaa-ocs-hydrography/shared.git#egg=hstb.shared

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

########################################## working on sv 2 #####################################

from fqpr_generation import *
from fqpr_convenience import *

fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\shipname")
self = fq
subset_time = None

systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
for s_cnt, system in enumerate(systems):
    ra = self.multibeam.raw_ping[s_cnt]
    sys_ident = ra.system_identifier
    self.logger.info('Operating on system serial number = {}'.format(sys_ident))
    self.initialize_intermediate_data(sys_ident, 'sv_corr')
    pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()
    profnames, casts, cast_times, castlocations = self.multibeam.return_all_profiles()

    for applicable_index, timestmp, prefixes in system:
        self.logger.info('using installation params {}'.format(timestmp))
        idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
        if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
            cast_chunks = self.return_cast_idx_nearestintime(cast_times, idx_by_chunk)
            addtl_offsets = self.return_additional_xyz_offsets(ra, prefixes, timestmp, idx_by_chunk)
            data_for_workers = self._generate_chunks_svcorr(ra, casts, cast_chunks, applicable_index, prefixes, timestmp, addtl_offsets)
            break
    break

cast, beam_azimuth, beam_angle, two_way_travel_time, surface_sound_speed, z_waterline_offset, additional_offsets = data_for_workers[0][0].result(), data_for_workers[0][1].result()[0], data_for_workers[0][1].result()[1], data_for_workers[0][2].result(), data_for_workers[0][3].result(), data_for_workers[0][4], data_for_workers[0][5].result()

######################################################################################################
from fqpr_surface_v3 import *
from time import perf_counter
from HSTB.kluster.fqpr_convenience import *

fq = reload_data(r"C:\Users\eyou1\Downloads\em2040_40224_02_15_2021", skip_dask=True)

# x = np.random.uniform(538900, 539300, 1000).astype(np.float32)
# y = np.random.uniform(5292800, 5293300, 1000).astype(np.float32)
# z = np.random.uniform(30, 35, 1000).astype(np.float32)
#
# test_data_arr = np.stack([x, y, z], axis=1)
# test_data_arr = test_data_arr.ravel().view(dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4')])

raw_ping = fq.multibeam.raw_ping[0]
dataset = raw_ping.drop_vars([nms for nms in raw_ping.variables if nms not in ['x', 'y', 'z', 'tvu', 'thu']])
dataset = dataset.isel(time=slice(0, 3600)).stack({'sounding': ('time', 'beam')})
fq.close()

allowed_variables = ['x', 'y', 'z', 'tvu']
dtype = [(varname, dataset[varname].dtype) for varname in allowed_variables if varname in dataset]
data_arr = np.empty(len(dataset['x']), dtype=dtype)
for nm, typ in dtype:
    data_arr[nm] = dataset[nm].values
data_dask = da.from_array(data_arr)


def timethisthing(userfunc, args, kwargs, msg):
    st = perf_counter()
    ret = userfunc(*args, **kwargs)
    end = perf_counter()
    print(msg.format(end-st))
    return ret

# qm = QuadManager()
# timethisthing(qm.create, [data_arr], {'max_points_per_quad': 5}, 'Numpy build time: {}')
# timethisthing(qm.save, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Numpy save time: {}')
# timethisthing(qm.tree.draw_tree, [], {'plot_points': False}, 'Numpy draw time: {}')
#
# qm = QuadManager()
# timethisthing(qm.create, [data_dask], {'max_points_per_quad': 5}, 'Dask build time: {}')
# timethisthing(qm.save, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Dask save time: {}')
# timethisthing(qm.tree.draw_tree, [], {'plot_points': False}, 'Dask draw time: {}')

qm = QuadManager()
coordsys = dataset.horizontal_crs
vertref = dataset.vertical_reference
containername = os.path.split(dataset.output_path)[1]
multibeamlist = list(dataset.multibeam_files.keys())

timethisthing(qm.create, [dataset],
              {'container_name': containername, 'multibeam_file_list': multibeamlist,
               'crs': coordsys, 'vertical_reference': vertref, 'min_grid_size': 1, 'max_grid_size': 1},
              'Dataset build time: {}')
qm.plot_surface('depth')
qm.tree.draw_tree()

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

############################################################################################
from HSTB.kluster.fqpr_convenience import *
from HSTB.kluster.gdal_helpers import *
from HSTB.kluster.fqpr_surface_v3 import *

fq = reload_data(r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710')

# {'0000_20200317_062845_S222_EM710.all': [1584426525.807, 1584426639.304],
#  '0001_20200317_063913_S222_EM710.all': [1584427153.265, 1584427342.72],
#  '0002_20200317_064945_S222_EM710.all': [1584427785.168, 1584427891.757],
#  '0003_20200317_065753_S222_EM710.all': [1584428273.944, 1584428468.645],
#  '0004_20200317_071004_S222_EM710.all': [1584429004.248, 1584429117.759],
#  '0005_20200317_072016_S222_EM710.all': [1584429616.899, 1584429791.078],
#  '0006_20200317_074548_S222_EM710.all': [1584431148.983, 1584431279.806],
#  '0007_20200317_075822_S222_EM710.all': [1584431902.919, 1584432001.206],
#  '0008_20200317_081048_S222_EM710.all': [1584432648.714, 1584432748.339],
#  '0009_20200317_082551_S222_EM710.all': [1584433551.414, 1584433666.558],
#  '0010_20200317_083654_S222_EM710.all': [1584434214.578, 1584434297.872],
#  '0011_20200317_084855_S222_EM710.all': [1584434935.098, 1584435041.788],
#  '0012_20200317_090126_S222_EM710.all': [1584435686.38, 1584435772.905],
#  '0013_20200317_091555_S222_EM710.all': [1584436555.863, 1584436678.324],
#  '0014_20200317_093221_S222_EM710.all': [1584437541.661, 1584437624.045],
#  '0015_20200317_094852_S222_EM710.all': [1584438532.577, 1584438683.563]}

for cnt, tim in enumerate([1584426639.304, 1584427342.72, 1584427891.757, 1584428468.645, 1584429117.759, 1584429791.078,
                           1584431279.806, 1584432001.206, 1584432748.339, 1584433666.558, 1584434297.872, 1584435041.788,
                           1584435772.905, 1584436678.324, 1584437624.045, 1584438683.563]):
    fq.subset_by_time(1584426525.807, tim)
    surf = generate_new_surface(fq, min_grid_size=1, max_grid_size=1)
    plt.imshow(surf.node_data['z'])
    plt.title('{}-{}'.format(1584426525.807, tim))
    plt.savefig(r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710\fig{}.tif'.format(cnt))
    plt.close()
    fq.restore_subset()


fq.subset_by_time(1584426525.807, 1584429117.759)
surf = generate_new_surface(fq, min_grid_size=1, max_grid_size=1)

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
