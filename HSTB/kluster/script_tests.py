#################### Get download count ##############################
# https://stackoverflow.com/questions/4338358/github-can-i-see-the-number-of-downloads-for-a-repo/57993109#57993109

from github import Github
g = Github("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")

for repo in g.get_user().get_repos():
    if repo.name == "kluster":
        releases = repo.get_releases()
        for i in releases:
            for j in i.get_assets():
                print("{} date: {} download count: {}".format(j.name, j.created_at, j.download_count))

###################### Install from a specific branch #########################

# pip install git+https://github.com/noaa-ocs-hydrography/bathygrid.git@bathygrid_1_2_0

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
            r"C:\collab\dasktest\data_dir\from_Lund\0063_20210612_092103_MAL_EM2040MKII.kmall", r"C:\collab\dasktest\data_dir\from_Lund\0011_20210304_094901_EM2040P.kmall",
            r"C:\collab\dasktest\data_dir\EM2040_HMNZS_wellington_wreck", r"C:\collab\dasktest\data_dir\EM712_kmall_fromkongsberg"]

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
              'lund_em2040p', 'EM2040_wellington_wreck', 'EM712_fromkongsberg_kmall']
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
    if system is None:  # get here if one of the heads is disabled (set to None)
        continue
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

################################################################
from HSTB.kluster.fqpr_convenience import reload_data
import numpy as np

fq = reload_data(r"C:\Pydro21_Dev\NOAA\site-packages\Python38\git_repos\hstb_kluster\test_data\em2040_40111_05_23_2017")
polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                    [-122.47771027, 47.78949665]])
x, y, z, tvu, rejected, pointtime, beam = fq.return_soundings_in_polygon(polygon)

###################################################################

"""
message 1003 has 1592 packets and 0.205% of file
message 1012 has 4774 packets and 0.472% of file
message 1013 has 4774 packets and 0.425% of file
message 7000 has 1018 packets and 0.282% of file
message 7001 has 1 packets and 0.007% of file
message 7004 has 1018 packets and 6.541% of file
message 7006 has 1018 packets and 6.958% of file
message 7008 has 1007 packets and 83.734% of file
message 7022 has 1 packets and 0.0% of file
message 7200 has 1 packets and 0.0% of file
message 7300 has 1 packets and 0.962% of file
message 7503 has 1018 packets and 0.413% of file
message 7777 has 2 packets and 0.001% of file
"""

from HSTB.drivers.prr3 import X7kRead

fil = r"C:\collab\dasktest\data_dir\s7kdata\20140416_060218.s7k"
tst = X7kRead(fil)
tst.mapfile()

dat = tst.getrecord(7004, 0)
data = dat.full_settings

########################################################################
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.fqpr_generation import Fqpr

fil = r"C:\Pydro21_Dev\NOAA\site-packages\Python38\git_repos\hstb_kluster\tests\resources\0009_20170523_181119_FA2806.all"

fq = perform_all_processing(fil)



mbes_read = BatchRead(fil)
fqpr_inst = Fqpr(mbes_read)
fqpr_inst.read_from_source(build_offsets=False)

#######################################################################
