################## TEST RUN THROUGH ##################################

from fqpr_convenience import *
import os

datasets = [r'C:\collab\dasktest\data_dir\EM710', r'C:\collab\dasktest\data_dir\EM2040',
            r'C:\collab\dasktest\data_dir\EM2040_smallfil', r'C:\collab\dasktest\data_dir\EM2040c',
            r'C:\collab\dasktest\data_dir\EM2040p (UNH ASV)', r'C:\collab\dasktest\data_dir\hassler_acceptance\refsurf',
            r'C:\collab\dasktest\data_dir\Hasslerdual', r'C:\collab\dasktest\data_dir\tj_sbet_test\MBES\2904_2019_KongsbergEM2040\2019-216',
            r'C:\collab\dasktest\data_dir\tjacceptance\reference_surface', r'C:\collab\dasktest\data_dir\tjmotionlatency\mbes']

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
             None, 2020, 12, 'NAD83']
            ]

outputdir = r'C:\collab\dasktest\data_dir\outputtest'

fldernames = ['EM710', 'EM2040', 'EM2040_smallfil', 'EM2040c', 'EM2040p_UNH', 'hassler_acceptance', 'Hasslerdual',
              'tj_sbet_test', 'tjacceptance', 'tjmotionlatency']
for cnt, dset in enumerate(datasets):
    fq = perform_all_processing(dset, outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='NAD83', vert_ref='waterline')
    # generate_new_surface(fq, resolution=2.0, output_path=os.path.join(outputdir, fldernames[cnt], 'surf.npz'))
    fq.export_pings_to_file()

fldernames = ['ra_mbes', 'tj_patch_test_710', 'tj_patch_test_2040']
for cnt, dset in enumerate(datasets_w_sbets):
    # isolate only the tj patch test
    if cnt != 1:
        continue
    sbet, smrmsg, logf, yr, wk, dat = sbetfils[cnt]
    fq = perform_all_processing(dset, navfiles=[sbet], outfold=os.path.join(outputdir, fldernames[cnt]), coord_system='NAD83',
                                vert_ref='waterline', errorfiles=[smrmsg], logfiles=[logf], weekstart_year=yr,
                                weekstart_week=wk, override_datum=dat)
    # generate_new_surface(fq, resolution=2.0, output_path=os.path.join(outputdir, fldernames[cnt], 'surf.npz'))
    fq.export_pings_to_file()


sbet = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet.out"
logf = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_sbet_export.log"
errorfil = r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\pospac\035_smrmsg.out"
fq = perform_all_processing(r'C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035',
                            navfiles=[sbet], errorfiles=[errorfil], logfiles=[logf], vert_ref='ellipse', outfold=r"C:\collab\dasktest\data_dir\outputtest\rambes35sbet")


sbet = r"C:\collab\dasktest\data_dir\val_kmall_patch\sbet_Mission 1.out"
smrmsg = r"C:\collab\dasktest\data_dir\val_kmall_patch\smrms_Mission 1.out"
fq = perform_all_processing(r'C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1',
                            navfiles=[sbet], vert_ref='waterline', errorfiles=[smrmsg], weekstart_year=2019, weekstart_week=15,
                            override_datum='WGS84')


############################ VISUALIZATIONS #########################

from fqpr_visualizations import FqprVisualizations
from fqpr_convenience import *

fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\patchtest", skip_dask=True)
fqv = FqprVisualizations(fq)
fqv.visualize_orientation_vector()
fqv.visualize_beam_pointing_vectors(False)
fqv.visualize_beam_pointing_vectors(True)

######################### PROCESS THROUGH API ########################

from fqpr_convenience import *

fq = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040\0001_20170822_144548_S5401_X.all")
fq = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040p (UNH ASV)\0002_20180728_191618_UNH_CW4.all")
fq = perform_all_processing(r"C:\collab\dasktest\data_dir\kmall_test\0007_20190513_154724_ASVBEN.kmall")

fq = convert_multibeam(r"C:\collab\dasktest\data_dir\kmall_test\0007_20190513_154724_ASVBEN.kmall")
fq = convert_multibeam(r"C:\collab\dasktest\data_dir\EM2040\0001_20170822_144548_S5401_X.all")

###################### PATCH TEST GENERATOR ##########################

from fqpr_convenience import *
from fqpr_generation import *
from xarray_conversion import *
fq = reload_data(r"C:\collab\dasktest\data_dir\EM2040\converted")
xyzrph = fq.source_dat.xyzrph
subset_time = [[fq.source_dat.raw_ping[0].time.values[0], fq.source_dat.raw_ping[0].time.values[10]],
               [fq.source_dat.raw_ping[0].time.values[50], fq.source_dat.raw_ping[0].time.values[60]]]

fq, soundings = reprocess_sounding_selection(fq, new_xyzrph=xyzrph, subset_time=subset_time, turn_off_dask=True)
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.scatter3D(soundings[0], soundings[1], soundings[2])

################################################################################

from fqpr_convenience import *
from time import perf_counter
import vgrid

fq = reload_data(r"C:\collab\dasktest\data_dir\tjmotionlatency\mbes\converted")
x = fq.soundings.x.values
y = fq.soundings.y.values
z = fq.soundings.z.values
w = np.full(x.shape, 1)
print('Operating on {} points'.format(len(x)))

orig = vgrid.vgrid()
st = perf_counter()
orig.add(x, y, z, w)
end = perf_counter()
print('Base add method: {} secs'.format(end - st))
# 55.84
orig.pcolor()

for chnksize in [100, 1000, 10000, 100000, 150000, 200000, 1000000]:
    newone = vgrid.vgrid()
    st = perf_counter()
    newone.numba_add(x, y, z, w, chnksize=chnksize)
    end = perf_counter()
    print('numba add method: {} chunksize {} secs'.format(chnksize, end - st))
    # 8.491535400000004
    newone.pcolor()
    break



import matplotlib.pyplot as plt
# @numba.jit(nopython=True)
chnks = [100, 1000, 10000, 100000, 150000, 200000, 1000000]
nopython_tme = [59.4, 28.5, 25.2, 24.5, 40.3, 40.1, 42.8]
fastmath_tme = [60.2, 30.3, 25.9, 25.4, 42.1, 40.9, 44.9]
parallel_tme = [59.9, 28.6, 25.4, 24.5, 41.3, 40.1, 45.0]
force_parallel = [55.2, 21.2, 17.9, 16.6, 16.2, 15.7, 18.0]
plt.hlines(55.8, 0, 1000000, colors='r', linestyle='dotted', label='current add method')
plt.plot(chnks, nopython_tme, label='nopython=True')
plt.plot(chnks, fastmath_tme, label='fastmath=True')
plt.plot(chnks, parallel_tme, label='parallel=True')
plt.plot(chnks, force_parallel, label='prange_force_parallel')
plt.xlabel('chunksize in number of soundings')
plt.ylabel('time in seconds to add')
plt.legend()
plt.ticklabel_format(useOffset=False, style='plain')
plt.title('Effect of numba with chunking vs current vgrid add on 4,510,400 soundings')
################## Building Entwine tiles
# https://pdal.io/stages/readers.numpy.html


####################################

from fqpr_convenience import *
import matplotlib.pyplot as plt
from scipy.stats import linregress
from scipy.signal import butter, sosfilt
from fqpr_sat import return_period_of_signal

tests = [r"C:\collab\dasktest\data_dir\EM710\subset\converted", r"C:\collab\dasktest\data_dir\hassler_acceptance\acc_test\converted",
         r"C:\collab\dasktest\data_dir\EM2040\subset\converted", r"C:\collab\dasktest\data_dir\kmall_test\converted",
         r"C:\collab\dasktest\data_dir\tjmotionlatency\mbes\converted"]
for test in tests:
    fq = reload_data(test)
    tms = fq.return_unique_times_across_sectors()
    ping_counters = fq.source_dat.return_ping_counters_at_time(tms)
    out, sec, p_tms = fq.reform_2d_vars_across_sectors_at_time(['x', 'y', 'z'], tms)
    print(test)
    print('*********')
    print(fq.return_total_pings(ping_counters))
    print(out.shape, p_tms.shape, p_tms[0], p_tms[1], p_tms[2])
    plt.scatter(out[0][::10], out[1][::10], c=out[2][::10])



###########################################

from fqpr_convenience import *
from fqpr_sat import *

test = r"C:\collab\dasktest\data_dir\tjmotionlatency\mbes\converted_waterline"
fq = reload_data(test)
wb = WobbleTest(fq)
wb.generate_starting_data(use_altitude=False)
wb.plot_correlation_table()
wb.plot_x_lever_arm_error(add_regression=True)
wb.plot_heave_sound_speed_two(add_regression=True)

from scipy import signal
n = 101
a = signal.firwin(n, cutoff=0.03125, window="hanning")
# Spectral inversion
a = -a
a[int(n/2)] = a[int(n/2)] + 1

orig_roll = wb.roll_at_ping_time
filt_roll = signal.lfilter(a, 1.0, orig_roll)
trimfilt_roll = filt_roll[int(n/2):]
plt.plot(orig_roll)
plt.plot(trimfilt_roll)

meandepth = wb.depth.mean(axis=1)
meandepth = meandepth - meandepth[0]
filt_depth = signal.lfilter(a, 1.0, meandepth)
trimfilt_depth = filt_depth[int(n/2):]
plt.plot(meandepth[:-int(n/2)])
plt.plot(trimfilt_depth)

#######################################################

from time import perf_counter
from fqpr_convenience import *
test = r"C:\collab\dasktest\data_dir\tjmotionlatency\mbes\converted_waterline"
fq = reload_data(test)

st = perf_counter()
fq.get_orientation_vectors()
end = perf_counter()
print('processing orientation: {} secs'.format(end - st))

#########################################################

from fqpr_convenience import *
from fqpr_generation import *
pth = r"C:\collab\dasktest\data_dir\kmall_test\mbes\0007_20190513_154724_ASVBEN.kmall"
fq = convert_multibeam(pth)
fq.get_orientation_vectors()
fq.get_beam_pointing_vectors()


fq = perform_all_processing(r"C:\collab\dasktest\data_dir\ra_mbes\2801_em2040\mbes\2020-035")


#####################################################

from xarray_conversion import *
from xarray_conversion import _run_sequential_read, _sequential_gather_max_beams, _sequential_trim_to_max_beam_number, \
    _sequential_to_xarray, _divide_xarray_futs, _return_xarray_mintime

mbes_read = BatchRead(r"C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1\0000_20190411_175243_ShipName.kmall")
self = mbes_read
self.client = dask_find_or_start_client()
self._batch_read_file_setup()
fil_start_end_times = self._gather_file_level_metadata(self.fils)
chnks_flat = self._batch_read_chunk_generation(self.fils)
newrecfutures = self._batch_read_sequential_and_trim(chnks_flat)
xarrfutures = self.client.map(_sequential_to_xarray, newrecfutures)

datatype = 'attitude'
input_xarrs = self.client.map(_divide_xarray_futs, xarrfutures, [datatype] * len(xarrfutures))
# input_xarrs = self._batch_read_sort_futures_by_time(input_xarrs)
mintims = self.client.gather(self.client.map(_return_xarray_mintime, input_xarrs))
sort_mintims = sorted(mintims)
if mintims != sort_mintims:
    self.logger.info('Resorting futures to time index: {}'.format(sort_mintims))
    idx = [mintims.index(t) for t in sort_mintims]
    input_xarrs = [input_xarrs[i] for i in idx]

data = self.client.gather(newrecfutures)
for i in range(5):
    print(float(np.min(data[i]['attitude']['time'])), float(np.max(data[i]['attitude']['time'])))

################################################################

from xarray_conversion import *
from xarray_conversion import _run_sequential_read, _sequential_gather_max_beams, _sequential_trim_to_max_beam_number, \
    _sequential_to_xarray, _divide_xarray_futs, _return_xarray_mintime

mbes_read = BatchRead(r"C:\collab\dasktest\data_dir\val_kmall_patch\Fallback_2040_40_1\0000_20190411_175243_ShipName.kmall")
self = mbes_read
self.client = dask_find_or_start_client()
self._batch_read_file_setup()
fil_start_end_times = self._gather_file_level_metadata(self.fils)
chnks_flat = self._batch_read_chunk_generation(self.fils)

# recfutures = self.client.map(_run_sequential_read, chnks_flat)
# data = self.client.gather(recfutures)
# for i in range(5):
#     print(float(np.min(data[i]['attitude']['time'])), float(np.max(data[i]['attitude']['time'])))

fil, offset, endpt = chnks_flat[0]
km = kmall(fil)
# kmall doesnt have ping-wise serial number in header, we have to provide it from install params
serial_translator = km.fast_read_serial_number_translator()
# recs = km.sequential_read_records(start_ptr=offset, end_ptr=endpt, serial_translator=serial_translator)
# print(float(np.min(recs['attitude']['time'])), float(np.max(recs['attitude']['time'])))

from HSTB.drivers.kmall import *
self = km
start_ptr = offset
end_ptr = endpt
serial_translator = serial_translator
first_installation_rec = False

recs_categories, recs_categories_translator, recs_categories_result = self._build_sequential_read_categories()
wanted_records = list(recs_categories.keys())
recs_to_read = copy.deepcopy(recs_categories_result)
recs_count = dict([(k, 0) for k in recs_to_read])

if self.FID is None:
    self.OpenFiletoRead()

filelen = self._initialize_sequential_read(start_ptr, end_ptr)
if start_ptr:
    self.seek_next_startbyte(filelen, start_ptr=start_ptr)

while not self.eof:
    if self.FID.tell() >= start_ptr + filelen:
        self.eof = True
        break
    self.decode_datagram()
    if self.datagram_ident not in wanted_records:
        self.skip_datagram()
        continue
    self.read_datagram()
    for rec_ident in list(recs_categories_translator[self.datagram_ident].values())[0]:
        recs_count[rec_ident[0]] += 1

    rec = self.datagram_data
    recs = self._divide_rec(rec)  # split up the MRZ record for multiple sectors, otherwise just returns [rec]
    for rec in recs:
        for subrec in recs_categories[self.datagram_ident]:
            #  override for nested recs, designated with periods in the recs_to_read dict
            if subrec.find('.') > 0:
                if len(subrec.split('.')) == 3:
                    rec_key = subrec.split('.')[2]
                    tmprec = rec[subrec.split('.')[0]][subrec.split('.')[1]][rec_key]
                else:
                    rec_key = subrec.split('.')[1]
                    tmprec = rec[subrec.split('.')[0]][rec_key]
            else:
                rec_key = subrec
                tmprec = rec[rec_key]

            if subrec in ['install_txt', 'runtime_txt']:  # str, casting to list splits the string, dont want that
                val = [tmprec]
            else:
                try:  # flow for array/list attribute
                    val = [np.array(tmprec)]
                except TypeError:  # flow for float/int attribute
                    val = [tmprec]

            # generate new list or append to list for each rec of that dgram type found
            for translated in recs_categories_translator[self.datagram_ident][subrec]:
                if recs_to_read[translated[0]][translated[1]] is None:
                    recs_to_read[translated[0]][translated[1]] = copy.copy(val)
                else:
                    recs_to_read[translated[0]][translated[1]].extend(val)
    if self.datagram_ident == 'IIP' and first_installation_rec:
        self.eof = True
recs_to_read = self._finalize_records(recs_to_read, recs_count, serial_translator=serial_translator)

#################################################

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
from fqpr_sat import accuracy_test
output_directory = r"C:\collab\dasktest\data_dir\outputtest\acc_test_screengrabs"
reference_surface = r"C:\collab\dasktest\data_dir\outputtest\hassler_refsurf.npz"
linepairs = [r"C:\collab\dasktest\data_dir\outputtest\hassler_400_long_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_400_short_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_400_long_cw",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_400_short_cw",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_300_long_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_300_short_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_300_long_cw",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_300_short_cw",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_200_long_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_200_short_fm",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_200_long_cw",
             r"C:\collab\dasktest\data_dir\outputtest\hassler_200_short_cw"]
accuracy_test(reference_surface, linepairs, vert_ref='waterline', output_directory=output_directory)


##################################### tpu module ##########################################
import tpu
from fqpr_convenience import reload_data
fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\rambes35sbet")
tvu, thu = tpu.calculate_tpu(fq.source_dat.raw_ping[0]['roll'], fq.source_dat.raw_ping[0].corr_pointing_angle,
                             fq.source_dat.raw_ping[0].acrosstrack, fq.source_dat.raw_ping[0].depthoffset,
                             fq.source_dat.tpu_parameters, fq.source_dat.raw_ping[0].qualityfactor,
                             fq.source_dat.raw_ping[0].north_position_error, fq.source_dat.raw_ping[0].east_position_error,
                             fq.source_dat.raw_ping[0].down_position_error, qf_type='kongsberg')
