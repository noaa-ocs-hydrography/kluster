# Examples related to data processing, last updated 11/2/2021, Kluster 0.8.2
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

# data processing can be done in one of three ways:
#  1. piece-wise lower level approach = use convert_multibeam, process_multibeam
#  2. merged lower level approach = use perform_all_processing
#  3. high level automated processing = use the intel process/service

# --- SKIP TO 3 FOR THE RECOMMENDED PROCESSING ROUTINE --- #

from HSTB.kluster.fqpr_convenience import convert_multibeam, process_multibeam, import_processed_navigation, \
    perform_all_processing, reload_data
from HSTB.kluster.fqpr_intelligence import intel_process, intel_process_service

#####################################
# 1. piece-wise lower level approach
#####################################

# conversion will generate data in the Kluster zarr/xarray format for you to use
# can either be a list of multibeam files
# fq = convert_multibeam([r"C:\data_dir\0009_20170523_181119_FA2806.all",])
# a path to a single file
# fq = convert_multibeam(r"C:\data_dir\\0009_20170523_181119_FA2806.all")
# or a path to a directory of files
# fq = convert_multibeam(r"C:\data_dir")
fq = convert_multibeam(r"C:\data_dir\0009_20170523_181119_FA2806.all")

# look at the summary of the converted data
fq
# Out[4]:
# FQPR: Fully Qualified Ping Record built by Kluster Processing
# -------------------------------------------------------------
# Contains:
# 1 sonar head, 216 pings, version 0.8.2
# Start: Tue May 23 18:11:19 2017 UTC
# End: Tue May 23 18:12:12 2017 UTC
# Minimum Latitude: 47.78890945494799 Maximum Latitude: 47.789430586674875
# Minimum Longitude: -122.47843908633611 Maximum Longitude: -122.47711319986821
# Minimum Northing: Unknown Maximum Northing: Unknown
# Minimum Easting: Unknown Maximum Easting: Unknown
# Minimum Depth: Unknown Maximum Depth: Unknown
# Current Status: converted
# Sonar Model Number: em2040
# Primary/Secondary System Serial Number: 40111/0
# Horizontal Datum: Unknown
# Vertical Datum: None
# Navigation Source: Unknown
# Sound Velocity Profiles: 1

# the converted ping datasets are in fq.multibeam.raw_ping.  There is one dataset for each sonar head.  These datasets
#  have (time, beam) dimensions, where each time value is a new ping.

# number of heads
len(fq.multibeam.raw_ping)
# Out[7]: 1

# dataset for the first (and only) head
fq.multibeam.raw_ping[0]
# Out[8]:
# <xarray.Dataset>
# Dimensions:            (beam: 400, time: 216)
# Coordinates:
#   * beam               (beam) int32 0 1 2 3 4 5 6 ... 394 395 396 397 398 399
#   * time               (time) float64 1.496e+09 1.496e+09 ... 1.496e+09
# Data variables: (12/18)
#     altitude           (time) float32 dask.array<chunksize=(216,), meta=np.ndarray>
#     beampointingangle  (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     counter            (time) int64 dask.array<chunksize=(216,), meta=np.ndarray>
#     delay              (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     detectioninfo      (time, beam) int32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     frequency          (time, beam) int32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     ...                 ...
#     qualityfactor      (time, beam) int32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     soundspeed         (time) float32 dask.array<chunksize=(216,), meta=np.ndarray>
#     tiltangle          (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     traveltime         (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     txsector_beam      (time, beam) uint8 dask.array<chunksize=(216, 400), meta=np.ndarray>
#     yawpitchstab       (time) <U2 dask.array<chunksize=(216,), meta=np.ndarray>
# Attributes: (12/23)
#     _conversion_complete:            Tue Nov  2 20:00:04 2021
#     attributes_1495563079:           {"location": [47.78890945494799, -122.47...
#     current_processing_status:       0
#     input_datum:                     WGS84
#     installsettings_1495563079:      {"waterline_vertical_location": "-0.640"...
#     kluster_version:                 0.8.2
#     ...                              ...
#     status_lookup:                   {'0': 'converted', '1': 'orientation', '...
#     survey_number:                   ['01_Patchtest_2806']
#     system_identifier:               40111
#     system_serial_number:            [40111]
#     units:                           {'altitude': 'meters (+ down from ellips...
#     xyzrph:                          {'beam_opening_angle': {'1495563079': 1....

# number of pings
fq.multibeam.raw_ping[0].time.size
# Out[9]: 216

# number of beams
fq.multibeam.raw_ping[0].beam.size
# Out[10]: 400

# fq.multibeam.raw_ping[0] is an xarray dataset, so we can access data using the dataset api
fq.multibeam.raw_ping[0].beampointingangle
# Out[11]:
# <xarray.DataArray 'beampointingangle' (time: 216, beam: 400)>
# dask.array<xarray-beampointingangle, shape=(216, 400), dtype=float32, chunksize=(216, 400), chunktype=numpy.ndarray>
# Coordinates:
#   * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 392 393 394 395 396 397 398 399
#   * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09
# Attributes:
#     _FillValue:  nan

# you might have some data that is not in the multibeam file, for instance:
# loading from a post processed Applanix SBET file
fq = import_processed_navigation(fq, [r'C:\data_dir\sbet.out'], [r'C:\data_dir\smrmsg.out'], [r'C:\data_dir\export_log.txt'])

# now you can process the converted/imported data.  The defaults will do a full processing run on all data, but you can
#  specify a few things if you like
# the default run
fq = process_multibeam(fq)
# include another sound velocity file
fq = process_multibeam(fq, add_cast_files=r'C:\data_dir\mysvpfile.svp')
# specify coordinate system and vertical reference
fq = process_multibeam(fq, coord_system='WGS84', vert_ref='ellipse')

# reload the data later on
fq = reload_data(r"C:\data_dir\converted")
#####################################
# 2. merged lower level approach
#####################################

# all the above can be combined into the perform_all_processing command for ease of use
fq = perform_all_processing(r"C:\data_dir\0009_20170523_181119_FA2806.all", navfiles=[r'C:\data_dir\sbet.out'],
                            errorfiles=[r'C:\data_dir\smrmsg.out'], logfiles=[r'C:\data_dir\export_log.txt'],
                            add_cast_files=r'C:\data_dir\mysvpfile.svp', coord_system='WGS84', vert_ref='ellipse')
# reload the data later on
fq = reload_data(r"C:\data_dir\converted")
########################################################################
# 3. high level automated processing = use the intel process/service
########################################################################

# when you drag in new multibeam data into Kluster, it generates a new conversion action and organizes data into:
#  sonarmodelnumber_serialnumber_dateofsurvey
# which it gets from the multibeam data itself.  This is the Kluster Intelligence module that will basically perform
#  the convert_multibeam and process_multibeam actions for you, putting data in the right place and doing only those
#  steps that are required.  For this reason it is recommended that you use the intelligence module rather than
#  the core processing routines described in 1 and 2.  Learn more here: https://kluster.readthedocs.io/en/latest/indepth/intel.html

# the intel process command will perform just like if you were to drag in new files in Kluster.  You just provide all the
#  files that you want to add, and Kluster Intelligence determines the type of file, how to add it and what processing
#  steps to take.  Those steps are all performed and you get the Fqpr object back

# just like all the others, either provide a list of files...
_, fq = intel_process([r"C:\data_dir\0009_20170523_181119_FA2806.all",
                       r'C:\data_dir\sbet.out', r'C:\data_dir\smrmsg.out',
                       r'C:\data_dir\export_log.txt', r'C:\data_dir\mysvpfile.svp'],
                      coord_system='WGS84', vert_ref='ellipse')
# a single file or a path to a directory full of files:
_, fq = intel_process(r"C:\data_dir")
# it will behave much like step 2, you probably won't notice a difference during the processing
# what you should notice is that the output directory is now a folder with the sonarmodelnumber_serialnumber_dateofsurvey of the file
# - note: fq is a list of the converted fqpr objects, since data is organized by modelnumber,etc., you might end up with multiple containers
fq[0].output_folder
# Out[10]: 'C:\\data_dir\\em2040_40111_05_23_2017'

# the last thing to mention is the intel_process_service, which combines intel_process with folder monitoring.  The service
#  will monitor a folder and add/process any files that you add to that directory (or are already in there)
_, fq = intel_process_service(r"C:\data_dir")
# this will lock up the console until you force it to quit

# reload the data later on
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")
