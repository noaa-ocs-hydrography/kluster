# Examples related to exporting data, last updated 11/3/2021, Kluster 0.8.2
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

# Kluster allows you to export data in a number of different ways, all using the export module.

from HSTB.kluster.fqpr_convenience import reload_data
from HSTB.kluster.fqpr_intelligence import intel_process

# we start with one of the preferred processing steps from the data_processing example
_, fq = intel_process(r"C:\data_dir\0009_20170523_181119_FA2806.all")
fq = fq[0]  # this should just be a list of one element if you have just one sonar/day of data, take the first one to get the data
# or we can just reload if you have data from before
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")

# the simplest export is to just export one of the variables to csv.  You can do that by specifying the output path and the
#  dataset/variable that you are interested in.  These routines are available in the GUI as well in the Basic Plots window.

# let's try a simple one: roll.
fq.export_variable('attitude', 'roll', r"C:\data_dir\roll_data.csv")

# we now have a roll_data_40111.csv file, which is the roll associated with the multibeam of serial number 40111
# all the one dimensional data is a simple export, with just time, value for all values.  Here are a few rows from my file.

# time,roll
# 1495563079.291000,1.330
# 1495563079.300000,1.190
# 1495563079.310000,1.040
# 1495563079.320000,0.900
# 1495563079.330000,0.750
# 1495563079.340000,0.600

# so what happens when we pick a two dimensional dataset like beam angle, which is (time, beam) dimension
fq.export_variable('multibeam', 'beampointingangle', r"C:\data_dir\bpa_data.csv")

# time,beam,beampointingangle
# 1495563079.534001,0,74.640
# 1495563079.534001,1,74.470
# 1495563079.534001,2,74.360
# 1495563079.534001,3,74.250

# you can see we get the value for each beam for each time/ping.  This might not be what you want, as you can often times
#  only import data that is one dimension in other software.  So let's look at the reduce methods
fq.export_variable('multibeam', 'beampointingangle', r"C:\data_dir\bpa_data.csv", reduce_method='nadir')

# time,beampointingangle
# 1495563079.534001,-15.300
# 1495563079.534010,-15.300
# 1495563080.018000,-8.690
# 1495563080.018001,-8.690

# better, now we get unique time records, as we drop the beam dimension and only retain the nadir beam, or the center
#  beam (which is what nadir beam means).  You can do similar things with only retaining outer beams, or taking the mean
#  value across the ping.

# we can also just export an entire dataset.  What happens if we export the multibeam dataset?  What do we get?
fq.export_dataset('multibeam', r"C:\data_dir\mbes_data.csv")

# time,mean_acrosstrack,mean_alongtrack,altitude,mean_beampointingangle,corr_altitude,corr_heave,mean_corr_pointing_angle,counter,mean_datum_uncertainty,mean_delay,mean_depthoffset,median_detectioninfo,median_frequency,nadir_geohash,latitude,longitude,mode,modetwo,ntx,median_processing_status,median_qualityfactor,mean_rel_azimuth,soundspeed,mean_thu,mean_tiltangle,mean_traveltime,mean_tvu,median_txsector_beam,mean_x,mean_y,yawpitchstab,mean_z
# 1495563079.5340009,23.80731,0.48085746,-23.933704,-3.8506494,0.0,-0.06,-0.15057938,61967,0.0,0.0022060382,88.12566,1.0,280000.0,c22zugx,47.78890945494799,-122.47711319986821,FM,__FM,3,5.0,6.0,2.9452024,1488.6,4.5293083,0.22404997,0.2170853,1.0640999,1.0,539178.870815,5292989.855275,PY,88.705666
# 1495563079.53401,23.665981,-0.09061504,-23.933704,-3.8822753,0.0,-0.06,-0.15070686,61966,0.0,3.7951395e-08,88.118515,1.0,270000.0,c22zugx,47.78890945494799,-122.47711319986821,FM,__FM,3,5.0,6.0,2.943974,1488.6,4.585592,0.0041500092,0.21705505,1.1111612,1.0,539179.2418725,5292989.3985749995,PY,88.69852
# 1495563080.018,15.559487,0.0021249389,-23.933704,-1.1309228,0.0,-0.07,-0.10908901,61968,0.0,3.7951395e-08,88.42957,1.0,270000.0,c22zugx,47.78890945494799,-122.47711319986821,FM,__FM,3,5.0,6.0,2.9861643,1488.6,4.6953177,0.05902508,0.21686696,1.106144,1.0,539174.38807,5292982.9089925,PY,88.99957
# 1495563080.0180008,15.6284275,0.57195,-23.933704,-1.1407495,0.0,-0.07,-0.109062046,61969,0.0,0.0022060382,88.46252,1.0,280000.0,c22zugx,47.78890945494799,-122.47711319986821,FM,__FM,3,5.0,6.0,2.9863741,1488.6,4.608861,0.27639997,0.21700116,1.0844115,1.0,539173.9779575,5292983.310222499,PY,89.03252

# jeez, that is a lot of information.  Probably too much honestly.  This is why you can export separate variables and
#  pare down the time region before exporting using the Basic Plots tool.  You definitely don't want a file like this
#  with a million rows.

# the dataset export will automatically reduce the two dimensional data using the best method depending on the variable.
#  floating point variables like beampointingangle retain the mean value, string values like geohash retain the nadir value,
#  etc.  So you are left with one row per ping.

# OK so let's get down to the more involved point cloud exports.  These are the exports that get you xyz and allow you
#  to bring this data in to another software to get a point cloud, like QGIS.

# the most basic one just exports all the soundings in the Fqpr object to csv
fq.export_pings_to_file(r"C:\data_dir")

# Out[11]:
# ['C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_0_265000_1.csv',
#  'C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_2_270000_1.csv',
#  'C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_0_275000_1.csv',
#  'C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_2_280000_1.csv',
#  'C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_1_285000_1.csv',
#  'C:\\data_dir\\csv_export\\em2040_40111_05_23_2017_1_290000_1.csv']

# you can see it returned paths to 6 files, one for each sector/frequency combination.  This is the default option, and
#  what I started with.  More than likely, you probably want export_by_identifiers=False
fq.export_pings_to_file(r"C:\data_dir", export_by_identifiers=False)
# Out[13]: ['C:\\data_dir\\csv_export_20211103_145808\\em2040_40111_05_23_2017_20211103_145808_1.csv']

# inside this file you see an exported point cloud that contains the x,y,z,uncertainty values
# easting northing depth uncertainty
# 539045.489 5292809.051 92.414 2.912
# 539045.996 5292809.731 92.579 1.275
# 539046.850 5292810.880 92.489 3.009
# 539047.764 5292812.113 92.295 1.994
# 539048.419 5292812.995 92.311 1.563

# similarly we can export to LAS using point format 3, which is a relatively pared down format in LAS.
fq.export_pings_to_file(r"C:\data_dir", file_format='las', export_by_identifiers=False)
# ****Exporting xyz data to las****
# Operating on system 40111
# writing to C:\data_dir\las_export_20211103_155159\em2040_40111_05_23_2017_20211103_155159_1.las
# 86400 total soundings, 78883 retained, 7517 filtered
# ****Exporting xyz data to las complete: 1 seconds****
# Out[18]: ['C:\\data_dir\\las_export_20211103_155159\\em2040_40111_05_23_2017_20211103_155159_1.las']

# there is also an entwine export that is available, however currently this is in development and will only work
#  in a pydro environment.  The entwine export process may change here in the near future, or get replaced by something
#  else, so we'll skip for now

# kluster also allows you to export data for just one line if you like.  Not much to say here, since we only have one
#  line, but it looks something like this

# look at the multibeam files in the Fqpr object
fq.multibeam.raw_ping[0].multibeam_files
# Out[16]: {'0009_20170523_181119_FA2806.all': [1495563079.364, 1495563133.171]}
fq.export_lines_to_file(['0009_20170523_181119_FA2806.all'], r"C:\data_dir", file_format='las', export_by_identifiers=False)
# ****Exporting xyz data to las****
# writing to C:\data_dir\las_export\0009_20170523_181119_FA2806.las
# ****Exporting xyz data to las complete: 0 seconds****
# Out[17]: ['C:\\data_dir\\las_export\\0009_20170523_181119_FA2806.las']
