# Examples related to changing, subsetting, filtering and saving data, last updated 2/23/2022, Kluster 0.8.10
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

import numpy as np
from HSTB.kluster.fqpr_convenience import reload_data
from HSTB.kluster.fqpr_intelligence import intel_process
from HSTB.kluster import kluster_variables

# we start with one of the preferred processing steps from the data_processing example
_, fq = intel_process(r"C:\data_dir\0009_20170523_181119_FA2806.all")
fq = fq[0]  # this should just be a list of one element if you have just one sonar/day of data, take the first one to get the data
# or we can just reload if you have data from before
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")

# Build out a polygon in geographic coordinates to just get a subset of data from this dataset (lon, lat)
polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                    [-122.47771027, 47.78949665]])
# return soundings gets you the variables used in Points View, these are all 1d arrays of the same length
head, x, y, z, tvu, rejected, pointtime, beam = fq.return_soundings_in_polygon(polygon)
assert head.shape == x.shape == y.shape == z.shape == tvu.shape == rejected.shape == pointtime.shape == beam.shape
assert x.shape == (1911,)

# rejected array is actually an array of integers that are the sounding flags kluster uses for rejecting/accepting soundings
print(kluster_variables.amplitude_detect_flag)  # added in kluster 0.8.10
print(kluster_variables.phase_detect_flag)  # added in kluster 0.8.10
print(kluster_variables.rejected_flag)
print(kluster_variables.accepted_flag)
# so we can easily find the rejected soundings in the polygon by building a mask where rejected == 2
z_rej = z[rejected == kluster_variables.rejected_flag]  # there currently are none
assert z_rej.size == 0

# now try just pulling the corrected beam angle for the soundings
beamangle = fq.return_soundings_in_polygon(polygon, variable_selection=('corr_pointing_angle',))
assert beamangle[0].shape == (1911,)

# now use the existing filter that we set with the last return_soundings_in_polygon to get an additional variable
getbeamangle = fq.get_variable_by_filter('corr_pointing_angle')
assert getbeamangle.shape == (1911,)
assert (beamangle == getbeamangle).all()

# try a 1d variable
alti = fq.get_variable_by_filter('altitude')
assert alti.shape == (1911,)

# try a attitude variable
rollv = fq.get_variable_by_filter('roll')
assert rollv.shape == (1911,)

# now try setting the filter separately from the return_soundings_in_polygon method.  This allows you to set the
#    filter without return_soundings_in_polygon if you want to do that.  You might not want to get head, x, y, z, etc.
fq.set_filter_by_polygon(polygon)
next_getbeamangle = fq.get_variable_by_filter('corr_pointing_angle')
assert next_getbeamangle.shape == (1911,)
assert (beamangle == next_getbeamangle).all()

# now we can talk about saving changes to disk.  With the filter set by the polygon, we can now overwrite the sounding
#  flag for the soundings in the polygon.
assert rejected[0] == 0  # first sounding is 0 status, amplitude detect
# detectioninfo is the variable name that is equivalent to our rejected array.  Let's set the first sounding in our polygon to rejected
fq.set_variable_by_filter('detectioninfo', kluster_variables.rejected_flag, selected_index=[[0]])
# now we can reload the detectioninfo and check if the first sounding is rejected
new_rejected = fq.get_variable_by_filter('detectioninfo')
assert new_rejected[0] == kluster_variables.rejected_flag

# we can also just set all the soundings in the polygon to rejected.
fq.set_variable_by_filter('detectioninfo', 2)
all_rejected = fq.get_variable_by_filter('detectioninfo')
assert all(all_rejected == 2)

# or we can go back to the way it was, and set detectioninfo to the original rejected array
fq.set_variable_by_filter('detectioninfo', rejected)
back_to_start_rejected = fq.get_variable_by_filter('detectioninfo')
assert all(back_to_start_rejected == rejected)
