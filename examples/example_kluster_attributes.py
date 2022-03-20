# Examples related to understanding kluster attributes, last updated 3/20/2022, Kluster 0.9.0
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

# See https://kluster.readthedocs.io/en/latest/indepth/datastructures.html to learn more

import numpy as np
import xarray as xr
from HSTB.kluster.fqpr_convenience import reload_data
from HSTB.kluster.fqpr_intelligence import intel_process

# we start with one of the preferred processing steps from the data_processing example
_, fq = intel_process(r"C:\data_dir\0009_20170523_181119_FA2806.all")
fq = fq[0]  # this should just be a list of one element if you have just one sonar/day of data, take the first one to get the data
# or we can just reload if you have data from before
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")

# many of the useful attributes can be seen in the __repr__ of the FQPR object
fq
# FQPR: Fully Qualified Ping Record built by Kluster Processing
# -------------------------------------------------------------
# Contains:
# 1 sonar head, 216 pings, version 0.9.0
# Start: Tue May 23 18:11:19 2017 UTC
# End: Tue May 23 18:12:12 2017 UTC
# Minimum Latitude: 47.78890945494799 Maximum Latitude: 47.789430586674875
# Minimum Longitude: -122.47843908633611 Maximum Longitude: -122.47711319986821
# Minimum Northing: 5292775.188 Maximum Northing: 5293237.445
# Minimum Easting: 538920.942 Maximum Easting: 539319.246
# Minimum Depth: 72.961 Maximum Depth: 94.294
# Current Status: tpu complete
# Sonar Model Number: em2040
# Primary/Secondary System Serial Number: 40111/0
# Horizontal Datum: 32610
# Vertical Datum: waterline
# Navigation Source: multibeam
# Contains SBETs: False
# Sound Velocity Profiles: 1

# some of these are built in properties
fq.number_of_pings
# 216
fq.horizontal_crs
# <Derived Projected CRS: EPSG:32610>
# Name: WGS 84 / UTM zone 10N
# Axis Info [cartesian]:
# - E[east]: Easting (metre)
# - N[north]: Northing (metre)
# Area of Use:
# - name: Between 126°W and 120°W, northern hemisphere between equator and 84°N, onshore and offshore. Canada - British Columbia (BC); Northwest Territories (NWT); Nunavut; Yukon. United States (USA) - Alaska (AK).
# - bounds: (-126.0, 0.0, -120.0, 84.0)
# Coordinate Operation:
# - name: UTM zone 10N
# - method: Transverse Mercator
# Datum: World Geodetic System 1984 ensemble
# - Ellipsoid: WGS 84
# - Prime Meridian: Greenwich
fq.vert_ref
# 'waterline'

# some you have to access through the xarray dataset interface
# Note that all raw_ping datasets will have the same attribution (in the case of dual head systems)
fq.multibeam.raw_ping[0].attrs['min_lat']
# 47.78890945494799
list(fq.multibeam.raw_ping[0].attrs.keys())
# ['_compute_beam_vectors_complete', '_compute_orientation_complete', '_conversion_complete', '_georeference_soundings_complete',
#  '_sound_velocity_correct_complete', '_total_uncertainty_complete', 'attributes_1495563079', 'current_processing_status',
#  'geohashes', 'horizontal_crs', 'input_datum', 'installsettings_1495563079', 'kluster_convention', 'kluster_version',
#  'max_lat', 'max_lon', 'max_x', 'max_y', 'max_z', 'min_lat', 'min_lon', 'min_x', 'min_y', 'min_z', 'multibeam_files', 'navigation_source',
#  'output_path', 'profile_1495563079', 'reference', 'runtimesettings_1495563080', 'secondary_system_serial_number', 'sonar_reference_point',
#  'sonartype', 'status_lookup', 'survey_number', 'svmode', 'system_identifier', 'system_serial_number', 'units', 'vertical_crs',
#  'vertical_reference', 'xyzrph']
# the attitude dataset also has some attribution
list(fq.multibeam.raw_att.attrs.keys())
# ['reference', 'units']

# the FQPR object also stores the dask information, address will be set for remote cluster (default is LocalCluster)
fq.client
# <Client: 'tcp://127.0.0.1:57450' processes=8 threads=16, memory=31.91 GiB>
fq.address
# None
