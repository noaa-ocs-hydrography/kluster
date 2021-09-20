# generic gui
pass_color = 'color : green'  # color of the gui labels and text where a test passes
error_color = 'color : red'  # color of the gui labels and text where a test does not pass
warning_color = 'color : orange'  # color of the gui labels and text where a warning is issued

# dask_helpers
# when we get the Client to run a task, we expect all tasks to have finished.  If you get the client and the mem
# utilization is greater than this percentage, we restart it automatically to clear the memory.
mem_restart_threshold = 0.40

# kluster_3dview
selected_point_color = (1, 0.476, 0.953, 1)  # color of points selected in 3dview
super_selected_point_color = (1, 1, 1, 1)  # color of points in super selection in 3dview

# generic processing
max_beams = 400  # starting max beams in kluster (can grow beyond)
epsg_nad83 = 6319
epsg_wgs84 = 7911
default_number_of_chunks = 4
converted_files_at_once = 5
geohash_precision = 7
max_processing_status = 5

supported_multibeam = ['.all', '.kmall']
multibeam_uses_quality_factor = ['.all']
multibeam_uses_ifremer = ['.kmall']
supported_ppnav = ['.out', '.sbet', '.smrmsg']
supported_ppnav_log = ['.txt', '.log']
supported_sv = ['.svp']

vertical_references = ['waterline', 'ellipse', 'NOAA MLLW', 'NOAA MHW']  # all vertical reference options
vdatum_vertical_references = ['NOAA MLLW', 'NOAA MHW']  # vertical reference options based in vdatum
ellipse_based_vertical_references = ['ellipse', 'NOAA MLLW', 'NOAA MHW']  # vertical reference options based on the ellipsoid
waterline_based_vertical_references = ['waterline']  # vertical reference options based on waterline
coordinate_systems = ['NAD83', 'WGS84']  # horizontal coordinate system options
default_coordinate_system = 'WGS84'
default_vertical_reference = 'waterline'

# export
pings_per_las = 50000  # LAS export will put this many pings in one file before starting a new file
pings_per_csv = 15000  # csv export will put this many pings in one file before starting a new file

# xarray conversion
ping_chunk_size = 1000  # chunk size (in pings) of each written chunk of data in the ping records
navigation_chunk_size = 50000  # chunk size (in time) of each written chunk of data in the navigation records
attitude_chunk_size = 20000  # chunk size (in time) of each written chunk of data in the attitude records

single_head_sonar = ['em122', 'em302', 'em710', 'em2045', 'em2040', 'em2040p', 'em3002', 'em2040p', 'em3020', 'me70bo']  # all single head sonar models
dual_head_sonar = ['em2040_dual_rx', 'em2040_dual_tx', 'em2045_dual']  # all dual head sonar models
# tpu parameter names controls what gets passed to the tpu calculator
tpu_parameter_names = ['tx_to_antenna_x', 'tx_to_antenna_y', 'tx_to_antenna_z', 'heave_error', 'roll_sensor_error',
                       'pitch_sensor_error', 'heading_sensor_error', 'x_offset_error',
                       'y_offset_error', 'z_offset_error', 'surface_sv_error', 'roll_patch_error', 'pitch_patch_error',
                       'heading_patch_error', 'latency_patch_error', 'timing_latency_error',
                       'separation_model_error', 'waterline_error', 'vessel_speed_error', 'horizontal_positioning_error',
                       'vertical_positioning_error', 'beam_opening_angle']
offset_parameter_names = ['tx_port_x', 'tx_stbd_x', 'rx_port_x', 'rx_stbd_x', 'tx_x', 'rx_x', 'tx_port_y', 'tx_stbd_y',
                          'rx_port_y', 'rx_stbd_y', 'tx_y', 'rx_y', 'tx_port_z', 'tx_stbd_z', 'rx_port_z', 'rx_stbd_z',
                          'tx_z', 'rx_z']
angle_parameter_names = ['tx_port_r', 'tx_stbd_r', 'rx_port_r', 'rx_stbd_r', 'tx_r', 'rx_r', 'tx_port_p', 'tx_stbd_p',
                         'rx_port_p', 'rx_stbd_p', 'tx_p', 'rx_p', 'tx_port_h', 'tx_stbd_h', 'rx_port_h', 'rx_stbd_h',
                         'tx_h', 'rx_h', 'latency']
# optional parameter names controls what is left out when comparing vessel entries to see if the new entry is worth keeping
optional_parameter_names = ['source', 'vessel_file', 'sonar_type', 'imu_h', 'imu_p', 'imu_r', 'imu_x', 'imu_y',
                            'imu_z', 'tx_to_antenna_x', 'tx_to_antenna_y', 'tx_to_antenna_z', 'vess_center_x', 'vess_center_y',
                            'vess_center_z', 'vess_center_yaw', 'vess_center_p', 'vess_center_r', 'sensor_size']

# see fqpr_generation.last_operation_date.  This is a list of attribute entries that contribute to the date of the last process
#     run.  The latest date of all of these is considered the last process (process being something that changes the data and would
#     then require regridding or something like that)
processing_log_names = ['_compute_orientation_complete', '_compute_beam_vectors_complete', '_sound_velocity_correct_complete',
                        '_georeference_soundings_complete', '_total_uncertainty_complete']

default_heave_error = 0.050  # default tpu parameter for heave
default_roll_error = 0.001  # default tpu parameter for roll
default_pitch_error = 0.001  # default tpu parameter for pitch
default_heading_error = 0.020  # default tpu parameter for heading
default_x_offset_error = 0.200  # default tpu parameter for x offset measurement
default_y_offset_error = 0.200  # default tpu parameter for y offset measurement
default_z_offset_error = 0.200  # default tpu parameter for z offset measurement
default_x_antenna_offset = 0.000  # default tpu parameter for x antenna offset
default_y_antenna_offset = 0.000  # default tpu parameter for y antenna offset
default_z_antenna_offset = 0.000  # default tpu parameter for z antenna offset
default_surface_sv_error = 0.500  # default tpu parameter for surface sv
default_roll_patch_error = 0.100  # default tpu parameter for roll patch
default_pitch_patch_error = 0.100  # default tpu parameter for pitch patch
default_heading_patch_error = 0.500  # default tpu parameter for heading patch
default_latency_patch_error = 0.000  # default tpu parameter for latency patch
default_latency_error = 0.001  # default tpu parameter for latency
default_separation_model_error = 0.000  # default tpu parameter for separation model
default_waterline_error = 0.020  # default tpu parameter for waterline
default_vessel_speed_error = 0.100  # default tpu parameter for vessel speed
default_horizontal_positioning_error = 1.500  # default tpu parameter for horizontal positioning
default_vertical_positioning_error = 1.000  # default tpu parameter for vertical positioning
default_beam_opening_angle = 1.0  # default parameter for beam opening angle in degrees

# zarr backend, chunksizes for writing to disk
ping_chunks = {'time': (ping_chunk_size,), 'beam': (max_beams,), 'xyz': (3,),
               'acrosstrack': (ping_chunk_size, max_beams),
               'alongtrack': (ping_chunk_size, max_beams),
               'altitude': (ping_chunk_size,),
               'beampointingangle': (ping_chunk_size, max_beams),
               'corr_altitude': (ping_chunk_size,),
               'corr_heave': (ping_chunk_size,),
               'corr_pointing_angle': (ping_chunk_size, max_beams),
               'counter': (ping_chunk_size,),
               'datum_uncertainty': (ping_chunk_size, max_beams),
               'delay': (ping_chunk_size, max_beams),
               'depthoffset': (ping_chunk_size, max_beams),
               'detectioninfo': (ping_chunk_size, max_beams),
               'frequency': (ping_chunk_size, max_beams),
               'geohash': (ping_chunk_size, max_beams),
               'latitude': (ping_chunk_size,),
               'longitude': (ping_chunk_size,),
               'mode': (ping_chunk_size,),
               'modetwo': (ping_chunk_size,),
               'ntx': (ping_chunk_size,),
               'processing_status': (ping_chunk_size, max_beams),
               'qualityfactor': (ping_chunk_size, max_beams),
               'rel_azimuth': (ping_chunk_size, max_beams),
               'rx': (ping_chunk_size, max_beams, 3),
               'rxid': (ping_chunk_size,),
               'samplerate': (ping_chunk_size,),
               'serial_num': (ping_chunk_size,), 
               'soundspeed': (ping_chunk_size,),
               'thu': (ping_chunk_size, max_beams),
               'tiltangle': (ping_chunk_size, max_beams),
               'traveltime': (ping_chunk_size, max_beams),
               'tvu': (ping_chunk_size, max_beams),
               'tx': (ping_chunk_size, max_beams, 3),
               'txsector_beam': (ping_chunk_size, max_beams),
               'waveformid': (ping_chunk_size,),
               'x': (ping_chunk_size, max_beams),
               'y': (ping_chunk_size, max_beams),
               'yawpitchstab': (ping_chunk_size,),
               'z': (ping_chunk_size, max_beams)
               }
nav_chunks = {'time': (navigation_chunk_size,),
              'alongtrackvelocity': (navigation_chunk_size,),
              'altitude': (navigation_chunk_size,),
              'latitude': (navigation_chunk_size,),
              'longitude': (navigation_chunk_size,),
              'down_position_error': (navigation_chunk_size,),
              'east_position_error': (navigation_chunk_size,),
              'north_position_error': (navigation_chunk_size,),
              'pitch_error': (navigation_chunk_size,),
              'roll_error': (navigation_chunk_size,),
              'heading_error': (navigation_chunk_size,)
              }
att_chunks = {'time': (attitude_chunk_size,),
              'heading': (attitude_chunk_size,),
              'heave': (attitude_chunk_size,),
              'pitch': (attitude_chunk_size,),
              'roll': (attitude_chunk_size,)
              }

# return soundings variable options, see subset.return_soundings_in_polygon
subset_variable_selection = ['head', 'time', 'beam', 'acrosstrack', 'alongtrack', 'altitude', 'beampointingangle', 'corr_altitude',
                             'corr_heave', 'corr_pointing_angle', 'counter', 'datum_uncertainty', 'delay', 'depthoffset', 'detectioninfo',
                             'frequency', 'geohash', 'latitude', 'longitude', 'mode', 'modetwo', 'ntx', 'processing_status', 'qualityfactor',
                             'rel_azimuth', 'soundspeed', 'thu', 'tiltangle', 'traveltime',
                             'tvu', 'txsector_beam', 'x', 'y', 'yawpitchstab', 'z']
subset_variable_2d = ['acrosstrack', 'alongtrack', 'beampointingangle', 'datum_uncertainty', 'delay', 'depthoffset', 'detectioninfo',
                      'frequency', 'geohash', 'ntx', 'processing_status', 'qualityfactor', 'rel_azimuth', 'thu',
                      'tiltangle', 'traveltime', 'tvu', 'tx', 'txsector_beam', 'x', 'y', 'yawpitchstab', 'z']
subset_variable_1d = ['head', 'time', 'beam', 'altitude', 'corr_altitude', 'corr_heave', 'corr_pointing_angle', 'counter', 'latitude',
                      'longitude', 'mode', 'modetwo', 'soundspeed']

# export helper for formatting variables in ascii export
variable_format_str = {'time': '%1.6f', 'beam': '%d', 'xyz': '%s',
                       'acrosstrack': '%1.3f', 'alongtrack': '%1.3f', 'altitude': '%1.3f',
                       'beampointingangle': '%1.3f', 'corr_altitude': '%1.3f',
                       'corr_heave': '%1.3f', 'corr_pointing_angle': '%1.6f',
                       'counter': '%d', 'datum_uncertainty': '%1.3f', 'delay': '%1.6f',
                       'depthoffset': '%1.3f', 'detectioninfo': '%d', 'frequency': '%d', 'geohash': '%s',
                       'latitude': '%1.8f', 'longitude': '%1.8f', 'mode': '%s', 'modetwo': '%s', 'ntx': '%d',
                       'processing_status': '%d', 'qualityfactor': '%d', 'rel_azimuth': '%f',
                       'rx': '%f', 'soundspeed': '%1.3f', 'thu': '%1.3f', 'tiltangle': '%1.3f',
                       'traveltime': '%1.6f', 'tvu': '%1.3f', 'tx': '%f', 'txsector_beam': '%d',
                       'x': '%1.3f', 'y': '%1.3f', 'yawpitchstab': '%s', 'z': '%1.3f',
                       'alongtrackvelocity': '%1.3f', 'down_position_error': '%1.3f', 'east_position_error': '%1.3f',
                       'north_position_error': '%1.3f', 'pitch_error': '%1.3', 'roll_error': '%1.3f',
                       'heading_error': '%1.3f', 'heading': '%1.3f', 'heave': '%1.3f', 'pitch': '%1.3f',
                       'roll': '%1.3f'}

# 2d plot helpers for handling variable information
variable_translator = {'acrosstrack': 'SoundVelocity_AcrossTrack', 'alongtrack': 'SoundVelocity_AlongTrack',
                       'beampointingangle': 'Uncorrected_Beam_Angle', 'corr_altitude': 'Corrected_Altitude',
                       'corr_heave': 'Corrected_Heave', 'corr_pointing_angle': 'Corrected_Beam_Angle',
                       'counter': 'Ping_Counter', 'delay': 'Beam_Delay', 'depthoffset': 'SoundVelocity_Depth',
                       'detectioninfo': 'Beam_Filter', 'frequency': 'Beam_Frequency', 'geohash': 'Geohash', 'mode': 'Ping_Mode',
                       'modetwo': 'Ping_Mode_Two', 'processing_status': 'Processing_Status',
                       'qualityfactor': 'Beam_Uncertainty', 'rel_azimuth': 'Relative_Azimuth',
                       'soundspeed': 'Surface_Sound_Velocity', 'thu': 'Beam_Total_Horizontal_Uncertainty',
                       'tiltangle': 'Ping_Tilt_Angle', 'traveltime': 'Beam_Travel_Time',
                       'tvu': 'Beam_Total_Vertical_Uncertainty', 'txsector_beam': 'Beam_Sector_Number',
                       'x': 'Georeferenced_Easting', 'y': 'Georeferenced_Northing',
                       'yawpitchstab': 'Yaw_Pitch_Stabilization', 'z': 'Georeferenced_Depth',
                       'datum_uncertainty': 'Vertical_Datum_Uncertainty'}
variable_reverse_lookup = {'SoundVelocity_AcrossTrack': 'acrosstrack', 'SoundVelocity_AlongTrack': 'alongtrack',
                           'Uncorrected_Beam_Angle': 'beampointingangle', 'Corrected_Altitude': 'corr_altitude',
                           'Corrected_Heave': 'corr_heave', 'Corrected_Beam_Angle': 'corr_pointing_angle',
                           'Ping_Counter': 'counter', 'Beam_Delay': 'delay',
                           'SoundVelocity_Depth': 'depthoffset', 'Beam_Filter': 'detectioninfo',
                           'Beam_Frequency': 'frequency', 'Ping_Mode': 'mode', 'Ping_Mode_Two': 'modetwo',
                           'Processing_Status': 'processing_status', 'Beam_Uncertainty': 'qualityfactor',
                           'Relative_Azimuth': 'rel_azimuth', 'Surface_Sound_Velocity': 'soundspeed',
                           'Beam_Total_Horizontal_Uncertainty': 'thu', 'Ping_Tilt_Angle': 'tiltangle',
                           'Beam_Travel_Time': 'traveltime', 'Beam_Total_Vertical_Uncertainty': 'tvu',
                           'Beam_Sector_Number': 'txsector_beam', 'Georeferenced_Easting': 'x',
                           'Georeferenced_Northing': 'y', 'Yaw_Pitch_Stabilization': 'yawpitchstab',
                           'Georeferenced_Depth': 'z', 'Vertical_Datum_Uncertainty': 'datum_uncertainty',
                           'Geohash': 'geohash'}
variable_descriptions = {'acrosstrack': 'The result of running Sound Velocity Correct in Kluster.  This is the acrosstrack (perpendicular to vessel movement) distance to the beam footprint on the seafloor from the vessel reference point in meters.',
                         'alongtrack': 'The result of running Sound Velocity Correct in Kluster.  This is the alongtrack (vessel direction) distance to the beam footprint on the seafloor from the vessel reference point in meters.',
                         'beampointingangle': 'The raw beam angle that comes from the multibeam data.  Angle in degrees from the receiver to the beam footprint on the seafloor, does not take attitude or mounting angles into account.',
                         'corr_altitude': 'If this dataset is processed to the waterline this will be zero.  Otherwise, the altitude correction is the attitude rotated lever arm between the reference point of the altitude and the transmitter, if non-zero.  This will be the original altitude plus this correction.',
                         'corr_heave': 'If this dataset is processed to the ellipse this will be zero.  Otherwise, the heave correction is the attitude rotated lever arm between the reference point of the heave and the transmitter, if non-zero. This will be the original heave plus this correction.',
                         'corr_pointing_angle': 'The result of running Compute Beam Vectors in Kluster.  This is the raw beam angles corrected for attitude and mounting angles, relative to nadir (straight down from sonar).',
                         'counter': 'The identification number assigned to each ping.  For Kongsberg .all, this is a 16bit number, so you will see it reset at 65536.',
                         'datum_uncertainty': 'Included when VDatum is used for vertical transformation to NOAA Chart Datums, is the uncertainty of that transform.  Will be all zeros if NOAA_MLLW/NOAA_MHW is not selected.',
                         'delay': 'The time delay applied to each sector, expanded to the beam dimension.  Comes from the multibeam raw data.  Generally fairly small, or zero.',
                         'depthoffset': 'The result of running Sound Velocity Correct in Kluster.  This is the down (positive down) distance to the beam footprint on the seafloor from the transmitter in meters.  Not from the vessel reference point to align with Kongsberg svcorrect convention.  We apply the z lever arm in georeferencing.',
                         'detectioninfo': 'The accepted/rejected state of each beam.  2 = rejected, 1 = phase detection, 0 = amplitude detection.  See Kongsberg "detectioninfo".',
                         'frequency': 'The frequency of each beam in Hz.',
                         'geohash': 'The computed base32 representation of the geohash, a code that defines the location of each beam in a region. \nPlotting will show the unique integer identifier instead of the string, for visualization purposes.',
                         'mode': 'The first mode value. \n(if TX Pulse Form) CW for continuous waveform, FM for frequency modulated, MIX for mix between FM and CW. \n(if Ping mode) VS for Very Shallow, SH for Shallow, ME for Medium, DE for Deep, VD for Very Deep, ED for Extra Deep.',
                         'modetwo': 'The second mode value. \n(if Pulse Length) vsCW = very short continuous waveform, shCW = short cw, meCW = medium cw, loCW = long cw, vlCW = very long cw, elCW = extra long cw, shFM = short frequency modulated, loFM = long fm. \n(if Depth Mode) VS = Very Shallow, SH = Shallow, ME = Medium, DE = Deep, DR = Deeper, VD = Very Deep, ED = Extra deep, XD = Extreme Deep, if followed by "m" system is in manual mode.',
                         'processing_status': 'The Kluster processing status of each beam, the highest state of the beam.  EX: If 3, sounding is only processed up to sound velocity correction. 0 = converted, 1 = orientation, 2 = beamvector, 3 = soundvelocity, 4 = georeference, 5 = tpu.',
                         'qualityfactor': 'The raw uncertainty record that comes from the multibeam.  Corresponds to the Kongsberg detectioninfo (.all) detectiontype (.kmall).  See datagram description for more information.',
                         'rel_azimuth': 'The result of running Compute Beam Vectors in Kluster.  This is the direction to the beam footprint on the seafloor from the sonar in radians.',
                         'soundspeed': 'The surface sound velocimeter data, in meters per second.',
                         'thu': 'The Hare-Godin-Mayer TPU model - horizontal component.  In meters.',
                         'tiltangle': 'Steering angle of the sector transmit beam, in degrees.',
                         'traveltime': 'The two way travel time of each beam in seconds.',
                         'tvu': 'The Hare-Godin-Mayer TPU model - vertical component.  In meters.',
                         'txsector_beam': 'The sector number of each beam.',
                         'x': 'The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Easting is in meters.',
                         'y': 'The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Northing is in meters.',
                         'yawpitchstab': 'Tells you whether yaw/pitch stabilization was enabled on the sonar\nY = Only yaw stab, P = Only pitch stab, PY = Pitch and yaw stab, N = Neither.',
                         'z': 'The result of running Georeference in Kluster.  This is the sound velocity offsets projected into the coordinate reference system you chose.  Depth is in meters from the vertical reference you chose.'
                         }

