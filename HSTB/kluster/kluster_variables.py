# generic gui
pass_color = 'color : green'  # color of the gui labels and text where a test passes
error_color = 'color : red'  # color of the gui labels and text where a test does not pass

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
converted_files_at_once = 10

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
                         'tx_h', 'rx_h']
# optional parameter names controls what is left out when comparing vessel entries to see if the new entry is worth keeping
optional_parameter_names = ['source', 'vessel_file', 'sonar_type', 'imu_h', 'imu_p', 'imu_r', 'imu_x', 'imu_y',
                            'imu_z', 'tx_to_antenna_x', 'tx_to_antenna_y', 'tx_to_antenna_z', 'vess_center_x', 'vess_center_y',
                            'vess_center_z', 'vess_center_yaw', 'vess_center_p', 'vess_center_r', 'sensor_size']

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


variable_format_str = {'time': '%1.6f', 'beam': '%d', 'xyz': '%s',
                       'acrosstrack': '%1.3f', 'alongtrack': '%1.3f', 'altitude': '%1.3f',
                       'beampointingangle': '%1.3f', 'corr_altitude': '%1.3f',
                       'corr_heave': '%1.3f', 'corr_pointing_angle': '%1.6f',
                       'counter': '%d', 'datum_uncertainty': '%1.3f', 'delay': '%1.6f',
                       'depthoffset': '%1.3f', 'detectioninfo': '%d', 'frequency': '%d',
                       'latitude': '%1.8f', 'longitude': '%1.8f', 'mode': '%s', 'modetwo': '%s', 'ntx': '%d',
                       'processing_status': '%d', 'qualityfactor': '%d', 'rel_azimuth': '%f',
                       'rx': '%f', 'soundspeed': '%1.3f', 'thu': '%1.3f', 'tiltangle': '%1.3f',
                       'traveltime': '%1.6f', 'tvu': '%1.3f', 'tx': '%f', 'txsector_beam': '%d',
                       'x': '%1.3f', 'y': '%1.3f', 'yawpitchstab': '%s', 'z': '%1.3f',
                       'alongtrackvelocity': '%1.3f', 'down_position_error': '%1.3f', 'east_position_error': '%1.3f',
                       'north_position_error': '%1.3f', 'pitch_error': '%1.3', 'roll_error': '%1.3f',
                       'heading_error': '%1.3f', 'heading': '%1.3f', 'heave': '%1.3f', 'pitch': '%1.3f',
                       'roll': '%1.3f'}
