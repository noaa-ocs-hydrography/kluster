# generic gui
pass_color = 'color : green'  # color of the gui labels and text where a test passes
error_color = 'color : red'  # color of the gui labels and text where a test does not pass

# kluster_3dview
selected_point_color = (1, 0.476, 0.953, 1)  # color of points selected in 3dview
super_selected_point_color = (1, 1, 1, 1)  # color of points in super selection in 3dview

# generic processing
max_beams = 400  # maximum allowed beams in kluster
epsg_nad83 = 6319
epsg_wgs84 = 7911

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

# xarray conversion
ping_chunk_size = 1000  # chunk size (in pings) of each written chunk of data in the ping records
navigation_chunk_size = 50000  # chunk size (in time) of each written chunk of data in the navigation records
attitude_chunk_size = 20000  # chunk size (in time) of each written chunk of data in the attitude records
single_head_sonar = ['em122', 'em302', 'em710', 'em2045', 'em2040', 'em2040p', 'em3002', 'em2040p', 'em3020', 'me70bo']  # all single head sonar models
dual_head_sonar = ['em2040_dual_rx', 'em2040_dual_tx', 'em2045_dual']  # all dual head sonar models
default_heave_error = 0.05  # default tpu parameter for heave
default_roll_error = 0.0005  # default tpu parameter for roll
default_pitch_error = 0.0005  # default tpu parameter for pitch
default_heading_error = 0.02  # default tpu parameter for heading
default_x_offset_error = 0.2  # default tpu parameter for x offset measurement
default_y_offset_error = 0.2  # default tpu parameter for y offset measurement
default_z_offset_error = 0.2  # default tpu parameter for z offset measurement
default_surface_sv_error = 0.5  # default tpu parameter for surface sv
default_roll_patch_error = 0.1  # default tpu parameter for roll patch
default_pitch_patch_error = 0.1  # default tpu parameter for pitch patch
default_heading_patch_error = 0.5  # default tpu parameter for heading patch
default_latency_patch_error = 0.0  # default tpu parameter for latency patch
default_latency_error = 0.001  # default tpu parameter for latency
default_dynamic_draft_error = 0.1  # default tpu parameter for dynamic draft
default_separation_model_error = 0.0  # default tpu parameter for separation model
default_waterline_error = 0.02  # default tpu parameter for waterline
default_vessel_speed_error = 0.1  # default tpu parameter for vessel speed
default_horizontal_positioning_error = 1.5  # default tpu parameter for horizontal positioning
default_vertical_positioning_error = 1.0  # default tpu parameter for vertical positioning
