import os
import logging
from copy import deepcopy
import numpy as np
from datetime import datetime
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, backend

from vispy import use
use(backend, 'gl2')

from vispy import scene
from vispy.io import read_mesh
from vispy.visuals import transforms
from vispy.color import Color

from HSTB.kluster.xarray_conversion import return_xyzrph_from_posmv, return_xyzrph_from_mbes
from HSTB.kluster.fqpr_vessel import VesselFile
from HSTB.kluster import kluster_variables
from HSTB.kluster.gui.common_widgets import AcceptDialog
from HSTB.shared import RegistryHelpers

launch_sensor_size = 0.6
v4_target_sensing_center_offset = np.array([-0.008, -0.031, 0.130])
v5_target_sensing_center_offset = np.array([0.005, -0.006, 0.089])
hide_location = np.array([0, 0, 1000.0])

test_xyzrph = {'123': {'sonar_type': {'1503413148': 'em2040', '1503423148': 'em2040', '1503443148': 'em2040'},
                       'source': {'1503413148': 'sonarfile1.all', '1503423148': 'sonarfile2.all', '1503443148': 'sonarfile3.all'},
                       'heading_patch_error': {'1503413148': 0.5, '1503423148': 0.5, '1503443148': 0.5},
                       'heading_sensor_error': {'1503413148': 0.02, '1503423148': 0.02, '1503443148': 0.02},
                       'heave_error': {'1503413148': 0.05, '1503423148': 0.05, '1503443148': 0.05},
                       'horizontal_positioning_error': {'1503413148': 1.5, '1503423148': 1.5, '1503443148': 1.5},
                       'latency': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'latency_patch_error': {'1503413148': 0.0, '1503423148': 0.0, '1503443148': 0.0},
                       'pitch_patch_error': {'1503413148': 0.1, '1503423148': 0.1, '1503443148': 0.1},
                       'pitch_sensor_error': {'1503413148': 0.0005, '1503423148': 0.0005, '1503443148': 0.0005},
                       'roll_patch_error': {'1503413148': 0.1, '1503423148': 0.1, '1503443148': 0.1},
                       'roll_sensor_error': {'1503413148': 0.0005, '1503423148': 0.0005, '1503443148': 0.0005},
                       'separation_model_error': {'1503413148': 0.0, '1503423148': 0.0, '1503443148': 0.0},
                       'surface_sv_error': {'1503413148': 0.5, '1503423148': 0.5, '1503443148': 0.5},
                       'timing_latency_error': {'1503413148': 0.001, '1503423148': 0.001, '1503443148': 0.001},
                       'vertical_positioning_error': {'1503413148': 1.0, '1503423148': 1.0, '1503443148': 1.0},
                       'vessel_speed_error': {'1503413148': 0.1, '1503423148': 0.1, '1503443148': 0.1},
                       'waterline_error': {'1503413148': '-0.640', '1503423148': '-0.640', '1503443148': '-0.640'},
                       'x_offset_error': {'1503413148': 0.2, '1503423148': 0.2, '1503443148': 0.2},
                       'y_offset_error': {'1503413148': 0.2, '1503423148': 0.2, '1503443148': 0.2},
                       'z_offset_error': {'1503413148': 0.2, '1503423148': 0.2, '1503443148': 0.2},
                       'tx_to_antenna_x': {'1503413148': '-0.889', '1503423148': '-0.889', '1503443148': '-0.889'},
                       'tx_to_antenna_y': {'1503413148': '-0.923', '1503423148': '-0.923', '1503443148': '-0.923'},
                       'tx_to_antenna_z': {'1503413148': '-4.193', '1503423148': '-4.193', '1503443148': '-4.193'},
                       'imu_h': {'1503413148': '0.29', '1503423148': '0.30', '1503443148': '0.31'},
                       'imu_p': {'1503413148': '-0.109', '1503423148': '-0.109', '1503443148': '-0.109'},
                       'imu_r': {'1503413148': '-0.231', '1503423148': '-0.231', '1503443148': '-0.231'},
                       'imu_x': {'1503413148': '-0.152', '1503423148': '-0.152', '1503443148': '-0.152'},
                       'imu_y': {'1503413148': '-0.195', '1503423148': '-0.195', '1503443148': '-0.195'},
                       'imu_z': {'1503413148': '-0.449', '1503423148': '-0.449', '1503443148': '-0.449'},
                       'rx_h': {'1503413148': '180.000', '1503423148': '180.000', '1503443148': '180.000'},
                       'rx_p': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_r': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_opening_angle': {'1503413148': 1.0, '1503423148': 1.0, '1503443148': 1.0},
                       'rx_stbd_h': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_stbd_p': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_stbd_r': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_stbd_opening_angle': {'1503413148': 1.0, '1503423148': 1.0, '1503443148': 1.0},
                       'rx_stbd_x': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'rx_stbd_y': {'1503413148': '-0.000', '1503423148': '-0.000', '1503443148': '-0.000'},
                       'rx_stbd_z': {'1503413148': '-1000.000', '1503423148': '-1000.000', '1503443148': '-1000.000'},
                       'rx_x': {'1503413148': '0.105', '1503423148': '0.105', '1503443148': '0.105'},
                       'rx_x_0': {'1503413148': '0.011', '1503423148': '0.011', '1503443148': '0.011'},
                       'rx_x_1': {'1503413148': '0.011', '1503423148': '0.011', '1503443148': '0.011'},
                       'rx_x_2': {'1503413148': '0.011', '1503423148': '0.011',  '1503443148': '0.011'},
                       'rx_y': {'1503413148': '-0.311', '1503423148': '-0.311', '1503443148': '-0.311'},
                       'rx_y_0': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'rx_y_1': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'rx_y_2': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'rx_z': {'1503413148': '-0.017', '1503423148': '-0.017', '1503443148': '-0.017'},
                       'rx_z_0': {'1503413148': '-0.006', '1503423148': '-0.006', '1503443148': '-0.006'},
                       'rx_z_1': {'1503413148': '-0.006', '1503423148': '-0.006', '1503443148': '-0.006'},
                       'rx_z_2': {'1503413148': '-0.006', '1503423148': '-0.006', '1503443148': '-0.006'},
                       'tx_h': {'1503413148': '0.000', '1503423148': '0.000',  '1503443148': '0.000'},
                       'tx_p': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_r': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_opening_angle': {'1503413148': 1.0, '1503423148': 1.0, '1503443148': 1.0},
                       'tx_stbd_h': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_stbd_p': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_stbd_r': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_stbd_opening_angle': {'1503413148': 1.0, '1503423148': 1.0, '1503443148': 1.0},
                       'tx_stbd_x': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_stbd_y': {'1503413148': '-0.000', '1503423148': '-0.000',  '1503443148': '-0.000'},
                       'tx_stbd_z': {'1503413148': '-1000.000', '1503423148': '-1000.000', '1503443148': '-1000.000'},
                       'tx_x': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_x_0': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'tx_x_1': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'tx_x_2': {'1503413148': '0.0', '1503423148': '0.0', '1503443148': '0.0'},
                       'tx_y': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_y_0': {'1503413148': '-0.0554', '1503423148': '-0.0554', '1503443148': '-0.0554'},
                       'tx_y_1': {'1503413148': '0.0131', '1503423148': '0.0131', '1503443148': '0.0131'},
                       'tx_y_2': {'1503413148': '0.0554', '1503423148': '0.0554', '1503443148': '0.0554'},
                       'tx_z': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'tx_z_0': {'1503413148': '-0.012', '1503423148': '-0.012', '1503443148': '-0.012'},
                       'tx_z_1': {'1503413148': '-0.006', '1503423148': '-0.006', '1503443148': '-0.006'},
                       'tx_z_2': {'1503413148': '-0.012', '1503423148': '-0.012', '1503443148': '-0.012'},
                       'vess_center_p': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'vess_center_r': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'vess_center_x': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.800'},
                       'vess_center_y': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'vess_center_yaw': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'vess_center_z': {'1503413148': '0.000', '1503423148': '0.000', '1503443148': '0.000'},
                       'vessel_file': {'1503413148': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj',
                                       '1503423148': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj',
                                       '1503443148': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj'},
                       'waterline': {'1503413148': '-1.010', '1503423148': '-0.910', '1503443148': '-1.310'}}}

test_xyzrph_dual = {'541': {'sonar_type': {'1583305645': 'em2040_dual_rx', '1583315645': 'em2040_dual_rx', '1583335645': 'em2040_dual_rx'},
                            'source': {'1583305645': 'sonarfile4.all', '1583315645': 'sonarfile5.all', '1583335645': 'sonarfile6.all'},
                            'heading_patch_error': {'1583305645': 0.5, '1583315645': 0.5, '1583335645': 0.5},
                            'heading_sensor_error': {'1583305645': 0.02, '1583315645': 0.02, '1583335645': 0.02},
                            'heave_error': {'1583305645': 0.05, '1583315645': 0.05, '1583335645': 0.05},
                            'horizontal_positioning_error': {'1583305645': 1.5, '1583315645': 1.5, '1583335645': 1.5},
                            'latency': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'latency_patch_error': {'1583305645': 0.0, '1583315645': 0.0, '1583335645': 0.0},
                            'pitch_patch_error': {'1583305645': 0.1, '1583315645': 0.1, '1583335645': 0.1},
                            'pitch_sensor_error': {'1583305645': 0.0005, '1583315645': 0.0005, '1583335645': 0.0005},
                            'roll_patch_error': {'1583305645': 0.1, '1583315645': 0.1, '1583335645': 0.1},
                            'roll_sensor_error': {'1583305645': 0.0005, '1583315645': 0.0005, '1583335645': 0.0005},
                            'separation_model_error': {'1583305645': 0.0, '1583315645': 0.0, '1583335645': 0.0},
                            'surface_sv_error': {'1583305645': 0.5, '1583315645': 0.5, '1583335645': 0.5},
                            'timing_latency_error': {'1583305645': 0.001, '1583315645': 0.001, '1583335645': 0.001},
                            'vertical_positioning_error': {'1583305645': 1.0, '1583315645': 1.0, '1583335645': 1.0},
                            'vessel_speed_error': {'1583305645': 0.1, '1583315645': 0.1, '1583335645': 0.1},
                            'waterline_error': {'1583305645': '-0.640', '1583315645': '-0.640', '1583335645': '-0.640'},
                            'x_offset_error': {'1583305645': 0.2, '1583315645': 0.2, '1583335645': 0.2},
                            'y_offset_error': {'1583305645': 0.2, '1583315645': 0.2, '1583335645': 0.2},
                            'z_offset_error': {'1583305645': 0.2, '1583315645': 0.2, '1583335645': 0.2},
                            'tx_to_antenna_x': {'1583305645': '2.569', '1583315645': '2.569', '1583335645': '2.569'},
                            'tx_to_antenna_y': {'1583305645': '-2.978', '1583315645': '-2.978', '1583335645': '-2.978'},
                            'tx_to_antenna_z': {'1583305645': '-12.975', '1583315645': '-12.975', '1583335645': '-12.975'},
                            'imu_h': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'imu_p': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'imu_r': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'imu_x': {'1583305645': '0.005', '1583315645': '0.005', '1583335645': '0.005'},
                            'imu_y': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'imu_z': {'1583305645': '0.089', '1583315645': '0.089', '1583335645': '0.089'},
                            'rx_port_h': {'1583305645': '0.899', '1583315645': '0.899', '1583335645': '0.899'},
                            'rx_port_p': {'1583305645': '0.363', '1583315645': '0.363', '1583335645': '0.363'},
                            'rx_port_r': {'1583305645': '4.274', '1583315645': '4.274', '1583335645': '4.274'},
                            'rx_port_opening_angle': {'1583305645': 1.0, '1583315645': 1.0, '1583335645': 1.0},
                            'rx_port_x': {'1583305645': '0.495', '1583315645': '0.495', '1583335645': '0.495'},
                            'rx_port_x_0': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_port_x_1': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_port_x_2': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_port_y': {'1583305645': '-13.598', '1583315645': '-13.598', '1583335645': '-13.598'},
                            'rx_port_y_0': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_port_y_1': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_port_y_2': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_port_z': {'1583305645': '1.282', '1583315645': '1.282', '1583335645': '1.282'},
                            'rx_port_z_0': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'rx_port_z_1': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'rx_port_z_2': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'rx_stbd_h': {'1583305645': '1.035', '1583315645': '1.035', '1583335645': '1.035'},
                            'rx_stbd_p': {'1583305645': '-0.166', '1583315645': '-0.166', '1583335645': '-0.166'},
                            'rx_stbd_r': {'1583305645': '-3.448', '1583315645': '-3.448', '1583335645': '-3.448'},
                            'rx_stbd_opening_angle': {'1583305645': 1.0, '1583315645': 1.0, '1583335645': 1.0},
                            'rx_stbd_x': {'1583305645': '0.331', '1583315645': '0.331', '1583335645': '0.331'},
                            'rx_stbd_x_0': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_stbd_x_1': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_stbd_x_2': {'1583305645': '0.011', '1583315645': '0.011', '1583335645': '0.011'},
                            'rx_stbd_y': {'1583305645': '1.251', '1583315645': '1.251', '1583335645': '1.251'},
                            'rx_stbd_y_0': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_stbd_y_1': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_stbd_y_2': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'rx_stbd_z': {'1583305645': '1.385', '1583315645': '1.385', '1583335645': '1.385'},
                            'rx_stbd_z_0': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'rx_stbd_z_1': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'rx_stbd_z_2': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'tx_port_h': {'1583305645': '1.045', '1583315645': '1.045', '1583335645': '1.045'},
                            'tx_port_p': {'1583305645': '0.363', '1583315645': '0.363', '1583335645': '0.363'},
                            'tx_port_r': {'1583305645': '4.374', '1583315645': '4.374', '1583335645': '4.374'},
                            'tx_port_opening_angle': {'1583305645': 1.0, '1583315645': 1.0, '1583335645': 1.0},
                            'tx_port_x': {'1583305645': '0.595', '1583315645': '0.595', '1583335645': '0.595'},
                            'tx_port_x_0': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_port_x_1': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_port_x_2': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_port_y': {'1583305645': '-13.292', '1583315645': '-13.292', '1583335645': '-13.292'},
                            'tx_port_y_0': {'1583305645': '-0.0554', '1583315645': '-0.0554', '1583335645': '-0.0554'},
                            'tx_port_y_1': {'1583305645': '0.0131', '1583315645': '0.0131', '1583335645': '0.0131'},
                            'tx_port_y_2': {'1583305645': '0.0554', '1583315645': '0.0554', '1583335645': '0.0554'},
                            'tx_port_z': {'1583305645': '1.319', '1583315645': '1.319', '1583335645': '1.319'},
                            'tx_port_z_0': {'1583305645': '-0.012', '1583315645': '-0.012', '1583335645': '-0.012'},
                            'tx_port_z_1': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'tx_port_z_2': {'1583305645': '-0.012', '1583315645': '-0.012', '1583335645': '-0.012'},
                            'tx_stbd_h': {'1583305645': '1.142', '1583315645': '1.142', '1583335645': '1.142'},
                            'tx_stbd_p': {'1583305645': '-0.068', '1583315645': '-0.068', '1583335645': '-0.068'},
                            'tx_stbd_r': {'1583305645': '-0.085', '1583315645': '-0.085', '1583335645': '-0.085'},
                            'tx_stbd_opening_angle': {'1583305645': 1.0, '1583315645': 1.0, '1583335645': 1.0},
                            'tx_stbd_x': {'1583305645': '0.427', '1583315645': '0.427', '1583335645': '0.427'},
                            'tx_stbd_x_0': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_stbd_x_1': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_stbd_x_2': {'1583305645': '0.0', '1583315645': '0.0', '1583335645': '0.0'},
                            'tx_stbd_y': {'1583305645': '1.559', '1583315645': '1.559', '1583335645': '1.559'},
                            'tx_stbd_y_0': {'1583305645': '-0.0554', '1583315645': '-0.0554', '1583335645': '-0.0554'},
                            'tx_stbd_y_1': {'1583305645': '0.0131', '1583315645': '0.0131', '1583335645': '0.0131'},
                            'tx_stbd_y_2': {'1583305645': '0.0554', '1583315645': '0.0554', '1583335645': '0.0554'},
                            'tx_stbd_z': {'1583305645': '1.381', '1583315645': '1.381', '1583335645': '1.381'},
                            'tx_stbd_z_0': {'1583305645': '-0.012', '1583315645': '-0.012', '1583335645': '-0.012'},
                            'tx_stbd_z_1': {'1583305645': '-0.006', '1583315645': '-0.006', '1583335645': '-0.006'},
                            'tx_stbd_z_2': {'1583305645': '-0.012', '1583315645': '-0.012', '1583335645': '-0.012'},
                            'vess_center_p': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vess_center_r': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vess_center_x': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vess_center_y': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vess_center_yaw': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vess_center_z': {'1583305645': '0.000', '1583315645': '0.000', '1583335645': '0.000'},
                            'vessel_file': {'1583305645': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj',
                                            '1583315645': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj',
                                            '1583335645': r'C:\\PydroXL_19\\NOAA\\site-packages\\Python38\\HSTB\\kluster\\gui\\vessel_stl_files\\westcoast_28ft_launch.obj'},
                            'waterline': {'1583305645': '-2.383', '1583315645': '-2.383', '1583335645': '-2.383'}}}


def new_cube(parent, size, color, origin):
    """
    Vispy helper, generate a new cube in the parent scene view.  Move the cube to the origin provided.

    Parameters
    ----------
    parent: vispy.scene, scene containing the objects for display
    size: float, size in meters
    color: RGB/RGBA numpy array, other inputs also acceptable, see vispy Color array
    origin: numpy array, 3 element array, xyz origin, given in meters

    Returns
    -------
    vispy.scene.visuals.Cube object

    """
    my_cube = scene.visuals.Cube(size=size * 1000, color=color, parent=parent)  # expects units in mm
    my_cube.transform = transforms.MatrixTransform()
    my_cube.transform.translate(origin * 1000)  # expects units in mm
    return my_cube


def new_plane(parent, width, height, color, origin):
    """
    Vispy helper, generate a new plane in the parent scene view.  Move the plane to the origin provided.

    Currently assumes you want the plane to face the z direction

    Parameters
    ----------
    parent: vispy.scene, scene containing the objects for display
    width: float, width in meters
    height: float, width in meters
    color: RGB/RGBA numpy array, other inputs also acceptable, see vispy Color array
    origin: numpy array, 3 element array, xyz origin, given in meters

    Returns
    -------
    vispy.scene.visuals.Cube object

    """
    my_plane = scene.visuals.Plane(width * 1000, height * 1000, 1, 1, direction='+z', color=color, parent=parent)  # expects units in mm
    my_plane.transform = transforms.MatrixTransform()
    my_plane.transform.translate(origin * 1000)  # expects units in mm
    return my_plane


class MovingObject:
    """
    Base class for vispy objects.  Anything that needs translation/rotation inherits from this.

    We want to work in:
    - X = + Forward, Y = + Starboard, Z = + Down
    - roll = + Port Up, pitch = + Bow Up, gyro = + Clockwise

    Vispy plots using:
    - X = + Forward, Y = + Port, Z = + Up
    - roll = + Port Up, pitch = + Bow Down, gyro = + CounterClockwise

    So we have some flip switches on setting position that we use to allow us to work externally using the coordinates
    we want to use, and internally it converts to Vispy desired coordinates for plotting.

    """
    def __init__(self, parent=None, size=None, width=None, height=None, color=None, origin=np.array([0, 0, 0]),
                 name='', debug=False):
        self.parent = parent
        self.mesh = None
        self.size = size
        self.width = width
        self.height = height
        self.color = color
        self.origin = origin
        self.position = origin
        self.rotation_history = []
        self.name = name
        self.debug = debug

        self.old_position = origin

    def set_position(self, pos, from_meters=True, flip_z=True, flip_y=True, flip_x=False):
        """
        translate the sensor to the location provided.  translations are relative to current position, so you have
        to back out your old position before moving it.

        Parameters
        ----------
        pos: numpy array, 3 element array, xyz origin
        from_meters: bool, if input is not from meters, converts to it assuming mm
        flip_z: bool, if True, flips the sign for the z value.  We expect z to be positive down, Vispy expects
                positive up.  Set this to true if your z is positive down.
        flip_y: bool, if True, flips the sign for the y value.  We expect y to be positive starboard, Vispy expects
                positive port  Set this to true if your y is positive starboard.
        flip_x: bool, if True, flips the sign for the x value.  We expect x to be positive forward, Vispy expects
                positive forward as well, you probably wont need this.

        """
        self.old_position = self.position
        if self.debug:
            print('{} set_position {} {}'.format(self.name, pos, from_meters))
        if not from_meters:  # mm is provided
            pos = pos / 1000  # we want to work internally in meters
        if flip_z:
            pos[2] = -pos[2]
        if flip_y:
            pos[1] = -pos[1]
        if flip_x:
            pos[0] = -pos[0]
        newpos = -self.position + pos + self.origin
        if self.debug:
            print('-- {} = -{} + {} + {}'.format(newpos, self.position, pos, self.origin))
        self.position = pos + self.origin
        if self.mesh is not None:  # mesh is None when we test
            if from_meters:
                newpos = newpos * 1000  # convert to mm
            self.mesh.transform.translate(newpos)  # expects units in mm
        if self.debug:
            print('-- pos {} new transform {} currorigin {}'.format(self.position, newpos, self.origin))

    def set_rotation(self, rot, from_deg=True, flip_r=False, flip_p=True, flip_yaw=True):
        """
        rotate object in Tate-Bryant order (yaw-pitch-roll).  Rotations are relative to current orientation, if a rotation has
        been performed already, this will back it out to perform the new rotation.

        Vispy expects pitch = + Bow Down and gyro = + CounterClockwise so flip those values

        Parameters
        ----------
        rot: numpy array, 3 elements [roll, pitch, yaw] angles
        from_deg: bool, if rot is in degrees, this is True
        flip_r: bool, if True, flips the sign for the roll value.  We expect roll to be + Port Up, Vispy expects
                port up as well.  Probably wont need this
        flip_p: bool, if True, flips the sign for the pitch value.  We expect pitch to be + Bow Up, Vispy expects
                positive bow down.  Set this to true if your pitch is positive bow up (most of the time)
        flip_yaw: bool, if True, flips the sign for the yaw value.  We expect x to be positive clockwise, Vispy expects
                positive counterclockwise, Set to true if your yaw is positive clockwise (most of the time)

        """
        if self.debug:
            print('{} set_rotation {} {}'.format(self.name, rot, from_deg))
        if not from_deg:
            rot = np.rad2deg(rot)

        if flip_r:
            rot[0] = -rot[0]
        if flip_p:
            rot[1] = -rot[1]
        if flip_yaw:
            rot[2] = -rot[2]
        if self.rotation_history:
            for old_rot in self.rotation_history:
                if self.mesh is not None:  # mesh is None when we test
                    self.mesh.transform.rotate(old_rot[0], old_rot[1])
        self.rotation_history = []
        self.rotation_history.append([-rot[0], (1, 0, 0)])
        self.rotation_history.append([-rot[1], (0, 1, 0)])
        self.rotation_history.append([-rot[2], (0, 0, 1)])
        if self.mesh is not None:  # mesh is None when we test
            self.mesh.transform.rotate(rot[2], (0, 0, 1))
            self.mesh.transform.rotate(rot[1], (0, 1, 0))
            self.mesh.transform.rotate(rot[0], (1, 0, 0))

    def set_origin(self, pos, pos_offset=np.array([0, 0, 0]), from_meters=True, flip_z=True, flip_y=True, flip_x=False):
        """
        set the origin for this object.  Will zero out the position (such that the position is now equal to the origin)
        unless you provide a pos_offset which serves as the new position.

        Parameters
        ----------
        pos: numpy array, 3 element array, xyz origin
        pos_offset: numpy array, 3 element array, xyz position
        from_meters: bool, if input is not from meters, converts to it assuming mm
        flip_z: bool, if True, flips the sign for the z value.  We expect z to be positive down, Vispy expects
                positive up.  Set this to true if your z is positive down.
        flip_y: bool, if True, flips the sign for the y value.  We expect y to be positive starboard, Vispy expects
                positive port  Set this to true if your y is positive starboard.
        flip_x: bool, if True, flips the sign for the x value.  We expect x to be positive forward, Vispy expects
                positive forward as well, you probably wont need this.

        """
        if self.debug:
            print('{} set_origin {} {}, old origin {}'.format(self.name, self.name, pos, from_meters))
        if flip_z:
            pos[2] = -pos[2]
        if flip_y:
            pos[1] = -pos[1]
        if flip_x:
            pos[0] = -pos[0]
        self.set_position(pos_offset, from_meters=from_meters)
        if not from_meters:  # mm is provided
            pos = pos / 1000  # we want to work internally in meters
        self.origin = pos
        if self.debug:
            print('-- neworigin {} newpos {}'.format(self.origin, pos_offset))

    def finalize_position(self, pos, from_meters=True, flip_z=True, flip_y=True, flip_x=False):
        """
        After initializing the object, we want to set the origin and position provided.  This is a separate method, as
        we want to trigger this after the creation of the Vispy mesh, which is done on init from one of the classes
        that inherits this.

        Parameters
        ----------
        pos: numpy array, 3 element array, xyz origin
        from_meters: bool, if input is not from meters, converts to it assuming mm
        flip_z: bool, if True, flips the sign for the z value.  We expect z to be positive down, Vispy expects
                positive up.  Set this to true if your z is positive down.
        flip_y: bool, if True, flips the sign for the y value.  We expect y to be positive starboard, Vispy expects
                positive port  Set this to true if your y is positive starboard.
        flip_x: bool, if True, flips the sign for the x value.  We expect x to be positive forward, Vispy expects
                positive forward as well, you probably wont need this.

        """
        self.set_origin(self.origin, from_meters=from_meters, flip_z=flip_z, flip_y=flip_y, flip_x=flip_x)
        if pos is not None:
            self.set_position(pos, from_meters=from_meters, flip_z=flip_z, flip_y=flip_y, flip_x=flip_x)
        self.rotation_history = []  # holds previous rotations that have been performed (so we can reverse them)


class Waterline(MovingObject):
    """
    MovingObject that is specifically a plane, with the relevant methods we need for the waterline object
    """
    def __init__(self, mesh_type, position, **kwargs):
        super().__init__(**kwargs)
        self.mesh_type = mesh_type
        if self.mesh_type == 'plane':
            self.mesh = new_plane(self.parent, self.width, self.height, self.color, self.origin)
        else:
            raise ValueError('Only "plane" is supported for Waterline class')
        self.finalize_position(position)

    def toggle_sensor(self, state=0):
        """
        Vispy helper function, take the vispy.scene.visuals object (sensor) and adjust the alpha based on the provided state.

        state = 0, hidden
        state = 1, dim
        state = 2, bright

        Parameters
        ----------
        state: int, state, see docstring

        """
        # state is as follows, 0 = hidden, 1 = dim, 2 = bright
        if self.mesh is not None:  # mesh is None when we test
            curr_rgba = self.mesh._mesh.color.rgba
            if state == 0:
                if self.debug:
                    print('{} toggle_sensor {} hidden'.format(self.name, state))
                curr_rgba[3] = 0
                self.set_position(hide_location, from_meters=True, flip_z=True, flip_y=True, flip_x=False)
            elif state == 1:
                if self.debug:
                    print('{} toggle_sensor {} dim'.format(self.name, state))
                curr_rgba[3] = 0.2
                if np.array_equal(self.position, hide_location):
                    self.set_position(self.old_position, from_meters=True, flip_z=False, flip_y=False, flip_x=False)
            elif state == 2:
                if self.debug:
                    print('{} toggle_sensor {} bright'.format(self.name, state))
                curr_rgba[3] = 0.6
                if np.array_equal(self.position, hide_location):
                    self.set_position(self.old_position, from_meters=True, flip_z=False, flip_y=False, flip_x=False)
            else:
                raise ValueError('State must be between 0 and 2')
            self.mesh._mesh.color = curr_rgba

    def get_sensor_state(self):
        """
        Return the current visibility state of the object

        Returns
        -------
        state: int, current state of the object.  state is as follows, 0 = hidden, 1 = dim, 2 = bright

        """
        # state is as follows, 0 = hidden, 1 = dim, 2 = bright
        if self.mesh is not None:  # mesh is None when we test
            curr_rgba = self.mesh._mesh.color.rgba
            if curr_rgba[3] == 0:
                state = 0
            elif curr_rgba[3] <= 0.2:
                state = 1
            else:
                state = 2
            return state
        return None


class Sensor(MovingObject):
    """
    Widget that can move/rotate and contains a Cube mesh.  Allows for visibility changes, resizing, etc.
    """
    def __init__(self, mesh_type, position, **kwargs):
        super().__init__(**kwargs)
        self.mesh_type = mesh_type
        if self.mesh_type == 'cube':
            self.mesh = new_cube(self.parent, self.size, self.color, self.origin)
        else:
            raise ValueError('Only "cube" is supported for Sensor class')
        self.finalize_position(position)

    def toggle_sensor(self, state=0):
        """
        Vispy helper function, take the vispy.scene.visuals object (sensor) and adjust the alpha based on the provided state.

        state = 0, hidden
        state = 1, dim
        state = 2, bright

        Parameters
        ----------
        state: int, state, see docstring

        """
        # state is as follows, 0 = hidden, 1 = dim, 2 = bright
        if self.mesh is not None:  # mesh is None when we test
            curr_rgba = self.mesh.mesh.color.rgba
            if state == 0:
                if self.debug:
                    print('{} toggle_sensor {} hidden'.format(self.name, state))
                curr_rgba[3] = 0
            elif state == 1:
                if self.debug:
                    print('{} toggle_sensor {} dim'.format(self.name, state))
                curr_rgba[3] = 0.2
            elif state == 2:
                if self.debug:
                    print('{} toggle_sensor {} bright'.format(self.name, state))
                curr_rgba[3] = 1
            else:
                raise ValueError('State must be between 0 and 2')
            self.mesh.mesh.color = curr_rgba

    def resize(self, new_size):
        """
        No simple way to resize the vispy mesh object.  The only way I could swing it is to regenerate the object
        with the new size and set the origin/position.

        Parameters
        ----------
        new_size: float, size of object

        """
        self.mesh.parent = None
        self.size = new_size

        if self.mesh_type == 'cube':
            self.mesh = new_cube(self.parent, self.size, self.color, self.origin)

        newpos = self.position
        self.position = np.array([0, 0, 0])
        # since newpos is the position already set for the object, we don't have to do any of the flipping but we do
        #   need to set it up to accept position in meters.
        self.set_position(newpos, from_meters=True, flip_x=False, flip_y=False, flip_z=False)

    def get_sensor_state(self):
        """
        Return the current visibility state of the object

        Returns
        -------
        state: int, current state of the object.  state is as follows, 0 = hidden, 1 = dim, 2 = bright

        """
        # state is as follows, 0 = hidden, 1 = dim, 2 = bright
        if self.mesh is not None:  # mesh is None when we test
            curr_rgba = self.mesh.mesh.color.rgba
            if curr_rgba[3] == 0:
                state = 0
            elif curr_rgba[3] <= 0.2:
                state = 1
            else:
                state = 2
            return state
        return None


class Vessel(MovingObject):
    """
    Vessel is the 3d model for the boat that we display in the vispy scene.  Only really need the translation/rotation
    stuff from MovingObject plus the Vispy mesh
    """
    def __init__(self, vessel_file, position, **kwargs):
        super().__init__(**kwargs)
        self.vessel_file = vessel_file
        self.vertices, self.faces, self.normals, self.texcoords = read_mesh(vessel_file)
        self.mesh = scene.visuals.Mesh(vertices=None, faces=None, color=self.color,
                                       parent=self.parent)
        self.mesh.transform = transforms.MatrixTransform()
        self.mesh.set_data(vertices=self.vertices, faces=self.faces)
        # self.mesh.light_dir = (10, 10, 10)

        if position is not None:
            self.finalize_position(position)


class TimestampDialog(QtWidgets.QDialog):
    """
    Dialog that allows the user to take in a datetime, and return a modified version
    """
    def __init__(self, parent=None, settings=None):
        super().__init__(parent)

        self.setWindowTitle('Edit Timestamp')
        layout = QtWidgets.QVBoxLayout()

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.serial_label = QtWidgets.QLabel('Serial Number')
        self.hlayout_one.addWidget(self.serial_label)
        self.serial_text = QtWidgets.QLabel()
        self.hlayout_one.addWidget(self.serial_text)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.orig_label = QtWidgets.QLabel('Original Timestamp')
        self.hlayout_two.addWidget(self.orig_label)
        self.orig_text = QtWidgets.QLabel()
        self.hlayout_two.addWidget(self.orig_text)

        self.directions = QtWidgets.QLabel('Timestamp Format    "MM/DD/YY hhmm"')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.new_label = QtWidgets.QLabel('New Timestamp')
        self.hlayout_three.addWidget(self.new_label)
        self.new_text = QtWidgets.QLineEdit()
        self.hlayout_three.addWidget(self.new_text)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.remove_this_checkbox = QtWidgets.QCheckBox('Remove this entry')

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.hlayout_five.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_five.addWidget(self.ok_button)
        self.hlayout_five.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_five.addWidget(self.cancel_button)
        self.hlayout_five.addStretch(1)

        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_two)
        layout.addStretch()
        layout.addWidget(self.directions)
        layout.addStretch()
        layout.addLayout(self.hlayout_three)
        layout.addWidget(self.remove_this_checkbox)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.new_text.textChanged.connect(self._event_update_status)
        self.remove_this_checkbox.clicked.connect(self._event_checkbox)
        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)

        self._event_update_status()

    def populate(self, serialnum, timestamp):
        """
        Fill the dialog with the serial number of the system and the timestamp that we want to modify

        Parameters
        ----------
        serialnum
            string identifier for the serial number
        timestamp
            raw timestamp in Month/Day/Year Hour/Minute format
        """

        self.serial_text.setText(str(serialnum))
        self.orig_text.setText(str(timestamp))
        self.new_text.setText(str(timestamp))

    def _event_update_status(self):
        """
        Update the status message if an Error presents itself.  Also controls the OK button, to prevent kicking off
        a process if we know it isn't going to work
        """

        newtext = self.new_text.text()
        checkpass = True
        try:
            formatted = datetime.strptime(newtext, '%m/%d/%Y %H%M')
        except ValueError:
            checkpass = False
        if len(newtext) != 15:
            checkpass = False

        if checkpass:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('Pass')
            self.ok_button.setEnabled(True)
        else:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: Timestamp Format Invalid')
            self.ok_button.setEnabled(False)

    def _event_checkbox(self):
        if self.remove_this_checkbox.isChecked():
            self.new_text.setDisabled(True)
        else:
            self.new_text.setDisabled(False)

    def return_new_timestamp(self):
        """
        Return the new timestamp

        Returns
        -------
        str
            new timestamp in Month/Day/Year Hour/Minute format
        """

        formatted_timestamp = self.new_text.text()
        formatted_timestamp = datetime.strptime(formatted_timestamp, '%m/%d/%Y %H%M')
        return str(int(formatted_timestamp.timestamp())), self.remove_this_checkbox.isChecked()

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_new_timestamp to get access to the
        settings the user entered into the dialog.
        """
        self.canceled = False
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


class AddEntryDialog(QtWidgets.QDialog):
    """
    Dialog that allows the user to take in a datetime, and return a modified version
    """
    def __init__(self, parent=None, settings=None):
        super().__init__(parent)

        self.setWindowTitle('Add New Entry')
        layout = QtWidgets.QVBoxLayout()

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.serial_label = QtWidgets.QLabel('Serial Number')
        self.hlayout_one.addWidget(self.serial_label)
        self.serial_text = QtWidgets.QLabel()
        self.hlayout_one.addWidget(self.serial_text)

        self.directions = QtWidgets.QLabel('Timestamp Format    "MM/DD/YY hhmm"')

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.new_label = QtWidgets.QLabel('New Timestamp')
        self.hlayout_two.addWidget(self.new_label)
        self.new_text = QtWidgets.QLineEdit()
        self.hlayout_two.addWidget(self.new_text)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.hlayout_five.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_five.addWidget(self.ok_button)
        self.hlayout_five.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_five.addWidget(self.cancel_button)
        self.hlayout_five.addStretch(1)

        layout.addLayout(self.hlayout_one)
        layout.addStretch()
        layout.addWidget(self.directions)
        layout.addLayout(self.hlayout_two)
        layout.addStretch()
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.new_text.textChanged.connect(self._event_update_status)
        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)

        self._event_update_status()

    def populate(self, serialnum, timestamp):
        """
        Fill the dialog with the serial number of the system and the timestamp that we want to modify

        Parameters
        ----------
        serialnum
            string identifier for the serial number
        timestamp
            raw timestamp in Month/Day/Year Hour/Minute format
        """

        self.serial_text.setText(str(serialnum))
        self.new_text.setText(str(timestamp))

    def _event_update_status(self):
        """
        Update the status message if an Error presents itself.  Also controls the OK button, to prevent kicking off
        a process if we know it isn't going to work
        """

        newtext = self.new_text.text()
        timepass = True
        try:
            formatted = datetime.strptime(newtext, '%m/%d/%Y %H%M')
        except ValueError:
            timepass = False
        if len(newtext) != 15:
            timepass = False

        if timepass:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('Pass')
            self.ok_button.setEnabled(True)
        else:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: Timestamp Format Invalid')
            self.ok_button.setEnabled(False)

    def return_new_timestamp(self):
        """
        Return the new timestamp

        Returns
        -------
        str
            new timestamp in Month/Day/Year Hour/Minute format
        """

        formatted_timestamp = self.new_text.text()
        formatted_timestamp = datetime.strptime(formatted_timestamp, '%m/%d/%Y %H%M')
        return str(int(formatted_timestamp.timestamp()))

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_new_timestamp to get access to the
        settings the user entered into the dialog.
        """
        self.canceled = False
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


class OptionsWidget(QtWidgets.QWidget):
    """
    OptionsWidget contains the controls that dictate the positioning of the elements in the scene.  Save those
    adjustments back out to file to get new xyzrph for future processing.
    """
    vess_selected_sig = Signal(str)  # user changed vessel
    sensor_selected_sig = Signal(str)  # user selected a new sensor
    update_sensor_sig = Signal(str, float, float, float, float, float, float, float, float, float, float, float)  # user submitted a sensor update
    hide_waterline_sig = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QtWidgets.QVBoxLayout()

        config_layout = QtWidgets.QHBoxLayout()
        config_layout_labels = QtWidgets.QVBoxLayout()
        config_layout_controls = QtWidgets.QVBoxLayout()
        configtxt = QtWidgets.QLabel('Vessel File: ')
        self.config_name = QtWidgets.QLabel('None')
        self.config_name.setMinimumHeight(18)
        sourcetxt = QtWidgets.QLabel('Source: ')
        self.source_name = QtWidgets.QLabel('None')
        self.source_name.setMinimumHeight(18)
        self.vess_descrip = QtWidgets.QLabel('Vessel: ')
        self.vess_select = QtWidgets.QComboBox()
        self.vess_select.setMinimumWidth(200)
        vessels = os.listdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'vessel_stl_files'))
        vessels = [vess for vess in vessels if os.path.splitext(vess)[1] in ['.obj', '.stl']]
        self.vess_select.addItems(vessels)
        self.vess_select.setEnabled(False)
        self.serial_descrip = QtWidgets.QLabel('S/N: ')
        self.serial_select = QtWidgets.QComboBox()
        self.serial_select.setMinimumWidth(200)
        self.serial_select.setEnabled(False)
        self.model_descrip = QtWidgets.QLabel('Model: ')
        self.model_select = QtWidgets.QLabel('None')
        self.model_select.setMinimumHeight(18)
        time_descrip = QtWidgets.QLabel('UTC Date: ')
        self.time_select = QtWidgets.QComboBox()
        self.time_select.setMinimumWidth(200)
        self.time_select.setEnabled(False)

        for lbl in [configtxt, sourcetxt, self.model_descrip, self.serial_descrip, self.vess_descrip, time_descrip]:
            config_layout_labels.addWidget(lbl)
        for widg in [self.config_name, self.source_name, self.model_select, self.serial_select, self.vess_select, self.time_select]:
            config_layout_controls.addWidget(widg)
        config_layout.addLayout(config_layout_labels)
        config_layout.addLayout(config_layout_controls)
        config_layout.addStretch()

        second_item = QtWidgets.QHBoxLayout()
        sensorlabel = QtWidgets.QLabel('Sensor:')
        self.sensor_select = QtWidgets.QComboBox()
        sensors = ['Display Config', 'Sonar Transmitter', 'Sonar Receiver', 'Waterline', 'Latency', 'Uncertainty']
        self.sensor_select.addItems(sensors)
        self.sensor_select.setEnabled(False)
        second_item.addWidget(sensorlabel)
        second_item.addWidget(self.sensor_select)

        self.basic_lever = QtWidgets.QGroupBox('Lever arms and angles')
        self.basic_lever.setCheckable(False)

        third_option = QtWidgets.QVBoxLayout()
        third_option_sub = QtWidgets.QHBoxLayout()
        third_option_sub_labels = QtWidgets.QVBoxLayout()
        third_option_sub_ctrls = QtWidgets.QVBoxLayout()
        self.refpt_label = QtWidgets.QLabel('Reference Point')
        self.refpt_select = QtWidgets.QComboBox()
        refpts = ['Sonar Transmitter', 'IMU', 'Custom']
        self.refpt_select.addItems(refpts)
        self.refpt_select.setEnabled(False)
        self.show_waterline = QtWidgets.QCheckBox('Show Waterline')
        self.show_waterline.setChecked(True)
        self.waterline_spacer = QtWidgets.QLabel('')
        self.xlabel, self.x = self.add_num_ctrl('x (+Forward)')
        self.ylabel, self.y = self.add_num_ctrl('y (+Starboard)')
        self.zlabel, self.z = self.add_num_ctrl('z (+Down)')
        self.rlabel, self.r = self.add_num_ctrl('roll (+Port Up)')
        self.plabel, self.p = self.add_num_ctrl('pitch (+Bow Up)')
        self.yawlabel, self.yaw = self.add_num_ctrl('yaw (+Clockwise)')
        self.opening_angle_label, self.opening_angle = self.add_num_ctrl('Opening Angle (degrees)', tooltip='beam opening angle, should auto populate from the multibeam data.')
        self.latencylabel, self.latency = self.add_num_ctrl('Motion Latency (Seconds)')
        for widg in [self.show_waterline, self.refpt_label, self.xlabel, self.ylabel, self.zlabel, self.rlabel,
                     self.plabel, self.yawlabel, self.opening_angle_label, self.latencylabel]:
            third_option_sub_labels.addWidget(widg)
        for widg in [self.waterline_spacer, self.refpt_select, self.x, self.y, self.z, self.r, self.p, self.yaw, self.opening_angle,
                     self.latency]:
            third_option_sub_ctrls.addWidget(widg)
        third_option_sub.addLayout(third_option_sub_labels)
        third_option_sub.addLayout(third_option_sub_ctrls)
        third_option.addLayout(third_option_sub)
        self.basic_lever.setLayout(third_option)

        self.basic_config = QtWidgets.QGroupBox('Display configuration options')
        self.basic_config.setCheckable(False)

        fourth_option = QtWidgets.QVBoxLayout()
        vessel_center = QtWidgets.QGroupBox('Vessel')
        toplevellayout = QtWidgets.QVBoxLayout()
        vessel_center_layout = QtWidgets.QHBoxLayout()
        vessel_center_descrp = QtWidgets.QLabel('For display only, align vessel with sensors')
        vessel_center_sub_labels = QtWidgets.QVBoxLayout()
        vessel_center_sub_ctrls = QtWidgets.QVBoxLayout()
        xlabel, self.vcenter_x = self.add_num_ctrl('x (+Forward)')
        ylabel, self.vcenter_y = self.add_num_ctrl('y (+Starboard)')
        zlabel, self.vcenter_z = self.add_num_ctrl('z (+Down)')
        rlabel, self.vcenter_r = self.add_num_ctrl('roll (+Port Up)')
        plabel, self.vcenter_p = self.add_num_ctrl('pitch (+Bow Up)')
        yawlabel, self.vcenter_yaw = self.add_num_ctrl('yaw (+Clockwise)')
        for widg in [xlabel, ylabel, zlabel, rlabel, plabel, yawlabel]:
            vessel_center_sub_labels.addWidget(widg)
        for widg in [self.vcenter_x, self.vcenter_y, self.vcenter_z, self.vcenter_r, self.vcenter_p, self.vcenter_yaw]:
            vessel_center_sub_ctrls.addWidget(widg)
        toplevellayout.addWidget(vessel_center_descrp)
        vessel_center_layout.addLayout(vessel_center_sub_labels)
        vessel_center_layout.addLayout(vessel_center_sub_ctrls)
        toplevellayout.addLayout(vessel_center_layout)
        vessel_center.setLayout(toplevellayout)
        fourth_option.addWidget(vessel_center)

        misc_layout = QtWidgets.QHBoxLayout()
        misc_sub_labels = QtWidgets.QVBoxLayout()
        misc_sub_ctrls = QtWidgets.QVBoxLayout()
        sensor_size_lbl, self.sensor_size = self.add_num_ctrl('Sensor size (meters)')
        dualhead_lbl = QtWidgets.QLabel('Dual Head?')
        self.dualhead_option = QtWidgets.QCheckBox()
        self.dualhead_option.setEnabled(False)
        for lbl in [dualhead_lbl, sensor_size_lbl]:
            misc_sub_labels.addWidget(lbl)
        for widg in [self.dualhead_option, self.sensor_size]:
            misc_sub_ctrls.addWidget(widg)
        misc_layout.addLayout(misc_sub_labels)
        misc_layout.addLayout(misc_sub_ctrls)
        fourth_option.addLayout(misc_layout)
        self.basic_config.setLayout(fourth_option)

        self.basic_tpu = QtWidgets.QGroupBox('Uncertainty Parameters (1 sigma)')
        self.basic_tpu.setCheckable(False)
        toplevellayout = QtWidgets.QVBoxLayout()
        tpulayout = QtWidgets.QHBoxLayout()
        vessel_center_sub_labels = QtWidgets.QVBoxLayout()
        vessel_center_sub_ctrls = QtWidgets.QVBoxLayout()
        hevelabel, self.heave_error = self.add_num_ctrl('Heave Error (meters)',
                                                        tooltip='1 sigma standard deviation in the heave sensor, generally found in manufacturer specifications.')
        rollsenslabel, self.roll_sensor_error = self.add_num_ctrl('Roll Sensor Error (degrees)',
                                                                  tooltip='1 sigma standard deviation in the roll sensor, generally found in manufacturer specifications.')
        pitchsenslabel, self.pitch_sensor_error = self.add_num_ctrl('Pitch Sensor Error (degrees)',
                                                                    tooltip='1 sigma standard deviation in the pitch sensor, generally found in manufacturer specifications.')
        headsenslabel, self.heading_sensor_error = self.add_num_ctrl('Yaw Sensor Error (degrees)',
                                                                     tooltip='1 sigma standard deviation in the heading sensor, generally found in manufacturer specifications.')
        surfsvlabel, self.surface_sv_error = self.add_num_ctrl('Surface SV Error (meters/second)',
                                                               tooltip='1 sigma standard deviation in surface sv sensor, generally found in manufacturer specifications.')
        rollpatchlabel, self.roll_patch_error = self.add_num_ctrl('Roll Patch Error (degrees)',
                                                                  tooltip='1 sigma standard deviation in your roll angle patch test procedure.')
        waterlinelabel, self.waterline_error = self.add_num_ctrl('Waterline Error (meters)',
                                                                 tooltip='1 sigma standard deviation of the waterline measurement, only used for waterline vertical reference.')
        horizlabel, self.horizontal_positioning_error = self.add_num_ctrl('Horizontal Positioning Error (meters)',
                                                                          tooltip='1 sigma standard deviation of the horizontal positioning system, only used if SBET is not provided.')
        vertlabel, self.vertical_positioning_error = self.add_num_ctrl('Vertical Positioning Error (meters)',
                                                                       tooltip='1 sigma standard deviation of the vertical positioning system, only used if SBET is not provided.')

        for widg in [hevelabel, rollsenslabel, pitchsenslabel, headsenslabel, surfsvlabel, rollpatchlabel,
                     waterlinelabel, horizlabel, vertlabel]:
            vessel_center_sub_labels.addWidget(widg)
        for widg in [self.heave_error, self.roll_sensor_error, self.pitch_sensor_error,
                     self.heading_sensor_error, self.surface_sv_error, self.roll_patch_error, self.waterline_error,
                     self.horizontal_positioning_error, self.vertical_positioning_error]:
            vessel_center_sub_ctrls.addWidget(widg)
        tpulayout.addLayout(vessel_center_sub_labels)
        tpulayout.addLayout(vessel_center_sub_ctrls)
        toplevellayout.addLayout(tpulayout)
        self.basic_tpu.setLayout(toplevellayout)

        self.update_button = QtWidgets.QPushButton('Update', self)
        self.update_button.setEnabled(False)

        layout.addLayout(config_layout)
        layout.addWidget(QtWidgets.QLabel(''))
        layout.addLayout(second_item)
        layout.addWidget(QtWidgets.QLabel(''))
        layout.addWidget(self.basic_lever)
        layout.addWidget(self.basic_config)
        layout.addWidget(self.basic_tpu)
        layout.addWidget(self.update_button)
        layout.addStretch()

        self.setLayout(layout)

        self.data = {}
        self.timestamps = []
        self.timestamps_converted = []
        self.curr_sensor_size = launch_sensor_size

        self.serial_select.currentTextChanged.connect(self.serial_selected)
        self.time_select.currentTextChanged.connect(self.time_selected)
        self.sensor_select.currentTextChanged.connect(self.sensor_selected)
        self.vess_select.currentTextChanged.connect(self.vessel_selected)
        self.update_button.clicked.connect(self.update_button_pressed)
        self.show_waterline.stateChanged.connect(self.waterline_checked)
        self.sensor_selected(None)

    def waterline_checked(self, checked):
        """
        activated on checking the show_waterline checkbox, hide the waterline plane if not checked.

        Parameters
        ----------
        checked: bool, checked state of show_waterline checkbox

        Returns
        -------

        """
        if checked:
            self.hide_waterline_sig.emit(False)
        else:
            self.hide_waterline_sig.emit(True)

    def add_num_ctrl(self, label, tooltip=None):
        """
        Consolidate the building of a label/lineedit pair for the OptionsWidget

        Parameters
        ----------
        label: str, value given to the QLabel
        tooltip: str, optional, will add a tooltip if provided

        Returns
        -------
        QtWidgets.QLineEdit, QtWidgets.QLabel

        """
        gui_label = QtWidgets.QLabel('{}: '.format(label))
        lineedit = QtWidgets.QLineEdit('')
        validator = QtGui.QDoubleValidator(-999, 999, 3)
        lineedit.setValidator(validator)
        lineedit.setText('0.000')
        lineedit.editingFinished.connect(self.validate_numctrl)
        if tooltip:
            gui_label.setToolTip(tooltip)
            lineedit.setToolTip(tooltip)
        return gui_label, lineedit

    def validate_numctrl(self):
        """
        validation function tied to whenever you lose focus on one of the number controls, automatically formats the
        number to something like '1.123'
        """
        sender = self.sender()
        sender.setText(format(float(sender.text()), '.3f'))

    def get_currently_selected_time(self):
        """
        The time_select combo box contains all the timestamps in the xyzrph record, but displays them in a datetime
        format that is readable.  This method retuns the unix timestamp in seconds that can be used to index the
        actual data

        Returns
        -------
        orig_tstmp: str, unix time in seconds as a string
        """
        tstmp = self.time_select.currentText()
        orig_tstmp = None
        if tstmp:  # on loading new xyzrph, vesselcenter is updated before timestamps are loaded.  But it gets updated later so skip here
            orig_tstmp = self.timestamps[self.timestamps_converted.index(tstmp)]
        return orig_tstmp

    def update_sensor_data(self, sensorname, x, y, z, r, p, h, extra=0.0, extra2=0.0, extra3=0.0, extra4=0.0, extra5=0.0):
        self.update_sensor_sig.emit(sensorname, x, y, z, r, p, h, extra, extra2, extra3, extra4, extra5)

    def update_button_pressed(self):
        """
        Each sensor has an update button, on click it runs this method.

        """
        sens = self.sensor_select.currentText()
        tstmp = self.get_currently_selected_time()
        serial_num = self.serial_select.currentText()
        if sens == 'Display Config':
            sensor_size = self.sensor_size.text()
            pos = [float(self.vcenter_x.text()), float(self.vcenter_y.text()), float(self.vcenter_z.text()),
                   float(self.vcenter_r.text()), float(self.vcenter_p.text()), float(self.vcenter_yaw.text()),
                   float(sensor_size)]
            self.data[serial_num][tstmp]['Vesselcenter'] = pos
            self.update_sensor_data('Vesselcenter', *pos)
            self.curr_sensor_size = float(sensor_size)
        elif sens == 'Latency':
            pos = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, float(self.latency.text())]
            self.data[serial_num][tstmp][sens] = pos
            self.update_sensor_data(sens, *pos)
        elif sens == 'Uncertainty':
            pos = [float(self.heave_error.text()), float(self.roll_sensor_error.text()),
                   float(self.pitch_sensor_error.text()), float(self.heading_sensor_error.text()), float(self.surface_sv_error.text()),
                   float(self.roll_patch_error.text()), float(self.waterline_error.text()), float(self.horizontal_positioning_error.text()),
                   float(self.vertical_positioning_error.text())]
            self.data[serial_num][tstmp][sens] = pos
            self.update_sensor_data(sens, *pos)
        else:
            pos = [float(self.x.text()), float(self.y.text()), float(self.z.text()), float(self.r.text()),
                   float(self.p.text()), float(self.yaw.text()), float(self.opening_angle.text())]
            self.data[serial_num][tstmp][sens] = pos
            self.update_sensor_data(sens, *pos)

    def parse_xyzrph(self, xyzrph):
        """
        Take the input xyzrph dict (from fqpr_generation processing) and build the records we need to update sensor
        positions in the scene.

        fqpr = fully qualified ping record, the term for the datastore in kluster

        Parameters
        ----------
        xyzrph: dict, dictionary of survey systems and the xyz and rollpitchheading values that go with each

        """
        self.data = {}
        for serial_num in xyzrph:
            self.data[serial_num] = {}
            if 'tx_port_x' in xyzrph[serial_num]:
                tstmps = list(xyzrph[serial_num]['tx_port_x'].keys())
                # tx/rx opening angles added in kluster 0.9.3, before it was just 'beam_opening_angle' and used as both
                # populate with the corrected beam angle keys, correcting this older dataset
                if 'tx_port_opening_angle' not in xyzrph[serial_num]:
                    xyzrph[serial_num]['tx_port_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                    xyzrph[serial_num]['tx_stbd_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                    xyzrph[serial_num]['rx_port_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                    xyzrph[serial_num]['rx_stbd_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                for tstmp in tstmps:
                    self.data[serial_num][tstmp] = {}
                    self.data[serial_num][tstmp]['Dual Head'] = True
                    self.data[serial_num][tstmp]['Port Sonar Transmitter'] = [float(xyzrph[serial_num]['tx_port_x'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_y'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_z'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_r'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_p'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_h'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_port_opening_angle'][tstmp])]
                    self.data[serial_num][tstmp]['Port Sonar Receiver'] = [float(xyzrph[serial_num]['rx_port_x'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_y'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_z'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_r'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_p'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_h'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_port_opening_angle'][tstmp])]
                    self.data[serial_num][tstmp]['Stbd Sonar Transmitter'] = [float(xyzrph[serial_num]['tx_stbd_x'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_y'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_z'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_r'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_p'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_h'][tstmp]),
                                                                              float(xyzrph[serial_num]['tx_stbd_opening_angle'][tstmp])]
                    self.data[serial_num][tstmp]['Stbd Sonar Receiver'] = [float(xyzrph[serial_num]['rx_stbd_x'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_y'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_z'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_r'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_p'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_h'][tstmp]),
                                                                           float(xyzrph[serial_num]['rx_stbd_opening_angle'][tstmp])]
            else:
                tstmps = list(xyzrph[serial_num]['tx_x'].keys())
                # tx/rx opening angles added in kluster 0.9.3, before it was just 'beam_opening_angle' and used as both
                # populate with the corrected beam angle keys, correcting this older dataset
                if 'tx_opening_angle' not in xyzrph[serial_num]:
                    xyzrph[serial_num]['tx_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                    xyzrph[serial_num]['rx_opening_angle'] = deepcopy(xyzrph[serial_num]['beam_opening_angle'])
                for tstmp in tstmps:
                    self.data[serial_num][tstmp] = {}
                    self.data[serial_num][tstmp]['Dual Head'] = False
                    self.data[serial_num][tstmp]['Sonar Transmitter'] = [float(xyzrph[serial_num]['tx_x'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_y'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_z'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_r'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_p'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_h'][tstmp]),
                                                                         float(xyzrph[serial_num]['tx_opening_angle'][tstmp])]
                    self.data[serial_num][tstmp]['Sonar Receiver'] = [float(xyzrph[serial_num]['rx_x'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_y'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_z'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_r'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_p'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_h'][tstmp]),
                                                                      float(xyzrph[serial_num]['rx_opening_angle'][tstmp])]

            for tstmp in tstmps:
                self.data[serial_num][tstmp]['Vessel Reference Point'] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                self.data[serial_num][tstmp]['Vessel File'] = os.path.normpath(xyzrph[serial_num]['vessel_file'][tstmp])
                self.data[serial_num][tstmp]['Sonar Type'] = xyzrph[serial_num]['sonar_type'][tstmp]
                self.data[serial_num][tstmp]['Source'] = xyzrph[serial_num]['source'][tstmp]
                try:
                    self.data[serial_num][tstmp]['IMU'] = [float(xyzrph[serial_num]['imu_x'][tstmp]),
                                                           float(xyzrph[serial_num]['imu_y'][tstmp]),
                                                           float(xyzrph[serial_num]['imu_z'][tstmp]),
                                                           float(xyzrph[serial_num]['imu_r'][tstmp]),
                                                           float(xyzrph[serial_num]['imu_p'][tstmp]),
                                                           float(xyzrph[serial_num]['imu_h'][tstmp])]
                except KeyError:
                    self.data[serial_num][tstmp]['IMU'] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                try:
                    self.data[serial_num][tstmp]['Primary Antenna'] = [float(xyzrph[serial_num]['tx_to_antenna_x'][tstmp]),
                                                                       float(xyzrph[serial_num]['tx_to_antenna_y'][tstmp]),
                                                                       float(xyzrph[serial_num]['tx_to_antenna_z'][tstmp]),
                                                                       0, 0, 0]
                except KeyError:
                    self.data[serial_num][tstmp]['Primary Antenna'] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                self.data[serial_num][tstmp]['Waterline'] = [0, 0, xyzrph[serial_num]['waterline'][tstmp], 0, 0, 0]
                self.data[serial_num][tstmp]['Latency'] = [0, 0, 0, 0, 0, 0, xyzrph[serial_num]['latency'][tstmp]]
                try:
                    self.data[serial_num][tstmp]['Vesselcenter'] = [float(xyzrph[serial_num]['vess_center_x'][tstmp]),
                                                                    float(xyzrph[serial_num]['vess_center_y'][tstmp]),
                                                                    float(xyzrph[serial_num]['vess_center_z'][tstmp]),
                                                                    float(xyzrph[serial_num]['vess_center_r'][tstmp]),
                                                                    float(xyzrph[serial_num]['vess_center_p'][tstmp]),
                                                                    float(xyzrph[serial_num]['vess_center_yaw'][tstmp]),
                                                                    float(xyzrph[serial_num]['sensor_size'][tstmp])]
                except KeyError:
                    self.data[serial_num][tstmp]['Vesselcenter'] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, launch_sensor_size]

                try:
                    self.data[serial_num][tstmp]['Uncertainty'] = [float(xyzrph[serial_num]['heave_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['roll_sensor_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['pitch_sensor_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['heading_sensor_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['surface_sv_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['roll_patch_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['waterline_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['horizontal_positioning_error'][tstmp]),
                                                                   float(xyzrph[serial_num]['vertical_positioning_error'][tstmp])]
                except KeyError:
                    self.data[serial_num][tstmp]['Uncertainty'] = [kluster_variables.default_heave_error,
                                                                   kluster_variables.default_roll_sensor_error,
                                                                   kluster_variables.default_pitch_sensor_error,
                                                                   kluster_variables.default_heading_sensor_error,
                                                                   kluster_variables.default_surface_sv_error,
                                                                   kluster_variables.default_roll_patch_error,
                                                                   kluster_variables.default_waterline_error,
                                                                   kluster_variables.default_horizontal_positioning_error,
                                                                   kluster_variables.default_vertical_positioning_error]

    def determine_reference_point(self, tstmp):
        """
        Each time the Vessel Reference Point comes up in the sensor_select, run this method to determine the current
        reference point.  We assume that either IMU, sonar transmitter, or some custom option will always be the
        reference point.

        Parameters
        ----------
        tstmp: str, unix timestamp for the given entry

        """

        serial_num = self.serial_select.currentText()

        refpt = 'Custom'
        if self.data and serial_num in self.data and tstmp in self.data[serial_num]:
            data_by_time = self.data[serial_num][tstmp]
            imu_vals = np.array([np.round(float(x), 3) for x in data_by_time['IMU']])
            if np.array_equal(imu_vals[0:3], v4_target_sensing_center_offset) or \
                    np.array_equal(imu_vals[0:3], v5_target_sensing_center_offset) or \
                    not np.any(imu_vals[0:3]):
                refpt = 'IMU'

            if 'Port Sonar Transmitter' in data_by_time:
                ident = 'Port Sonar Transmitter'
                sonar_vals = np.array([np.round(float(x), 3) for x in data_by_time['Port Sonar Transmitter']])
            else:
                ident = 'Sonar Transmitter'
                sonar_vals = np.array([np.round(float(x), 3) for x in data_by_time['Sonar Transmitter']])
            if not np.any(sonar_vals):
                refpt = ident
        else:
            refpt = None
        return refpt

    def populate_from_xyzrph(self, xyzrph):
        """
        User has provided a xyzrph dict.  Here we signal the scene to update the positions of each sensor and adjust
        the sensor list for dual head if necessary.

        Parameters
        ----------
        xyzrph: dict, dictionary of survey systems and the xyz and rollpitchheading values that go with each

        """

        if xyzrph is None:  # clear the data
            self.vess_select.setEnabled(False)
            self.serial_select.setEnabled(False)
            self.sensor_select.setEnabled(False)
            self.time_select.setEnabled(False)
            self.update_button.setEnabled(False)
            for sens in ['Port Sonar Transmitter', 'Port Sonar Receiver', 'Stbd Sonar Transmitter', 'Stbd Sonar Receiver',
                         'Sonar Transmitter', 'Sonar Receiver', 'IMU', 'Primary Antenna', 'Waterline', 'Latency', 'Vesselcenter', 'Uncertainty']:
                self.update_sensor_data(sens, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            self.serial_select.clear()
            self.time_select.clear()
            self.sensor_select.clear()
            self.refpt_select.clear()
            self.source_name.setText('None')
            self.model_select.setText('None')
            self.data = {}
        else:
            # this method is run on new config or importing from multibeam.  So we need to start by enabling the controls
            self.vess_select.setEnabled(True)
            self.serial_select.setEnabled(True)
            self.sensor_select.setEnabled(True)
            self.time_select.setEnabled(True)
            self.update_button.setEnabled(True)
            self.parse_xyzrph(xyzrph)
            self.serial_selected(None, setup=True)

        # loading a new config should reset waterline visibility so there isn't an issue with hiding and visibility
        self.show_waterline.setChecked(True)

    def serial_selected(self, evt, setup=False):
        """
        Triggered on selecting a serial number, loads the possible sensor options and triggers the time_select event
        to populate the data
        """

        if setup:
            self.serial_select.clear()
            self.serial_select.addItems(list(self.data.keys()))
        else:
            serial_num = self.serial_select.currentText()
            if serial_num:
                data = self.data[serial_num]
                first_tstmp = list(data.keys())[0]
                if data[first_tstmp]['Dual Head']:
                    sensors = ['Display Config', 'Vessel Reference Point', 'Port Sonar Transmitter', 'Port Sonar Receiver',
                               'Stbd Sonar Transmitter', 'Stbd Sonar Receiver', 'IMU', 'Primary Antenna', 'Waterline',
                               'Latency', 'Uncertainty']
                else:
                    sensors = ['Display Config', 'Vessel Reference Point', 'Sonar Transmitter', 'Sonar Receiver', 'IMU',
                               'Primary Antenna', 'Waterline', 'Latency', 'Uncertainty']
                curr_select = self.sensor_select.currentText()
                self.sensor_select.clear()
                self.sensor_select.addItems(sensors)
                if curr_select in sensors:
                    self.sensor_select.setCurrentText(curr_select)
                self.time_selected(None, setup=True)

    def time_selected(self, evt, setup=False):
        """
        Triggered on selecting in the gui or loading new data, will pull the data for the timestamp and use the
        update_sensor_data method to sync with the master xyzrph dict and update the vessel view with the existing
        positions.
        """

        serial_num = self.serial_select.currentText()
        if setup:
            self.time_select.clear()
            self.timestamps = list(self.data[serial_num].keys())
            self.timestamps_converted = [datetime.fromtimestamp(int(tstmp)).strftime('%m/%d/%Y %H%M') for tstmp in self.timestamps]
            self.time_select.addItems([tstmp_conv for _, tstmp_conv in sorted(zip(self.timestamps, self.timestamps_converted))])
        else:
            curr_timestamp = self.get_currently_selected_time()
            if serial_num and curr_timestamp:
                hide_loc = hide_location.tolist() + [0, 0, 0]

                data = self.data[serial_num][curr_timestamp]
                self.model_select.setText(data['Sonar Type'])
                self.source_name.setText(data['Source'])
                vess = self.data[serial_num][curr_timestamp]['Vessel File']
                vessindex = self.vess_select.findText(os.path.split(vess)[1])
                currindex = self.vess_select.findText(self.vess_select.currentText())
                self.vess_select.setCurrentIndex(vessindex)
                if vessindex == currindex:
                    self.vessel_selected(None)
                if data['Dual Head']:  # dual head
                    refpts = ['Port Sonar Transmitter', 'IMU', 'Custom']
                    self.update_sensor_data('Port Sonar Transmitter', *[np.round(float(x), 3) for x in data['Port Sonar Transmitter']])
                    self.update_sensor_data('Port Sonar Receiver', *[np.round(float(x), 3) for x in data['Port Sonar Receiver']])
                    self.update_sensor_data('Stbd Sonar Transmitter', *[np.round(float(x), 3) for x in data['Stbd Sonar Transmitter']])
                    self.update_sensor_data('Stbd Sonar Receiver', *[np.round(float(x), 3) for x in data['Stbd Sonar Receiver']])
                else:
                    refpts = ['Sonar Transmitter', 'IMU', 'Custom']
                    self.update_sensor_data('Stbd Sonar Transmitter', *hide_loc)
                    self.update_sensor_data('Stbd Sonar Receiver', *hide_loc)
                    self.update_sensor_data('Sonar Transmitter', *[np.round(float(x), 3) for x in data['Sonar Transmitter']])
                    self.update_sensor_data('Sonar Receiver', *[np.round(float(x), 3) for x in data['Sonar Receiver']])
                self.update_sensor_data('IMU', *[np.round(float(x), 3) for x in data['IMU']])
                self.update_sensor_data('Primary Antenna', *[np.round(float(x), 3) for x in data['Primary Antenna']])
                self.update_sensor_data('Waterline', *[np.round(float(x), 3) for x in data['Waterline']])
                self.update_sensor_data('Latency', *[np.round(float(x), 3) for x in data['Latency']])
                self.update_sensor_data('Vesselcenter', *[np.round(float(x), 3) for x in data['Vesselcenter']])
                self.update_sensor_data('Uncertainty', *[np.round(float(x), 3) for x in data['Uncertainty']])

                self.refpt_select.clear()
                self.refpt_select.addItems(refpts)
            self.sensor_selected(None)

    def vessel_selected(self, evt):
        """
        When the user selects a new vessel in the combobox, emit the vessel name to update the scene

        """
        pth_to_vess = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'vessel_stl_files',
                                   self.vess_select.currentText())
        serial_num = self.serial_select.currentText()
        curr_tstmp = self.get_currently_selected_time()
        if self.data and serial_num in self.data and curr_tstmp in self.data[serial_num]:
            self.data[serial_num][curr_tstmp]['Vessel File'] = self.vess_select.currentText()
        self.vess_selected_sig.emit(str(pth_to_vess))

    def sensor_selected(self, evt):
        """
        When the user selects a new sensor, show the correct controls and emit the sensor name to update the scene,
        updates the alpha value for the cube to identify the sensor selected.

        """
        sens = self.sensor_select.currentText()
        self.sensor_selected_sig.emit(sens)
        if sens == 'Display Config':
            self.basic_config.show()
            self.basic_lever.hide()
            self.basic_tpu.hide()
        elif sens == 'Latency':
            self.basic_config.hide()
            self.basic_lever.show()
            self.basic_tpu.hide()
            self.show_waterline.hide()
            self.waterline_spacer.hide()
            self.refpt_label.hide()
            self.refpt_select.hide()
            self.xlabel.hide()
            self.x.hide()
            self.ylabel.hide()
            self.y.hide()
            self.zlabel.hide()
            self.z.hide()
            self.rlabel.hide()
            self.r.hide()
            self.plabel.hide()
            self.p.hide()
            self.yawlabel.hide()
            self.yaw.hide()
            self.opening_angle_label.hide()
            self.opening_angle.hide()
            self.latencylabel.show()
            self.latency.show()
        elif sens == 'Waterline':
            self.basic_config.hide()
            self.basic_lever.show()
            self.basic_tpu.hide()
            self.show_waterline.show()
            self.waterline_spacer.show()
            self.refpt_label.hide()
            self.refpt_select.hide()
            self.xlabel.hide()
            self.x.hide()
            self.ylabel.hide()
            self.y.hide()
            self.zlabel.show()
            self.z.show()
            self.rlabel.hide()
            self.r.hide()
            self.plabel.hide()
            self.p.hide()
            self.yawlabel.hide()
            self.yaw.hide()
            self.opening_angle_label.hide()
            self.opening_angle.hide()
            self.latencylabel.hide()
            self.latency.hide()
        elif sens == 'Primary Antenna':
            self.basic_config.hide()
            self.basic_lever.show()
            self.basic_tpu.hide()
            self.show_waterline.hide()
            self.waterline_spacer.hide()
            self.refpt_label.hide()
            self.refpt_select.hide()
            self.xlabel.show()
            self.x.show()
            self.ylabel.show()
            self.y.show()
            self.zlabel.show()
            self.z.show()
            self.rlabel.hide()
            self.r.hide()
            self.plabel.hide()
            self.p.hide()
            self.yawlabel.hide()
            self.yaw.hide()
            self.opening_angle_label.hide()
            self.opening_angle.hide()
            self.latencylabel.hide()
            self.latency.hide()
        elif sens == 'Uncertainty':
            self.basic_config.hide()
            self.basic_lever.hide()
            self.basic_tpu.show()
        else:
            self.basic_config.hide()
            self.basic_lever.show()
            self.basic_tpu.hide()
            self.show_waterline.hide()
            self.waterline_spacer.hide()
            if sens == 'Vessel Reference Point':
                self.refpt_label.show()
                self.refpt_select.show()
            else:
                self.refpt_label.hide()
                self.refpt_select.hide()
            self.xlabel.show()
            self.x.show()
            self.ylabel.show()
            self.y.show()
            self.zlabel.show()
            self.z.show()
            self.rlabel.show()
            self.r.show()
            self.plabel.show()
            self.p.show()
            self.yawlabel.show()
            self.yaw.show()
            if sens not in ['Vessel Reference Point', 'Display Config', 'Vessel Reference Point', 'IMU']:
                self.opening_angle_label.show()
                self.opening_angle.show()
            else:
                self.opening_angle_label.hide()
                self.opening_angle.hide()
            self.latencylabel.hide()
            self.latency.hide()
        if sens:
            self.populate_sensor(sens)
        else:
            self.x.setText('0.000')
            self.y.setText('0.000')
            self.z.setText('0.000')
            self.r.setText('0.000')
            self.p.setText('0.000')
            self.yaw.setText('0.000')
            self.opening_angle.setText('0.000')

    def populate_sensor(self, sensor_label):
        """
        On selecting a new sensor in sensor_select combobox, set the input options.

        Parameters
        ----------
        sensor_label: str, one of the sensor_select combobox items

        """
        if self.data:
            serial_num = self.serial_select.currentText()
            tstmp = self.get_currently_selected_time()
            if serial_num and tstmp:
                refsensor = self.determine_reference_point(tstmp)
                if refsensor:
                    if sensor_label == 'Display Config':
                        element_size = self.curr_sensor_size
                        self.dualhead_option.setChecked(self.data[serial_num][tstmp]['Dual Head'])
                        self.sensor_size.setText(str(element_size))
                        data = self.data[serial_num][tstmp]['Vesselcenter']
                        self.vcenter_x.setText(format(float(data[0]), '.3f'))
                        self.vcenter_y.setText(format(float(data[1]), '.3f'))
                        self.vcenter_z.setText(format(float(data[2]), '.3f'))
                        self.vcenter_r.setText(format(float(data[3]), '.3f'))
                        self.vcenter_p.setText(format(float(data[4]), '.3f'))
                        self.vcenter_yaw.setText(format(float(data[5]), '.3f'))
                        self.sensor_size.setText(format(float(data[6]), '.3f'))
                        self.update_button.show()
                    elif sensor_label == 'Vessel Reference Point':
                        index = self.refpt_select.findText(refsensor)
                        self.refpt_select.setCurrentIndex(index)
                        if refsensor == 'Custom':
                            data = [0, 0, 0, 0, 0, 0]
                        else:
                            data = self.data[serial_num][tstmp][refsensor]
                        self.x.setText(format(float(data[0]), '.3f'))
                        self.y.setText(format(float(data[1]), '.3f'))
                        self.z.setText(format(float(data[2]), '.3f'))
                        self.r.setText(format(float(data[3]), '.3f'))
                        self.p.setText(format(float(data[4]), '.3f'))
                        self.yaw.setText(format(float(data[5]), '.3f'))
                        self.x.setEnabled(False)
                        self.y.setEnabled(False)
                        self.z.setEnabled(False)
                        self.r.setEnabled(False)
                        self.p.setEnabled(False)
                        self.yaw.setEnabled(False)
                        self.update_button.hide()
                    elif sensor_label == 'Uncertainty':
                        data = self.data[serial_num][tstmp]['Uncertainty']
                        self.heave_error.setText(format(float(data[0]), '.3f'))
                        self.roll_sensor_error.setText(format(float(data[1]), '.3f'))
                        self.pitch_sensor_error.setText(format(float(data[2]), '.3f'))
                        self.heading_sensor_error.setText(format(float(data[3]), '.3f'))
                        self.surface_sv_error.setText(format(float(data[4]), '.3f'))
                        self.roll_patch_error.setText(format(float(data[5]), '.3f'))
                        self.waterline_error.setText(format(float(data[6]), '.3f'))
                        self.horizontal_positioning_error.setText(format(float(data[7]), '.3f'))
                        self.vertical_positioning_error.setText(format(float(data[8]), '.3f'))
                    else:
                        data = self.data[serial_num][tstmp][sensor_label]
                        if sensor_label == 'IMU':
                            if np.array_equal(data[0:3], v4_target_sensing_center_offset) or \
                                    np.array_equal(data[0:3], v5_target_sensing_center_offset):
                                data[0:3] = np.array([0, 0, 0])
                            self.x.setEnabled(False)
                            self.y.setEnabled(False)
                            self.z.setEnabled(False)
                            self.r.setEnabled(False)
                            self.p.setEnabled(False)
                            self.yaw.setEnabled(False)
                            self.opening_angle.setEnabled(False)
                            self.update_button.hide()
                        else:
                            self.x.setEnabled(True)
                            self.y.setEnabled(True)
                            self.z.setEnabled(True)
                            self.r.setEnabled(True)
                            self.p.setEnabled(True)
                            self.yaw.setEnabled(True)
                            self.opening_angle.setEnabled(True)
                            self.update_button.show()

                        self.x.setText(format(float(data[0]), '.3f'))
                        self.y.setText(format(float(data[1]), '.3f'))
                        self.z.setText(format(float(data[2]), '.3f'))
                        self.r.setText(format(float(data[3]), '.3f'))
                        self.p.setText(format(float(data[4]), '.3f'))
                        self.yaw.setText(format(float(data[5]), '.3f'))
                        if sensor_label == 'Latency':
                            self.latency.setText(format(float(data[6]), '.3f'))
                        elif sensor_label in ['Sonar Transmitter', 'Sonar Receiver', 'Port Sonar Transmitter',
                                              'Port Sonar Receiver', 'Stbd Sonar Transmitter', 'Stbd Sonar Receiver']:
                            self.opening_angle.setText(format(float(data[6]), '.3f'))


class VesselView(QtWidgets.QWidget):
    """
    Widget containing the Vispy scene.  Controled with mouse (rotation/translation) and dictated by the values
    shown in the OptionsWidget.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = scene.SceneCanvas(self, keys='interactive', show=True)
        self.vessview = self.scene.central_widget.add_view()

        self.first_time_setup = True
        self.vessel = None
        self.x_axis = None
        self.x_axis_lbl = None
        self.y_axis = None
        self.y_axis_lbl = None
        self.z_axis = None
        self.z_axis_lbl = None

        self.tx_primary = None
        self.tx_secondary = None
        self.rx_primary = None
        self.rx_secondary = None
        self.imu = None
        self.antenna = None
        self.waterline = None

        self.show_vessel = True
        self.show_axes = True
        self.show_waterline = True

        self.currselected = None
        self.curr_sensor_size = launch_sensor_size
        self.sensor_lookup = None
        self.origin = np.array([0, 0, 0])

        # start off with the first vessel found
        vess_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'vessel_stl_files')
        self.pth_to_vessel_file = os.path.join(vess_folder, os.listdir(vess_folder)[0])
        self.current_vessel_position = None
        self.current_vessel_rotation = None
        self.current_waterline_position = None

        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.scene.native)
        self.setLayout(layout)

    def show_vessel_triggered(self, state):
        self.show_vessel = state
        if self.pth_to_vessel_file:
            self.build_vessel(self.pth_to_vessel_file)

    def show_axes_triggered(self, state):
        self.show_axes = state
        if self.pth_to_vessel_file:
            self.build_vessel(self.pth_to_vessel_file)

    def clear_sensors(self):
        if self.tx_primary is not None:
            self.tx_primary.mesh.parent = None
            self.tx_primary = None
            self.tx_secondary.mesh.parent = None
            self.tx_secondary = None
            self.rx_primary.mesh.parent = None
            self.rx_primary = None
            self.rx_secondary.mesh.parent = None
            self.rx_secondary = None
            self.imu.mesh.parent = None
            self.imu = None
            self.antenna.mesh.parent = None
            self.antenna = None
            self.waterline.mesh.parent = None
            self.waterline = None
            self.clear_axis()

    def clear_axis(self):
        if self.x_axis is not None:
            self.x_axis.parent = None
            self.x_axis = None
            self.y_axis.parent = None
            self.y_axis = None
            self.z_axis.parent = None
            self.z_axis = None
            self.x_axis_lbl.parent = None
            self.x_axis_lbl = None
            self.y_axis_lbl.parent = None
            self.y_axis_lbl = None
            self.z_axis_lbl.parent = None
            self.z_axis_lbl = None

    def clear_vessel(self):
        if self.vessel is not None:
            self.vessel.mesh.parent = None
            self.vessel.mesh = None
            self.vessel = None

    def build_sensors(self):
        """
        On creating a new scene, the first thing to do is build all the sensors we are showing.  You'll see that new
        sensors are positioned at the 'hide_location'.  This is done intentionally, as there is no real good way to
        hide a sensor except by moving it far away from the vessel and making it transparent.  Transparent objects that
        overlap with shown objects are still visible as black cubes.

        sensor_lookup is used later on as a quick way to interact with all sensors

        """
        self.origin = np.array([0, 0, 0])

        # secondary is only used for dual head
        self.tx_primary = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                                 name='tx_primary', color=Color('red', alpha=0.2), origin=self.origin)
        self.tx_secondary = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                                   name='tx_secondary', color=Color('red', alpha=0.2), origin=self.origin)
        self.rx_primary = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                                 name='rx_primary', color=Color('green', alpha=0.2), origin=self.origin)
        self.rx_secondary = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                                   name='rx_secondary', color=Color('green', alpha=0.2), origin=self.origin)
        self.imu = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                          name='imu', color=Color('orange', alpha=0.2), origin=self.origin)
        self.antenna = Sensor('cube', hide_location, parent=self.vessview.scene, size=self.curr_sensor_size,
                              name='primary_antenna', color=Color('yellow', alpha=0.2), origin=self.origin)

        self.sensor_lookup = {'Sonar Transmitter': self.tx_primary, 'Sonar Receiver': self.rx_primary,
                              'Port Sonar Transmitter': self.tx_primary,
                              'Port Sonar Receiver': self.rx_primary, 'Stbd Sonar Transmitter': self.tx_secondary,
                              'Stbd Sonar Receiver': self.rx_secondary, 'IMU': self.imu, 'Primary Antenna': self.antenna}

    def build_waterline_sensor(self):
        if self.show_waterline and self.waterline is None:
            self.waterline = Waterline('plane', hide_location, parent=self.vessview.scene, width=100, height=100,
                                       name='waterline', color=Color('blue', alpha=0.2), origin=self.origin)
            if self.current_waterline_position is not None:
                self.waterline.set_position(self.current_waterline_position.copy())
            self.sensor_lookup['Waterline'] = self.waterline
            self.sensor_selected('Waterline')
        elif self.waterline is not None and not self.show_waterline:
            self.waterline.mesh.parent = None
            self.waterline = None
            self.sensor_lookup['Waterline'] = self.waterline

    def build_axes(self):
        self.x_axis = scene.visuals.Arrow(pos=np.array([[0, 0], [50000, 0]]), color='r', parent=self.vessview.scene,
                                          arrows=np.array([[49000, 0, 50000, 0], [49000, 0, 50000, 0]]), arrow_size=8,
                                          arrow_color='r', arrow_type='triangle_60')
        self.x_axis_lbl = scene.visuals.Text('x + Forward', color='r', pos=(58000, 0, 0), font_size=1000000,
                                             parent=self.vessview.scene)
        self.y_axis = scene.visuals.Arrow(pos=np.array([[0, 0], [0, -50000]]), color='g', parent=self.vessview.scene,
                                          arrows=np.array([[0, -49000, 0, -50000], [0, -49000, 0, -50000]]), arrow_size=8,
                                          arrow_color='g', arrow_type='triangle_60')
        self.y_axis_lbl = scene.visuals.Text('y + Starboard', color='g', pos=(0, -59000, 0), font_size=1000000,
                                             parent=self.vessview.scene)
        self.z_axis = scene.visuals.Arrow(pos=np.array([[0, 0, 0], [0, 0, -50000]]), color='b', parent=self.vessview.scene,
                                          arrows=np.array([[0, 0, -49000, 0, 0, -50000], [0, 0, -49000, 0, 0, -50000]]), arrow_size=8,
                                          arrow_color='b', arrow_type='triangle_60')
        self.z_axis_lbl = scene.visuals.Text('z + Down', color='b', pos=(0, 0, -58000), font_size=1000000,
                                             parent=self.vessview.scene)

    def build_vessel(self, pth_to_vess_file):
        """
        Builds the vispy scene.  Sensors are built first (so that they remain in the foreground) and the vessel is built
        last.  First time through we actually build things, see build_sensors.

        Subsequent times through (when user changes boat model) we want to only replace or move existing objects.  If
        we build again, we get duplicates.  So instead, remove the existing vessel (by setting parent=None) and create
        a new one from the model, leaving the existing sensors.

        We use the arcballcamera here (which should always be created after the vessel so that it centers on the vessel)
        just because it seems to be the easiest to use.

        Parameters
        ----------
        pth_to_vess_file: str, path to the 3d model of the boat, must be either .obj or .stl

        """
        if os.path.isfile(pth_to_vess_file):  # if the vessel control is blank for some reason, or the file goes missing, it will default back to the fist in the os.listdir init
            self.pth_to_vessel_file = pth_to_vess_file
        pth_to_vess_file_ext = os.path.splitext(pth_to_vess_file)[1]
        if pth_to_vess_file_ext not in ['.obj', '.stl']:
            raise ValueError('Only .obj and .stl are currently supported.  Got {}'.format(pth_to_vess_file))
        vesspos = np.array([0, 0, 0])

        self.clear_vessel()
        if self.tx_primary is None:
            self.build_sensors()
        self.build_waterline_sensor()
        if self.show_vessel:
            self.vessel = Vessel(pth_to_vess_file, vesspos, parent=self.vessview.scene,
                                 color=Color('grey', alpha=0.3), name='vessel')
            if self.current_vessel_position is not None:
                self.vessel.set_position(self.current_vessel_position.copy(), from_meters=True, flip_z=True, flip_y=True, flip_x=False)
            if self.current_vessel_rotation is not None:
                self.vessel.set_rotation(self.current_vessel_rotation.copy(), from_deg=True)
        if self.x_axis is None and self.show_axes:
            self.build_axes()
        elif self.x_axis and not self.show_axes:
            self.clear_axis()

        # Tried using the axis visual, but I couldn't get the ticks and labels to work, our scale is too large i guess
        #
        # self.x_axis = scene.Axis(pos=[[0, 0], [10000, 0]], tick_direction=(0, -1), axis_color='r', tick_color='r', minor_tick_length=2000, major_tick_length=5000, tick_width=500,
        #                          text_color='r', font_size=16, parent=self.vessview.scene)
        # self.y_axis = scene.Axis(pos=[[0, 0], [0, -10000]], tick_direction=(-1, 0), axis_color='g', tick_color='g',
        #                          text_color='g', font_size=16, parent=self.vessview.scene)
        # self.z_axis = scene.Axis(pos=[[0, 0], [10000, 0]], tick_direction=(0, -1), axis_color='b', tick_color='b',
        #                          text_color='b', font_size=16, parent=self.vessview.scene)
        # self.z_axis.transform = scene.transforms.MatrixTransform()  # its acutally an inverted xaxis
        # self.z_axis.transform.rotate(90, (0, 1, 0))  # rotate cw around yaxis
        # self.z_axis.transform.rotate(-45, (0, 0, 1))  # tick direction towards (-1,-1)

        if self.first_time_setup:
            self.vessview.camera = scene.cameras.TurntableCamera(parent=self.vessview.scene, center=(0, 0, 0))
            self.first_time_setup = False

    def sensor_selected(self, sensor_name):
        """
        Method driven by signal from the OptionsWidget when the user changes the sensor_select combobox.  All we do
        currently with a selected sensor is light up the sensor cube by giving it an alpha of 1.

        Parameters
        ----------
        sensor_name: str, sensor name, ex: 'Sonar Transmitter'

        """
        if self.currselected is not None:
            if self.currselected in self.sensor_lookup:
                old_sensor = self.sensor_lookup[self.currselected]
                if old_sensor:
                    old_sensor_state = old_sensor.get_sensor_state()
                    if old_sensor_state != 0:
                        old_sensor.toggle_sensor(1)  # dim
        if not sensor_name or sensor_name in ['Display Config', 'Vessel Reference Point', 'Latency']:
            self.currselected = None
        else:
            if sensor_name and sensor_name in self.sensor_lookup:
                sensor = self.sensor_lookup[sensor_name]
                if sensor:
                    sensor_state = sensor.get_sensor_state()
                    if sensor_state != 0:
                        sensor.toggle_sensor(2)  # bright
                    self.currselected = sensor_name

    def position_sensor(self, sensor_lbl, x, y, z, r, p, h, extra=0.0, extra2=0.0, extra3=0.0, extra4=0.0, extra5=0.0):
        """
        See OptionsWidget populate_from_xyzrph/update_sensor_sig.  Driven from that signal, will position the object in
        the scene based on the input xyzrph.

        Currently only handles position.  Expects:
        X = + Forward, Y = + Starboard, Z = + Down
        As it will convert to what Vispy wants:
        X = + Forward, Y = + Port, Z = + Up

        Parameters
        ----------
        sensor_lbl: str, sensor name, ex: 'Sonar Transmitter'
        x: float, x (+forward) coordinate in meters
        y: float, y (+starboard) coordinate in meters
        z: float, z (+down) coordinate in meters
        r: float, roll (+port) coordinate in degrees
        p: float, pitch (+bow) coordinate in degrees
        h: float, yaw (+clockwise) coordinate in degrees
        extra: float, optional parameter

        """
        if self.sensor_lookup:  # sensors must be built first
            if sensor_lbl == 'Vesselcenter':
                if self.vessel:  # might not be a vessel if vessel is 'hidden'
                    self.current_vessel_position = np.array([x, y, z])
                    self.current_vessel_rotation = np.array([r, p, h])
                    self.vessel.set_position(np.array([x, y, z]), from_meters=True, flip_z=True, flip_y=True, flip_x=False)
                    self.vessel.set_rotation(np.array([r, p, h]), from_deg=True)
                    self.update_sensor_sizes(extra)
            elif sensor_lbl not in ['Latency', 'Uncertainty']:
                sensor = self.sensor_lookup[sensor_lbl]
                if sensor:
                    if sensor_lbl == 'Waterline':
                        self.current_waterline_position = np.array([x, y, z])
                    newpos = np.array([x, y, z])
                    sensor.set_position(newpos, from_meters=True, flip_z=True, flip_y=True, flip_x=False)

            # currently rotations are set from origin.  We would want to rotate our little cubes relative to the center
            #   of the cube.  Not currently implemented
            # sensor.set_rotation(np.array([r, p, h]), from_deg=True)

    def update_sensor_sizes(self, new_size):
        """
        Triggered on pressing the update button in the OptionsWidget, will resize all sensors based on the provided
        new size.

        We also have to reparent the vessel and axes, so that the sensors draw on top.  Last parented object is on the
        bottom of the visualization stack.

        Parameters
        ----------
        new_size: float, size of sensor in meters

        """
        if new_size != self.curr_sensor_size:
            self.curr_sensor_size = new_size
            self.tx_primary.resize(new_size)
            self.tx_secondary.resize(new_size)
            self.rx_primary.resize(new_size)
            self.rx_secondary.resize(new_size)
            self.imu.resize(new_size)
            self.antenna.resize(new_size)

            self.vessel.mesh.parent = None
            self.vessel.mesh.parent = self.vessview.scene
            self.x_axis.parent = None
            self.x_axis.parent = self.vessview.scene
            self.y_axis.parent = None
            self.y_axis.parent = self.vessview.scene
            self.z_axis.parent = None
            self.z_axis.parent = self.vessview.scene

    def hide_waterline(self, checked):
        """
        Triggered on the user checking the show_waterline checkbox in the OptionsWidget.  'Hides' the sensor by moving
        it to the hide location and setting the alpha to zero.

        Parameters
        ----------
        checked: bool, if checked, hide the waterline plane

        """
        if checked:
            self.show_waterline = False
        else:
            self.show_waterline = True
        if self.pth_to_vessel_file:
            self.build_vessel(self.pth_to_vessel_file)


class VesselWidget(QtWidgets.QWidget):
    """
    Widget containing the OptionsWidget (left pane) and VesselView (right pane).  Manages the signals that connect the
    two widgets.
    """
    vessel_file_modified = Signal(bool)
    converted_xyzrph_modified = Signal(dict)

    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)

        if self.parent() is not None:
            self.logger = self.parent().logger
        else:
            self.logger = None

        self.setWindowTitle('Kluster Vessel Setup')
        self.setWindowFlags(QtCore.Qt.Window)

        self.vessview_window = VesselView(self)
        self.vessview_window.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        self.opts_window = OptionsWidget(self)
        # self.opts_window.setFixedWidth(300)

        self.show_vessel_action = QtWidgets.QAction('Show Vessel', self)
        self.show_vessel_action.setCheckable(True)
        self.show_vessel_action.setChecked(True)
        self.show_axes_action = QtWidgets.QAction('Show Axes', self)
        self.show_axes_action.setCheckable(True)
        self.show_axes_action.setChecked(True)

        self.show_vessel_action.toggled.connect(self.vessview_window.show_vessel_triggered)
        self.show_axes_action.toggled.connect(self.vessview_window.show_axes_triggered)
        self.opts_window.vess_selected_sig.connect(self.vessview_window.build_vessel)
        self.opts_window.vess_selected_sig.connect(self.update_xyzrph_vessel)
        self.opts_window.sensor_selected_sig.connect(self.vessview_window.sensor_selected)
        self.opts_window.update_sensor_sig.connect(self.vessview_window.position_sensor)
        self.opts_window.update_sensor_sig.connect(self.update_xyzrph_sensorposition)
        self.opts_window.hide_waterline_sig.connect(self.vessview_window.hide_waterline)

        self.mainlayout = QtWidgets.QVBoxLayout()
        self.mbar = QtWidgets.QMenuBar(self)
        self.mainlayout.setMenuBar(self.mbar)

        self.seclayout = QtWidgets.QHBoxLayout()
        self.seclayout.addWidget(self.opts_window)
        self.seclayout.addWidget(self.vessview_window)
        self.mainlayout.addLayout(self.seclayout)

        instruct = 'Left Mouse Button: Rotate,  Right Mouse Button/Mouse wheel: Zoom,  Shift + Left Mouse Button: Move,'
        instruct += '  Shift + Right Mouse Button: Change Field of View'
        self.instructions = QtWidgets.QLabel(instruct)
        self.instructions.setAlignment(QtCore.Qt.AlignCenter)
        self.instructions.setStyleSheet("QLabel { font-style : italic ; font-size : 12px }")
        self.mainlayout.addWidget(self.instructions)

        self.setLayout(self.mainlayout)

        self.xyzrph = None
        self.canceled = False
        self.setup_toolbar()

    def print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            self.parent().print(msg, loglevel)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger, when debug is enabled

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            self.parent().debug_print(msg, loglevel)
        else:
            print(msg)

    def setup_toolbar(self):
        """
        Build the FileMenu for the widget
        """
        file = self.mbar.addMenu('File')
        newconfig = QtWidgets.QAction('New Config', self)
        openconfig = QtWidgets.QAction('Open Config', self)
        saveconfig = QtWidgets.QAction('Save Config', self)
        addconfig = QtWidgets.QAction('Add to Config File', self)
        importpos = QtWidgets.QAction('Import from POSMV', self)
        importmulti = QtWidgets.QAction('Import from Multibeam', self)
        importpos.triggered.connect(self.import_from_posmv)
        importmulti.triggered.connect(self.import_from_multibeam)
        newconfig.triggered.connect(self.new_configuration)
        openconfig.triggered.connect(self.open_configuration)
        saveconfig.triggered.connect(self.save_configuration)
        addconfig.triggered.connect(self.add_to_configuration)
        file.addAction(newconfig)
        file.addAction(openconfig)
        file.addAction(saveconfig)
        file.addSeparator()
        file.addAction(addconfig)
        file.addSeparator()
        file.addAction(importpos)
        file.addAction(importmulti)

        edit = self.mbar.addMenu('Edit')
        new_timestamp = QtWidgets.QAction('New Entry', self)
        new_timestamp.triggered.connect(self.new_entry)
        edit_timestamp = QtWidgets.QAction('Alter Timestamp', self)
        edit_timestamp.triggered.connect(self.edit_timestamp)
        edit.addAction(new_timestamp)
        edit.addAction(edit_timestamp)

        view = self.mbar.addMenu('View')
        view.addAction(self.show_vessel_action)
        view.addAction(self.show_axes_action)

        testing = self.mbar.addMenu('Testing')
        importsingle = QtWidgets.QAction('Load Test Dataset', self)
        importdual = QtWidgets.QAction('Load Test Dualhead Dataset', self)
        importsingle.triggered.connect(self.load_test_dataset)
        importdual.triggered.connect(self.load_dualhead_test_dataset)
        testing.addAction(importsingle)
        testing.addAction(importdual)

    def edit_timestamp(self):
        """
        Edit the current timestamp using the TimestampDialog.  Will replace all timestamped records with the current
        timestamp with the new timestamp.
        """

        if self.xyzrph:
            dlog = TimestampDialog()
            orig_time = self.opts_window.time_select.currentText()
            orig_serial = self.opts_window.serial_select.currentText()
            dlog.populate(orig_serial, orig_time)
            if dlog.exec_() and not dlog.canceled:
                new_timestamp, remove_this = dlog.return_new_timestamp()
                orig_timestamp = self.opts_window.get_currently_selected_time()
                if remove_this:
                    for sensor in self.xyzrph[orig_serial]:
                        self.xyzrph[orig_serial][sensor].pop(orig_time)
                    self.opts_window.data[orig_serial].pop(orig_time)
                    return
                if new_timestamp in self.opts_window.data[orig_serial]:
                    self.print('{} is an existing timestamp, cannot change {} to {}'.format(new_timestamp, orig_time, new_timestamp), logging.ERROR)
                for sensor in self.xyzrph[orig_serial]:
                    sensor_data = self.xyzrph[orig_serial][sensor]
                    orig_timestamp_data = sensor_data[orig_timestamp]
                    sensor_data[new_timestamp] = orig_timestamp_data
                    del sensor_data[orig_timestamp]
                orig_timestamp_data = self.opts_window.data[orig_serial][orig_timestamp]
                self.opts_window.data[orig_serial][new_timestamp] = orig_timestamp_data
                del self.opts_window.data[orig_serial][orig_timestamp]
                self.opts_window.time_selected(None, setup=True)
        else:
            self.print('No vessel file loaded', logging.ERROR)

    def new_entry(self):
        """
        Add a new entry in the vessel file at the specified time
        """

        if self.xyzrph:
            dlog = AddEntryDialog()
            orig_time = self.opts_window.time_select.currentText()
            orig_serial = self.opts_window.serial_select.currentText()
            dlog.populate(orig_serial, orig_time)
            if dlog.exec_() and not dlog.canceled:
                new_timestamp = dlog.return_new_timestamp()
                if new_timestamp in self.opts_window.data[orig_serial]:
                    self.print('{} is an existing timestamp, cannot change {} to {}'.format(new_timestamp, orig_time,
                                                                                            new_timestamp), logging.ERROR)
                tstmps = np.array([float(tst) for tst in self.opts_window.data[orig_serial].keys()])
                nearest = str(int(tstmps[np.abs(tstmps - float(new_timestamp)).argmin()]))
                for sensor in self.xyzrph[orig_serial]:
                    self.xyzrph[orig_serial][sensor][new_timestamp] = self.xyzrph[orig_serial][sensor][nearest]
                self.opts_window.data[orig_serial][new_timestamp] = self.opts_window.data[orig_serial][nearest]
                self.opts_window.time_selected(None, setup=True)
        else:
            self.print('No vessel file loaded', logging.ERROR)

    def update_xyzrph_vessel(self, pth_to_vessel):
        """
        When the user changes the vessel, triggers this method to update the xyzrph with that vessel name

        Parameters
        ----------
        pth_to_vessel: str, file path to the vessel 3d model file

        """
        if self.xyzrph is not None:
            serial_num = self.opts_window.serial_select.currentText()
            if not serial_num:
                serial_num = list(self.xyzrph.keys())[0]
            orig_tstmp = self.opts_window.get_currently_selected_time()
            if orig_tstmp is not None:
                self.xyzrph[serial_num]['vessel_file'][orig_tstmp] = os.path.normpath(pth_to_vessel)

    def update_xyzrph_sensorposition(self, sensor_lbl, x, y, z, r, p, h, extra=0.0, extra2=0.0, extra3=0.0):
        """
        Whenever the OptionsWidget update_sensor_sig triggers (whenever a sensor position changes) this method runs
        and updates the xyzrph with that new information

        Parameters
        ----------
        sensor_lbl: str, sensor name, ex: 'Sonar Transmitter'
        x: float, x (+forward) coordinate in meters
        y: float, y (+starboard) coordinate in meters
        z: float, z (+down) coordinate in meters
        r: float, roll (+port) coordinate in degrees
        p: float, pitch (+bow) coordinate in degrees
        h: float, yaw (+clockwise) coordinate in degrees
        extra: float, optional parameter, only used for Vesselcenter, TPU sensor, opening angle
        extra2: float, optional parameter, only used for TPU sensor
        extra3: float, optional parameter, only used for TPU sensor

        """
        serial_num = self.opts_window.serial_select.currentText()
        orig_tstmp = self.opts_window.get_currently_selected_time()
        if orig_tstmp:  # on loading new xyzrph, vesselcenter is updated before timestamps are loaded.  But it gets updated later so skip here
            sensors_to_write = {'Vesselcenter': ['vess_center_x', 'vess_center_y', 'vess_center_z', 'vess_center_r', 'vess_center_p', 'vess_center_yaw', 'sensor_size'],
                                'Sonar Transmitter': ['tx_x', 'tx_y', 'tx_z', 'tx_r', 'tx_p', 'tx_h', 'tx_opening_angle'],
                                'Sonar Receiver': ['rx_x', 'rx_y', 'rx_z', 'rx_r', 'rx_p', 'rx_h', 'rx_opening_angle'],
                                'Port Sonar Transmitter': ['tx_port_x', 'tx_port_y', 'tx_port_z', 'tx_port_r', 'tx_port_p', 'tx_port_h', 'tx_port_opening_angle'],
                                'Port Sonar Receiver': ['rx_port_x', 'rx_port_y', 'rx_port_z', 'rx_port_r', 'rx_port_p', 'rx_port_h', 'rx_port_opening_angle'],
                                'Stbd Sonar Transmitter': ['tx_stbd_x', 'tx_stbd_y', 'tx_stbd_z', 'tx_stbd_r', 'tx_stbd_p', 'tx_stbd_h', 'tx_stbd_opening_angle'],
                                'Stbd Sonar Receiver': ['rx_stbd_x', 'rx_stbd_y', 'rx_stbd_z', 'rx_stbd_r', 'rx_stbd_p', 'rx_stbd_h', 'rx_stbd_opening_angle'],
                                'IMU': ['imu_x', 'imu_y', 'imu_z', 'imu_r', 'imu_p', 'imu_h'],
                                'Primary Antenna': ['tx_to_antenna_x', 'tx_to_antenna_y', 'tx_to_antenna_z', None, None, None],
                                'Waterline': [None, None, 'waterline', None, None, None],
                                'Latency': [None, None, None, None, None, None, 'latency'],
                                'Uncertainty': ['heave_error', 'roll_sensor_error', 'pitch_sensor_error',
                                                'heading_sensor_error', 'surface_sv_error', 'roll_patch_error', 'waterline_error',
                                                'horizontal_positioning_error', 'vertical_positioning_error']}
            if sensor_lbl in sensors_to_write:
                xyzrph_entries = sensors_to_write[sensor_lbl]
                if sensor_lbl in ['Vesselcenter', 'Latency', 'Sonar Transmitter', 'Sonar Receiver', 'Port Sonar Transmitter',
                                  'Port Sonar Receiver', 'Stbd Sonar Transmitter', 'Stbd Sonar Receiver']:
                    data = np.array([x, y, z, r, p, h, extra])
                elif sensor_lbl == 'Uncertainty':
                    data = np.array([x, y, z, r, p, h, extra, extra2, extra3])
                else:
                    data = np.array([x, y, z, r, p, h])
                if not np.array_equal(data[0:3], hide_location):
                    for cnt, entry in enumerate(xyzrph_entries):
                        if entry is not None and self.xyzrph is not None:
                            try:
                                if entry in self.xyzrph[serial_num]:
                                    if float(self.xyzrph[serial_num][entry][orig_tstmp]) != float(data[cnt]):
                                        self.xyzrph[serial_num][entry][orig_tstmp] = format(data[cnt], '.3f')
                                elif entry in ['tx_opening_angle', 'tx_port_opening_angle', 'tx_starboard_opening_angle',
                                               'rx_opening_angle', 'rx_port_opening_angle', 'rx_starboard_opening_angle']:
                                    # tx/rx opening angles added in kluster 0.9.3, before it was just 'beam_opening_angle' and used as the rx angle
                                    # get here if this is data processed prior to this version, add a new xyzrph entry
                                    self.xyzrph[serial_num][entry] = deepcopy(self.xyzrph[serial_num]['beam_opening_angle'])
                                    self.xyzrph[serial_num][entry][orig_tstmp] = format(data[cnt], '.3f')
                                else:
                                    # vesselcenter might not be in xyzrph, as it is created in the vessel view widget
                                    self.xyzrph[serial_num][entry] = {orig_tstmp: format(data[cnt], '.3f')}
                            except KeyError:
                                self.print('Unable to update self.xyzrph for {} with time stamp {}: {} not in {}'.format(entry, orig_tstmp, orig_tstmp, entry),
                                           logging.ERROR)

    def populate_from_xyzrph(self):
        """
        Trigger the loading of data from Kluster xyzrph.
        """

        if self.xyzrph:
            self.opts_window.populate_from_xyzrph(deepcopy(self.xyzrph))
        else:
            self.opts_window.populate_from_xyzrph(None)

    def import_from_posmv(self):
        """
        Once a configuration is loaded/created, running this will import the antenna and IMU location from a pos file
        and apply it to all timestamps.  You need to have loaded from xyzrph first, so that there is data to append to.

        """
        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='Select a POS MV file (.000)',
                                                         AppName='klusterbrowse', bMulti=False, bSave=False,
                                                         fFilter='POS MV file (*.*)')
        if fil:
            posxyzrph = return_xyzrph_from_posmv(fil)
            if posxyzrph is not None:
                if self.xyzrph is not None:
                    sensors = list(posxyzrph.keys())
                    serial_num = self.opts_window.serial_select.currentText()
                    for sens in sensors:
                        for tstmp in self.xyzrph[serial_num][sens]:
                            if float(posxyzrph[sens]):
                                self.xyzrph[serial_num][sens][tstmp] = str(posxyzrph[sens])
                    self.populate_from_xyzrph()
                else:
                    self.print('Expect data to exist before loading from POS MV, please import from multibeam first or open config file',
                               logging.ERROR)
            else:
                self.print('Unable to load from {}'.format(fil), logging.ERROR)
        else:
            self.print('Import cancelled.', logging.INFO)

    def import_from_multibeam(self):
        """
        A new configuration is created from loading a multibeam file and getting the sensor locations

        """

        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster',
                                                         Title='Select a Kongsberg file (.kmall, .all)',
                                                         AppName='klusterbrowse', bMulti=False, bSave=False,
                                                         fFilter=f"Multibeam file ({';'.join(['*' + sm for sm in kluster_variables.supported_multibeam])})")
        if fil:
            self.vessview_window.clear_sensors()
            self.vessview_window.build_vessel(self.vessview_window.pth_to_vessel_file)
            mbesxyzrph, sonar_model, serial_number = return_xyzrph_from_mbes(fil, logger=self.logger)
            if mbesxyzrph is not None:
                first_sensor = list(mbesxyzrph.keys())[0]
                tstmps = list(mbesxyzrph[first_sensor].keys())
                if not self.xyzrph:
                    self.xyzrph = {str(serial_number): mbesxyzrph}
                    self.xyzrph[str(serial_number)]['sonar_type'] = {tstmps[0]: sonar_model}
                    self.xyzrph[str(serial_number)]['source'] = {tstmps[0]: os.path.split(fil)[1]}
                elif str(serial_number) not in self.xyzrph:
                    self.xyzrph[str(serial_number)] = mbesxyzrph
                    self.xyzrph[str(serial_number)]['sonar_type'] = {tstmps[0]: sonar_model}
                    self.xyzrph[str(serial_number)]['source'] = {tstmps[0]: os.path.split(fil)[1]}
                else:
                    for tstmp in tstmps:
                        self.xyzrph[str(serial_number)]['sonar_type'][tstmp] = sonar_model
                        self.xyzrph[str(serial_number)]['source'][tstmp] = os.path.split(fil)[1]
                    for sensor in mbesxyzrph:
                        for tstmp in tstmps:
                            try:
                                self.xyzrph[str(serial_number)][sensor][tstmp] = mbesxyzrph[sensor][tstmp]
                            except:
                                raise ValueError('ERROR: Unable to load SN{} with sensor {} at timestamp {}'.format(serial_number, sensor, tstmp))
                self.load_from_existing_xyzrph()
            else:
                self.print('Unable to load from {}'.format(fil), logging.ERROR)
        else:
            self.print('Import cancelled', logging.INFO)

    def _update_xyzrph_vesselposition(self, serial_number, tstmps):
        for tstmp in tstmps:
            sensors = ['vessel_file', 'vess_center_x', 'vess_center_y', 'vess_center_z', 'vess_center_r',
                       'vess_center_p', 'vess_center_yaw', 'sensor_size']
            sensor_data = [{tstmp: os.path.normpath(self.vessview_window.pth_to_vessel_file)}, {tstmp: '0.000'},
                           {tstmp: '0.000'}, {tstmp: '0.000'}, {tstmp: '0.000'}, {tstmp: '0.000'}, {tstmp: '0.000'},
                           {tstmp: format(launch_sensor_size, '.3f')}]
            for cnt, sensor in enumerate(sensors):
                vess_entry = sensor_data[cnt]
                if sensor not in self.xyzrph[serial_number]:
                    self.xyzrph[serial_number][sensor] = vess_entry
                else:
                    for ky, val in vess_entry.items():
                        if ky not in self.xyzrph[serial_number][sensor]:
                            self.xyzrph[serial_number][sensor][ky] = val

    def new_configuration(self):
        """
        Open a blank instance of the vessel view
        """

        if self._handle_close_event():
            self.opts_window.config_name.setText('None')
            self.xyzrph = None
            self.populate_from_xyzrph()
            self.vessview_window.clear_sensors()
            self.vessview_window.clear_vessel()

    def save_configuration(self, event=None):
        """
        Save the xyzrph to kluster configuration file.  If you haven't changed the vessel or vessel position, you
        won't have recorded it just yet to xyzrph.  We do a last minute check to ensure the save config has all the
        right information.
        """

        if self.opts_window.config_name.text() == 'None':  # this data loaded was not from a config file, so we can update or save a new file
            save_first = AcceptDialog('Do you want to create a new vessel file or update the multibeam data offsets?\n\n' +
                                      '(update only works when this data was loaded from the main Kluster display)')
            save_first.button(QtWidgets.QMessageBox.Yes).setText('Create')
            save_first.button(QtWidgets.QMessageBox.No).setText('Update')
            save_state = save_first.run()
            if save_state == 'yes':
                self.save_to_config_file()
            elif save_state == 'no':
                self.converted_xyzrph_modified.emit(self.xyzrph)
                pass
            else:
                if event:
                    event.ignore()
                return
        else:
            self.save_to_config_file()

    def add_to_configuration(self):
        """
        Add or modify the existing entry for this system in the vessel file with the currently loaded data
        """

        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster',
                                                         Title='Add our configuration to this configuration file',
                                                         AppName='klustersave', bMulti=False, bSave=False,
                                                         fFilter='Kluster configuration (*.kfc)')
        if fil:
            if os.path.exists(fil):
                vf = VesselFile(fil)
                for i in range(self.opts_window.serial_select.count()):
                    serial_num = self.opts_window.serial_select.itemText(i)
                    first_sensor = list(self.xyzrph[serial_num].keys())[0]
                    tstmps = list(self.xyzrph[serial_num][first_sensor].keys())
                    self._update_xyzrph_vesselposition(serial_num, tstmps)
                    vf.update(serial_num, self.xyzrph[serial_num])
                vf.save(fil)
                self.print('Saving to {}'.format(fil), logging.INFO)
            else:
                self.print('Unable to find file: {}'.format(fil), logging.ERROR)
        else:
            self.print('Add to configuration cancelled', logging.INFO)

    def load_from_existing_xyzrph(self):
        """
        Kind of messy right now, we want to load from a new xyzrph data set which involves some manually setting of
        gui controls to trigger signals.  Probably worth revisiting in the future.  Anyway, this will set the vessel
        model and position all sensors /  populate all the gui controls
        """

        for serial_num in list(self.xyzrph.keys()):
            first_sensor = list(self.xyzrph[serial_num].keys())[0]
            tstmps = list(self.xyzrph[serial_num][first_sensor].keys())
            self._update_xyzrph_vesselposition(serial_num, tstmps)

        serial_num = list(self.xyzrph.keys())[0]
        first_sensor = list(self.xyzrph[serial_num].keys())[0]
        tstmps = list(self.xyzrph[serial_num][first_sensor].keys())
        first_tstmp = tstmps[0]

        # set the vessel specific information
        try:
            vess = os.path.normpath(self.xyzrph[serial_num]['vessel_file'][first_tstmp])
            vessindex = self.opts_window.vess_select.findText(os.path.split(vess)[1])
            currindex = self.opts_window.vess_select.findText(self.opts_window.vess_select.currentText())
            self.opts_window.vess_select.setCurrentIndex(vessindex)
            if vessindex == currindex:
                self.opts_window.vessel_selected(None)
            pos = [float(self.xyzrph[serial_num]['vess_center_x'][first_tstmp]), float(self.xyzrph[serial_num]['vess_center_y'][first_tstmp]),
                   float(self.xyzrph[serial_num]['vess_center_z'][first_tstmp]), float(self.xyzrph[serial_num]['vess_center_r'][first_tstmp]),
                   float(self.xyzrph[serial_num]['vess_center_p'][first_tstmp]), float(self.xyzrph[serial_num]['vess_center_yaw'][first_tstmp]),
                   float(self.xyzrph[serial_num]['sensor_size'][first_tstmp])]
            self.opts_window.update_sensor_data('Vesselcenter', *pos)
        except KeyError:
            self.opts_window.vess_select.setCurrentIndex(0)
        self.populate_from_xyzrph()

    def load_test_dataset(self):
        """
        Load from the test dataset for standard EM2040 launch system
        """

        self.new_configuration()
        self.xyzrph = deepcopy(test_xyzrph)
        self.load_from_existing_xyzrph()

    def load_dualhead_test_dataset(self):
        """
        Load from the test dataset for dual head EM2040 as seen on Ferdinand Hassler
        """

        self.new_configuration()
        self.xyzrph = deepcopy(test_xyzrph_dual)
        self.load_from_existing_xyzrph()

    def load_from_config_file(self, config_file: str):
        self.print('Loading from {}'.format(config_file), logging.INFO)
        self.opts_window.config_name.setText(os.path.split(config_file)[1])
        self.vessview_window.clear_sensors()
        vf = VesselFile(config_file)
        self.xyzrph = vf.data
        self.load_from_existing_xyzrph()

    def save_to_config_file(self):
        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster',
                                                         Title='Save the kluster configuration to disk',
                                                         AppName='klustersave', bMulti=False, bSave=True,
                                                         fFilter='Kluster configuration (*.kfc)')
        if fil:
            if self.xyzrph is not None:
                self.opts_window.config_name.setText(os.path.split(fil)[1])
                vf = VesselFile()
                vf.data = self.xyzrph
                vf.save(fil)
                self.print('Saving to {}'.format(fil), logging.INFO)
                self.vessel_file_modified.emit(True)
            else:
                self.print('No data found for xyzrph: {}'.format(self.xyzrph), logging.ERROR)
        else:
            self.print('Save cancelled', logging.INFO)

    def open_configuration(self):
        """
        Open the kluster configuration file and store it as xyzrph
        """

        self.new_configuration()
        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster',
                                                         Title='Open a kluster configuration file',
                                                         AppName='klustersave', bMulti=False, bSave=False,
                                                         fFilter='Kluster configuration (*.kfc)')
        if fil:
            if os.path.exists(fil):
                self.load_from_config_file(fil)
            else:
                self.print('Unable to find file: {}'.format(fil), logging.ERROR)
        else:
            self.print('Open cancelled', logging.INFO)

    def _handle_close_event(self, event=None):
        """
        Each time the dialog is closed, runs this dialog to check if we want to save changes.
        """
        if self.xyzrph:
            save_first = AcceptDialog("Do you want to save your changes?", 'Kluster Vessel Setup')
            save_state = save_first.run()
            if save_state == 'yes':
                self.save_configuration(event)
                return True
            elif save_state == 'no':
                return True
            elif save_state == 'cancel':
                if event:
                    event.ignore()
                return False
        else:
            return True

    def closeEvent(self, event):
        """
        override the close event for the mainwindow, attach saving settings
        """
        if self._handle_close_event(event):
            super(VesselWidget, self).closeEvent(event)


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    win = VesselWidget()
    win.show()
    app.exec_()
