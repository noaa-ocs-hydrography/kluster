import os
import json
from copy import deepcopy

from HSTB.kluster import kluster_variables


class VesselFile:
    """
    Class to manage the vessel configuration file (.kfc) for Kluster.  Holds the tpu parameters and lever arm information
    for each system in the project.  Stored in a nested dictionary that looks like this:

    {serial number1: {sensor_name1: {utc timestamp1: value, utc timestamp2: value, ...},
                      sensor_name2: {utc timestamp1: value, utc timestamp2: value, ...}, ...},
     serial number2: {sensor_name1: {utc timestamp1: value, utc timestamp2: value, ...},
                      sensor_name2: {utc timestamp1: value, utc timestamp2: value, ...}, ...}, ... }
    """

    def __init__(self, filepath: str = None):
        self.data = {}
        self.source_file = ''
        if filepath:
            self.open(filepath)

    def open(self, filepath: str):
        if not os.path.exists(filepath):
            raise ValueError('VesselFile: {} can not be found'.format(filepath))
        filext = os.path.splitext(filepath)[1]
        if filext != '.kfc':
            raise ValueError('VesselFile: {} is not a valid Kluster configuration file (.kfc)'.format(filepath))
        with open(filepath, 'r') as json_fil:
            self.data = json.load(json_fil)
        self.source_file = filepath

    def update(self, serial_number: str, data: dict, carry_over_tpu: bool = True):
        if serial_number in self.data:
            identical = compare_dict_data(self.data[serial_number], data)
            if not identical:
                if carry_over_tpu:
                    new_data = carry_over_optional(self.data[serial_number], deepcopy(data))
                else:
                    new_data = data
                for entry in new_data.keys():
                    for ky, val in new_data[entry].items():
                        self.data[serial_number][entry][ky] = val
        else:
            self.data[serial_number] = data

    def save(self, filepath: str):
        with open(filepath, 'w') as json_fil:
            json.dump(self.data, json_fil, indent=4)
        self.source_file = filepath

    def return_data(self, serial_number: str, starttime: int, endtime: int):
        subset_data = {}
        if serial_number in self.data:
            first_sensor = list(self.data[serial_number].keys())[0]
            timestamps = [int(f) for f in list(self.data[serial_number][first_sensor].keys())]
            final_timestamps = get_overlapping_timestamps(timestamps, starttime, endtime)
            for entry in self.data[serial_number].keys():
                subset_data[entry] = {}
                for tstmp in final_timestamps:
                    subset_data[entry][tstmp] = self.data[serial_number][entry][tstmp]
            return subset_data
        else:
            raise ValueError('VesselFile: Unable to find serial number {} in vessel file'.format(serial_number))


def get_overlapping_timestamps(timestamps: list, starttime: int, endtime: int):
    final_timestamps = []
    # we require a starting time stamp that is either less than the given starttime or no greater than
    #   the given starttime by 60 seconds
    buffer = 60
    starting_timestamp = None
    for tstmp in timestamps:
        if tstmp < starttime + buffer:
            if not starting_timestamp:
                starting_timestamp = tstmp
            elif (tstmp > starting_timestamp) and (tstmp <= starttime):
                starting_timestamp = tstmp
    if starting_timestamp is None:
        raise ValueError('VesselFile: Found no overlapping timestamps for range {} -> {}, within the available timestamps: {}'.format(starttime, endtime, timestamps))
    starttime = starting_timestamp
    final_timestamps.append(str(starttime))
    for tstmp in timestamps:
        if (tstmp > starttime) and (tstmp <= endtime):
            final_timestamps.append(str(tstmp))
    return final_timestamps


def compare_dict_data(dict_one: dict, dict_two: dict):
    for sensor_one, data_one in dict_one.items():
        # only care about non-tpu differences
        if (sensor_one in kluster_variables.tpu_parameter_names) or (sensor_one in kluster_variables.optional_parameter_names):
            continue
        if sensor_one in dict_two:
            data_two = dict_two[sensor_one]
            for tstmp, entry in data_one.items():
                if tstmp in data_two:
                    if float(data_one[tstmp]) != float(data_two[tstmp]):
                        return False
                else:
                    return False
        else:
            return False
    return True


def carry_over_optional(starting_data: dict, new_data: dict):
    first_entry = list(starting_data.keys())[0]
    starting_tstmps = [int(tstmp) for tstmp in starting_data[first_entry].keys()]
    first_new_entry = list(new_data.keys())[0]
    last_tstmp = str(max(starting_tstmps))
    for sensor in starting_data:
        if (sensor in kluster_variables.tpu_parameter_names) or (sensor in kluster_variables.optional_parameter_names):
            if sensor not in new_data:
                new_data[sensor] = {}
            for tstmp, val in new_data[first_new_entry].items():
                new_data[sensor][tstmp] = starting_data[sensor][last_tstmp]
    return new_data
