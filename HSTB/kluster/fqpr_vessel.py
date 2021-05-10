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
            identical_offsets, identical_tpu, data_matches = compare_dict_data(self.data[serial_number], data)
            if not identical_offsets or not identical_tpu:
                if carry_over_tpu:
                    new_data = carry_over_optional(self.data[serial_number], deepcopy(data))
                else:
                    new_data = data
                for entry in new_data.keys():
                    for ky, val in new_data[entry].items():
                        self.data[serial_number][entry][ky] = val
                only_retain_earliest_entry(self.data[serial_number])
        else:
            self.data[serial_number] = data

    def save(self, filepath: str = None):
        if not filepath:
            filepath = self.source_file
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
            return None


def create_new_vessel_file(filepath: str):
    vf = VesselFile()
    vf.save(filepath)
    return vf


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
        # raise ValueError('VesselFile: Found no overlapping timestamps for range {} -> {}, within the available timestamps: {}'.format(starttime, endtime, timestamps))
        return final_timestamps
    starttime = starting_timestamp
    final_timestamps.append(str(starttime))
    for tstmp in timestamps:
        if (tstmp > starttime) and (tstmp <= endtime):
            final_timestamps.append(str(tstmp))
    return final_timestamps


def compare_dict_data(dict_one: dict, dict_two: dict):
    check = {'identical_offsets': True, 'identical_tpu': True, 'data_matches': True}
    for sensor_one, data_one in dict_one.items():
        # only care about non-tpu differences
        if (sensor_one in kluster_variables.tpu_parameter_names) or (sensor_one in kluster_variables.optional_tpu_parameter_names):
            ky = 'identical_tpu'
        elif sensor_one not in kluster_variables.optional_parameter_names:
            ky = 'identical_offsets'
        else:
            continue
        if sensor_one in dict_two:
            data_two = dict_two[sensor_one]
            if check['identical_tpu'] or check['identical_offsets']:
                for tstmp, entry in data_one.items():
                    if tstmp in data_two:
                        if float(data_one[tstmp]) != float(data_two[tstmp]):
                            check[ky] = False
                    else:
                        check[ky] = False
            if check['data_matches']:
                vals_one = [data_one[t] for t in [t for t in data_one.keys()]]
                vals_two = [data_two[t] for t in [t for t in data_two.keys()]]
                if vals_one != vals_two:
                    check['data_matches'] = False
        else:
            check = {'identical_offsets': False, 'identical_tpu': False, 'data_matches': False}
            break
    return check['identical_offsets'], check['identical_tpu'], check['data_matches']


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


def only_retain_earliest_entry(data: dict):
    first_entry = list(data.keys())[0]
    timestamps = [tstmp for tstmp in data[first_entry].keys()]
    remove_these = []
    for primary_cnt, timestamp in enumerate(timestamps):
        for secondary_cnt, sec_timestamp in enumerate(timestamps):
            if primary_cnt == secondary_cnt or timestamp in remove_these or sec_timestamp in remove_these:
                continue
            prim_values = [data[entry][timestamp] for entry in data]
            sec_values = [data[entry][sec_timestamp] for entry in data]
            if prim_values == sec_values:
                if int(timestamp) >= int(sec_timestamp):
                    remove_these.append(timestamp)
                else:
                    remove_these.append(sec_timestamp)
    if remove_these:
        for entry in data:
            for tstmp in remove_these:
                data[entry].pop(tstmp)


def convert_from_fqpr_xyzrph(xyzrph: dict, sonar_model: str, system_identifier: str, source_identifier: str):
    first_sensor = list(xyzrph.keys())[0]
    tstmps = list(xyzrph[first_sensor].keys())
    vess_xyzrph = {str(system_identifier): xyzrph}
    vess_xyzrph[str(system_identifier)]['sonar_type'] = {tst: sonar_model for tst in tstmps}
    vess_xyzrph[str(system_identifier)]['source'] = {tst: source_identifier for tst in tstmps}
    return vess_xyzrph
