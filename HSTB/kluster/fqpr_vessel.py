import os
import json
from copy import deepcopy

from HSTB.kluster import kluster_variables


class VesselFile:
    """
    Class to manage the vessel configuration file (.kfc) for Kluster.  Holds the tpu parameters and lever arm information
    for each system in the project.  Stored in a nested dictionary that looks like this:
    serial number1: {sensor_name1: {utc timestamp1: value, utc timestamp2: value, ...}, ...
    """

    def __init__(self, filepath: str = None):
        self.data = {}
        self.source_file = ''
        if filepath:
            self.open(filepath)

    def open(self, filepath: str):
        """
        Open from a Vessel File json instance (vessel configuration file (.kfc))

        Parameters
        ----------
        filepath
            absolute file path to the vessel file
        """

        if not os.path.exists(filepath):
            raise ValueError('VesselFile: {} can not be found'.format(filepath))
        filext = os.path.splitext(filepath)[1]
        if filext != '.kfc':
            raise ValueError('VesselFile: {} is not a valid Kluster configuration file (.kfc)'.format(filepath))
        with open(filepath, 'r') as json_fil:
            self.data = json.load(json_fil)
        self.source_file = filepath

    def update(self, serial_number: str, data: dict, carry_over_tpu: bool = True):
        """
        Call to update the internal vessel settings data (still must call save to write to disk).  If the data provided
        does not match the internal data, will overwrite the internal data.  Will do the following additional tasks:
        compare the provided data and the internal data and determine if:

        identical_offsets = offsets and angles match between the two data
        identical_tpu = tpu parameters match between the two data
        data_matches = the two data have values that exactly match (can match even if the keys are different, happens
        when timestamps (the keys) do not match but the data does.)
        populate the optional and tpu parameters in the new data with the latest existing entry
        compare all entries and only retain the earliest entry if you find two entries that match exactly

        Parameters
        ----------
        serial_number
            system identifier for the primary system (serial number of the sonar)
        data
            dictionary with the offsets, angles and tpu parameters for the new data to add to the vessel file
        carry_over_tpu
            if True, will use the latest existing tpu parameters to populate the newly provided data

        """

        if serial_number in self.data:
            identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(self.data[serial_number], data)
            if not identical_offsets or not identical_angles or not identical_tpu or new_waterline:
                if carry_over_tpu:
                    new_data = carry_over_optional(self.data[serial_number], deepcopy(data))
                else:
                    new_data = data
                for entry in new_data.keys():
                    for ky, val in new_data[entry].items():
                        self.data[serial_number][entry][ky] = val
                only_retain_earliest_entry(self.data[serial_number])
        else:
            print('Adding new entry in vessel file for {}'.format(serial_number))
            self.data[serial_number] = data

    def save(self, filepath: str = None):
        """
        Save the internal vessel file data to a json file

        Parameters
        ----------
        filepath
            absolute file path to the newly created vessel file
        """

        if not filepath:
            filepath = self.source_file
        with open(filepath, 'w') as json_fil:
            json.dump(self.data, json_fil, indent=4)
        self.source_file = filepath

    def return_data(self, serial_number: str, starttime: int, endtime: int):
        """
        Get the vessel file timstamped entries that fall within the provided starttime/endtime.

        Parameters
        ----------
        serial_number
            system identifier for the primary system (serial number of the sonar)
        starttime
            integer utc timestamp in seconds
        endtime
            integer utc timestamp in seconds

        Returns
        -------
        dict
            dictionary of the vessel file entries that fall within the provided time
        """
        subset_data = {}
        if serial_number in self.data:
            first_sensor = list(self.data[serial_number].keys())[0]
            timestamps = [int(f) for f in list(self.data[serial_number][first_sensor].keys())]
            final_timestamps = get_overlapping_timestamps(timestamps, starttime, endtime)
            if final_timestamps:
                for entry in self.data[serial_number].keys():
                    subset_data[entry] = {}
                    for tstmp in final_timestamps:
                        subset_data[entry][tstmp] = self.data[serial_number][entry][tstmp]
                return subset_data
            else:
                return None
        else:
            return None


def create_new_vessel_file(filepath: str):
    """
    Build a new vessel file at the provided path

    Parameters
    ----------
    filepath
        absolute file path to the new vessel file we want to make

    Returns
    -------
    VesselFile
        newly generated VesselFile instance
    """

    vf = VesselFile()
    vf.save(filepath)
    return vf


def get_overlapping_timestamps(timestamps: list, starttime: int, endtime: int):
    """
    Find the timestamps in the provided list of timestamps that fall between starttime/endtime.  Return these timestamps
    as a list.  First timestamp in the list is always the nearest to the starttime without going over.

    Parameters
    ----------
    timestamps
        list of timestamps we want to pull from, to get the timestamps between starttime and endtime
    starttime
        integer utc timestamp in seconds
    endtime
        integer utc timestamp in seconds

    Returns
    -------
    list
        list of timestamps that are within the starttime/endtime range
    """

    final_timestamps = []
    # we require a starting time stamp that is either less than the given starttime or no greater than
    #   the given starttime by 60 seconds
    buffer = 60
    starting_timestamp = None
    for tstmp in timestamps:  # first pass, find the nearest timestamp (to starttime) without going over the starttime
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
    for tstmp in timestamps:  # second pass, append all timestamps that are between the starting timestamp and endtime
        if (tstmp > starttime) and (tstmp <= endtime):
            final_timestamps.append(str(tstmp))
    return final_timestamps


def compare_dict_data(dict_one: dict, dict_two: dict):
    """
    Compare two dictionary objects to determine how identical they are.  data_two is the new data, so we do some checks
    to see if it is relevant or if we need to keep it.  Expect the dicts to be like:

    {sensor_name1: {utc timestamp1: value, utc timestamp2: value, ...},
     sensor_name2: {utc timestamp1: value, utc timestamp2: value, ...}, ...}

    Return a check that has attributes matching each check performed:
        - identical_offsets = offsets match between the two data
        - identical_angles = mounting angles match between the two data
        - identical_tpu = tpu parameters match between the two data
        - data_matches = the two data have values that exactly match (can match even if the keys are different, happens
                when timestamps (the keys) do not match but the data does.)
        - new_waterline = found a new waterline value in dict_two (the new data) that does not match data_one.  Return
                this in case we want to retain this value regardless of the other checks

    Parameters
    ----------
    dict_one
        base dict to compare against
    dict_two
        new dict to compare with

    Returns
    -------
    bool
        identical_offsets check value
    bool
        identical_angles check value
    bool
        identical_tpu check value
    bool
        data_matches check value
    float
        new waterline value found
    """

    check = {'identical_offsets': True, 'identical_angles': True, 'identical_tpu': True, 'data_matches': True,
             'new_waterline': None}
    for sensor_one, data_one in dict_one.items():
        # only care about non-tpu differences
        if sensor_one in kluster_variables.tpu_parameter_names:
            ky = 'identical_tpu'
        elif sensor_one in kluster_variables.offset_parameter_names:
            ky = 'identical_offsets'
        elif sensor_one in kluster_variables.angle_parameter_names:
            ky = 'identical_angles'
        elif sensor_one.lower() == 'waterline':
            ky = 'new_waterline'
        else:
            continue
        if sensor_one in dict_two:
            data_two = dict_two[sensor_one]
            if check['identical_tpu'] or check['identical_offsets'] or check['identical_angles']:
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
            if ky == 'new_waterline':
                waterline_one = data_one[list(data_one.keys())[0]]
                waterline_two = data_two[list(data_two.keys())[0]]
                if float(waterline_one) != float(waterline_two):
                    check['new_waterline'] = float(waterline_two)
        else:
            check = {'identical_offsets': False, 'identical_angles': False, 'identical_tpu': False, 'data_matches': False,
                     'new_waterline': None}
            break
    return check['identical_offsets'], check['identical_angles'], check['identical_tpu'], check['data_matches'], check['new_waterline']


def split_by_timestamp(xyzrph: dict):
    """
    Takes a Kluster xyzrph (the dictionary object that stores uncertainty, offsets, angles, etc. settings) and returns
    a new dictionary for each timestamped entry.

    Parameters
    ----------
    xyzrph
        dict of offsets/angles/tpu parameters from the fqpr instance

    Returns
    -------
    list
        list of dictionaries, one for each timestamp entry in the base xyzrph

    """
    first_sensor = list(xyzrph.keys())[0]
    tstmps = list(xyzrph[first_sensor].keys())
    split_data = [{} for t in tstmps]
    for ky, dictdata in xyzrph.items():
        for tstmp, val in dictdata.items():
            tindex = tstmps.index(tstmp)
            split_data[tindex][ky] = {tstmp: val}
    return split_data


def carry_over_optional(starting_data: dict, new_data: dict):
    """
    Populate the optional and tpu parameters in the new data with the latest existing entry

    Parameters
    ----------
    starting_data
        base dict to compare against
    new_data
        new dict to compare with

    Returns
    -------
    dict
        new_data with tpu parameters overrided with the latest entry in starting_data
    """

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
    """
    Compare all timestamps in the data, if you find duplicate values associated with two timestamps, only retain the
    earliest timestamp of the two.  Expects data in the format

    {sensor_name1: {utc timestamp1: value, utc timestamp2: value, ...},
     sensor_name2: {utc timestamp1: value, utc timestamp2: value, ...}, ...}

    Parameters
    ----------
    data
        dictionary of timestamped xyzrph entries to search through

    Returns
    -------
    dict
        dictionary object with the duplicate entries removed, retaining the earliest entry of all duplicates
    """

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
    """
    xyzrph data (offsets/angles/tpu parameters) are stored as an attribute in each zarr store for the fqpr instance.
    Here we convert that data to the format the the VesselFile wants, including the sonartype and source attribute.

    Parameters
    ----------
    xyzrph
        dict of offsets/angles/tpu parameters from the fqpr instance
    sonar_model
        sonar model identifier
    system_identifier
        identifier string for the system (the primary sonar serial number)
    source_identifier
        the source of the xyzrph, should be the multibeam file name or the converted folder name

    Returns
    -------
    dict
        converted xyzrph ready to be passed to VesselFile
    """

    xyzrph = deepcopy(xyzrph)  # don't alter the original
    first_sensor = list(xyzrph.keys())[0]
    tstmps = list(xyzrph[first_sensor].keys())
    vess_xyzrph = {str(system_identifier): xyzrph}
    vess_xyzrph[str(system_identifier)]['sonar_type'] = {tst: sonar_model for tst in tstmps}
    vess_xyzrph[str(system_identifier)]['source'] = {tst: source_identifier for tst in tstmps}
    return vess_xyzrph


def convert_from_vessel_xyzrph(vess_xyzrph: dict):
    """
    Take the vessel xyzrph we converted with the convert_from_fqpr_xyzrph function and convert back to the format
    used within the Fqpr instance.

    Parameters
    ----------
    vess_xyzrph
        vessel xyzrph generated with convert_from_fqpr_xyzrph that we want to convert back

    Returns
    -------
    dict
        dict of offsets/angles/tpu parameters for the fqpr instance
    """

    system_identifiers = list(vess_xyzrph.keys())
    xyzrph = []
    sonar_type = []
    source = []
    for sysident in system_identifiers:
        xdata = deepcopy(vess_xyzrph[sysident])
        xyzrph.append(xdata)
        if 'sonar_type' in xdata:
            sonar_type.append(xdata.pop('sonar_type'))
        if 'source' in xdata:
            source.append(xdata.pop('source'))
    return xyzrph, sonar_type, system_identifiers, source
