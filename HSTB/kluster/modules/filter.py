import os
import importlib.util
from types import ModuleType


class FilterManager:
    """
    Class that finds and runs filters found in the current file system.  Filters are designed to flag soundings based
    on custom algorithms, i.e. filter soundings with beam numbers > 45.  Any python file with a Filter class found in
    the plugins/filters folder or in the external_filter_directory will be loaded and accessible from this class.
    """
    def __init__(self, fqpr=None, external_filter_directory: str = None):
        """
        Parameters
        ----------
        fqpr
            fully qualified ping record, the term for the datastore in kluster, the main object returned
            by processing and reload methods, see fqpr_convenience
        external_filter_directory
            path to a folder where you have your user designed filter python files, if you have any
        """

        self.fqpr = fqpr
        self.external_filter_directory = external_filter_directory
        self.base_filter_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plugins', 'filters')

        self.filter_names = []  # ex: ['filter_by_beam']
        self.filter_lookup = {}  # ex: {'filter_by_beam': <filter_by_beam.Filter at 0x28e1b6afa00>}
        self.reverse_filter_lookup = {}  # ex: {<filter_by_beam.Filter at 0x28e1b6afa00>: 'filter_by_beam'}
        self.filter_file = {}  # ex: {'filter_by_beam': "C:\\Pydro21_Dev\\noaa\\site-packages\\python38\\git_repos\\hstb_kluster\\HSTB\\kluster\\plugins\\filters\\filter_by_beam.py"}
        self.initialize_filters()  # ex:

    def clear_filters(self):
        """
        clear the lookup lists/dicts that are populated during initialize_filters
        """

        self.filter_names = []
        self.filter_lookup = {}
        self.reverse_filter_lookup = {}
        self.filter_file = {}

    def list_filters(self):
        """
        Return the names of the loaded filters

        Returns
        -------
        list
            list of filter names (file names of the files) for all loaded filters.
        """

        return self.filter_names

    def initialize_filters(self):
        """
        Load all filters from the plugins/filters directory and the external_filter_directory if that attribute is set.

        Loaded data can be seen in the attributes of this class, see filter_names, filter_lookup, etc.
        """

        self.clear_filters()
        potential_dirs = [self.base_filter_directory]
        if self.external_filter_directory:
            if os.path.exists(self.external_filter_directory):
                potential_dirs += [self.external_filter_directory]
            else:
                print(f'initialize_filters: skipping {self.external_filter_directory}, as {self.external_filter_directory} is a path to a folder that does not exist.')
        for dirpath in potential_dirs:
            for filtername in os.listdir(dirpath):
                if filtername == '__init__.py':
                    continue
                filterbase, filter_extension = os.path.splitext(filtername)
                filterpath = os.path.join(dirpath, filtername)
                if filterbase in self.filter_names:
                    print(f'initialize_filters: skipping {filterpath}, as {filterbase} is already in the list of loaded filters')
                    continue
                if os.path.splitext(filtername)[1] == '.py':
                    filtermodule, filterclass = self._get_filter_class(filterpath)
                    if filtermodule is not None and filterclass is not None:
                        self.filter_names.append(filterbase)
                        self.filter_lookup[filterbase] = filterclass
                        self.reverse_filter_lookup[filterclass] = filterbase
                        self.filter_file[filterbase] = filterpath
                    else:
                        print(f'initialize_filters: skipping {filterpath}, Unable to load Filter class')

    def _get_filter_class(self, filepath: str):
        """
        Use importlib to load the given path to a python module and return the module/filter class

        Parameters
        ----------
        filepath
            path to a python .py file that has the required Filter class

        Returns
        -------
        ModuleType
            python module loaded from the given file
        BaseFilter
            Filter class from that given python file
        """

        folderpath, fname = os.path.split(filepath)
        fnamebase, fnameext = os.path.splitext(fname)
        try:
            spec = importlib.util.spec_from_file_location(fnamebase, filepath)
            foo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(foo)
        except (FileNotFoundError, ModuleNotFoundError):
            print(f'get_filter_class: Unable to find filter that is supposed to be at {filepath}')
            return None, None
        try:
            return foo, foo.Filter(self.fqpr)
        except AttributeError:
            print(f'get_filter_class: Found filter {filepath}, but did not find the required "Filter" class')
            return None, None

    def return_filter_class(self, filtername: str):
        """
        Return the Filter class associated with the given filtername.  filtername should be the name of the file
        that contains the Filter class you want.

        Parameters
        ----------
        filtername
            name of the file that you want to load

        Returns
        -------
        BaseFilter
            Filter class from that given python file
        """

        if filtername in self.filter_lookup:
            return self.filter_lookup[filtername]
        else:
            print(f'return_filter_class: no loaded filter for filter name {filtername}')
            return None

    def return_optional_filter_controls(self, filtername: str):
        """
        Get the requested gui control parameters for the given filter.

        Parameters
        ----------
        filtername
            name of the file that you want to load

        Returns
        -------
        list
            list of gui control parameter lists
        """

        filterclass = self.return_filter_class(filtername)
        return filterclass.controls

    def run_filter(self, filtername: str, selected_index: list = None, save_to_disk: bool = True, **kwargs):
        """
        Run the Filter class from the given filter name.  filtername should be the name of the file that contains the
        Filter class you want.

        Parameters
        ----------
        filtername
            name of the file that you want to load
        selected_index
            optional list of 1d boolean arrays representing the flattened index of those values to retain.  Used mainly
            in Points View filtering, where you have a (time,beam) space but only want to retain the beams shown in
            Points View.
        save_to_disk
            if True, will save the new sounding status to disk
        """

        filterclass = self.return_filter_class(filtername)
        if filterclass is not None:
            filterclass._selected_index = selected_index
            new_status = filterclass.run(**kwargs)
            if save_to_disk:
                filterclass.save()
            return new_status
        return None


class BaseFilter:
    """
    All custom filters will inherit this class.  Contains the base run/save methods and attributes that all filters
    need to run.  When you write a custom filter (see plugins/filters), your algorithm will be contained within the
    _run_algorithm method.  If you need to prompt the user for a value, set the controls attribute (see
    plugins/filters/filter_by_angle for an example of that) and a custom GUI window will pop up ASSUMING YOU ARE RUNNING
    FROM KLUSTER GUI.  Otherwise, you'll need to pass those arguments when you call run_filter.
    """
    def __init__(self, fqpr, selected_index=None):
        self.fqpr = fqpr  # the kluster Fully Qualified Ping Record (FQPR), this is the datasets containing the raw and processed data
        self._selected_index = selected_index  # an optional index if you only want to save a subset of the fqpr
        self.new_status = None  # this is the new detectioninfo that should be the result of the algorithm
        # each sublist in controls will populate a new GUI control to ask for values when running from Kluster GUI
        #  (see dialog_filter.AdditionalFilterOptionsDialog to get a better idea of how it is implemented)
        #  self.controls = [[datatype, name, start_value, qt_properties]
        #  where:
        #  datatype = one of [float, int, text, checkbox, combobox], this controls what kind of GUI control you get
        #  name = sets the label of the control, MUST MATCH THE _run_algorithm KEY WORD ARGUMENT
        #  start_value = the starting value of the control
        #  qt_properties = dict of properties that QT can apply with setProperty
        self.controls = []

    def _run_algorithm(self, **kwargs):
        """
        This will contain your custom algorithm
        """
        raise NotImplementedError('BaseFilter: you must create a Filter class and implement this method')

    def run(self, **kwargs):
        self._run_algorithm(**kwargs)
        try:
            assert [ns.ndim == 2 for ns in self.new_status]
        except AssertionError:
            print(f'BaseFilter: expect new_status returned from filter to be 2 dimensional, got {[ns.shape for ns in self.new_status]}')
        return self.new_status

    def save(self):
        if self.new_status is None or not isinstance(self.new_status, list):
            print('BaseFilter: unable to save new sounding flags, new_status should be a list of arrays, one array for '
                  'each sonar head (len(self.new_status) must equal len(self.fqpr.multibeam.raw_ping)')
            return
        if self._selected_index and not isinstance(self._selected_index, list):
            print('BaseFilter: unable to save new sounding flags, the optional selected_index should be a list of arrays, one array for '
                  'each sonar head (len(self.selected_index) must equal len(self.fqpr.multibeam.raw_ping)')
            return
        if self._selected_index:  # save when you have a subset selected, such as when you are filtering Points View points
            for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
                rp_detect = rp['detectioninfo'].load()  # convert to numpy and load in memory
                rp_detect_values = rp_detect.values
                sel_index = self._selected_index[cnt].reshape(rp_detect.shape)  # reshape selected index to the 2d array
                rp_detect_values[sel_index] = self.new_status[cnt][sel_index]  # overwrite all values matching selected index with new status
                rp_detect[:] = rp_detect_values  # write the change back to the xarray dataarray
                # save to disk
                self.fqpr.write('ping', [rp_detect.to_dataset()], time_array=[rp_detect.time], sys_id=rp.system_identifier, skip_dask=True)
        else:  # expect that the new_status is the same size as the existing status, no subset
            for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
                rp_detect = rp['detectioninfo'].load()  # convert to numpy and load in memory
                rp_detect[:] = self.new_status[cnt]  # overwrite with new status
                # save to disk
                self.fqpr.write('ping', [rp_detect.to_dataset()], time_array=[rp_detect.time], sys_id=rp.system_identifier, skip_dask=True)

    def return_controls(self):
        return self.controls


if __name__ == '__main__':
    fm = FilterManager()
    print('Filters currently loaded')
    print(fm.list_filters())

    from HSTB.kluster.fqpr_convenience import reload_data
    fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710")
    fq.filter.run_filter('filter_by_angle', min_angle=-45, max_angle=45)
