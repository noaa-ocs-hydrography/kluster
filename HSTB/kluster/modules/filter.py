import os, sys
import importlib.util


class FilterManager:
    def __init__(self, external_filter_directory: str = None):
        self.external_filter_directory = external_filter_directory
        self.base_filter_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plugins', 'filters')

        self.filter_names = []
        self.filter_lookup = {}
        self.reverse_filter_lookup = {}
        self.module_lookup = {}
        self.initialize_filters()

    def clear_filters(self):
        self.filter_names = []
        self.filter_lookup = {}
        self.reverse_filter_lookup = {}
        self.module_lookup = {}

    def initialize_filters(self):
        self.clear_filters()
        potential_dirs = [self.base_filter_directory]
        if self.external_filter_directory:
            potential_dirs += [self.external_filter_directory]
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
                    filtermodule, filterclass = self.get_filter_class(filterpath)
                    if filtermodule is not None and filterclass is not None:
                        self.filter_names.append(filterbase)
                        self.filter_lookup[filterbase] = filterclass
                        self.reverse_filter_lookup[filterclass] = filterbase
                        self.module_lookup[filterbase] = filtermodule
                    else:
                        print(f'initialize_filters: skipping {filterpath}, Unable to load Filter class')
                else:
                    print(f'initialize_filters: skipping {filterpath}, not a valid python module')

    def get_filter_class(self, filepath: str):
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
            return foo, foo.Filter()
        except AttributeError:
            print(f'get_filter_class: Found filter {filepath}, but did not find the required "Filter" class')
            return None, None

