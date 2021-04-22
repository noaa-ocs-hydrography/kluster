class BaseBackend:
    def __init__(self, output_folder: str = None):
        self.output_folder = output_folder
        self.client = None
        self.show_progress = False
        self.parallel_write = True

    def write(self, dataset_name, data, time_array, attributes, sys_id=None, append_dim='time', skip_dask=False):
        raise NotImplementedError('BaseBackend: write method must be implemented')

    def write_attributes(self, dataset_name, attributes, sys_id=None):
        raise NotImplementedError('BaseBackend: write_attributes method must be implemented')
