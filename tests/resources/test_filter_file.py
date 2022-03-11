from HSTB.kluster.modules.filter import BaseFilter


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        # when running from Kluster GUI, these two controls will pop up after starting the filter to ask for min and max depth
        self.controls = []
        self.description = 'only for testing'

    def _run_algorithm(self):
        self.new_status = []