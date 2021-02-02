from PySide2 import QtCore

from HSTB.kluster.fqpr_convenience import generate_new_surface


class ActionWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.action_container = None
        self.action_index = None

        self.result = None
        self.action_type = None

    def populate(self, action_container, action_index):
        self.action_container = action_container
        self.action_index = action_index

    def run(self):
        self.started.emit(True)
        # turn off progress, it creates too much clutter in the output window
        self.action_type = self.action_container.actions[self.action_index].action_type
        self.result = self.action_container.execute_action(self.action_index)
        self.finished.emit(True)


class ExportWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []
        self.export_type = ''
        self.z_pos_down = False
        self.delimiter = ' '
        self.filterset = False
        self.separateset = False

    def populate(self, fq_chunks, export_type, z_pos_down, delimiter, filterset, separateset):
        self.fq_chunks = fq_chunks
        self.export_type = export_type
        self.z_pos_down = z_pos_down
        if delimiter == 'comma':
            self.delimiter = ','
        elif delimiter == 'space':
            self.delimiter = ' '
        else:
            raise ValueError('ExportWorker: Expected either "comma" or "space", received {}'.format(delimiter))
        self.filterset = filterset
        self.separateset = separateset

    def export_process(self, fq):
        fq.export_pings_to_file(file_format=self.export_type, csv_delimiter=self.delimiter, filter_by_detection=self.filterset,
                                z_pos_down=self.z_pos_down, export_by_identifiers=self.separateset)
        return fq

    def run(self):
        self.started.emit(True)
        for chnk in self.fq_chunks:
            self.fqpr_instances.append(self.export_process(chnk[0]))
        self.finished.emit(True)


class SurfaceWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_instances = None
        self.fqpr_surface = None
        self.opts = []

    def populate(self, fqpr_instances, opts):
        self.fqpr_instances = fqpr_instances
        self.opts = opts

    def run(self):
        self.started.emit(True)
        self.fqpr_surface = generate_new_surface(self.fqpr_instances, **self.opts)
        self.finished.emit(True)
