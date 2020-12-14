from PySide2 import QtCore

from HSTB.kluster.fqpr_convenience import convert_multibeam, process_multibeam, generate_new_surface, import_navigation


class ConversionWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.mbes_files = None
        self.output_folder = None
        self.client = None
        self.fq = None

    def populate(self, mbes_files, output_folder, client):
        self.mbes_files = mbes_files
        self.output_folder = output_folder
        self.client = client

    def run(self):
        self.started.emit(True)
        # turn off progress, it creates too much clutter in the output window
        self.fq = convert_multibeam(self.mbes_files, self.output_folder, self.client, show_progress=False)
        self.finished.emit(True)


class AllProcessingWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []

    def populate(self, fq_chunks):
        self.fq_chunks = fq_chunks

    def run(self):
        self.started.emit(True)
        for chnk in self.fq_chunks:
            self.fqpr_instances.append(process_multibeam(chnk[0], **chnk[1]))
        self.finished.emit(True)


class ImportNavigationWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = QtCore.Signal(bool)
    finished = QtCore.Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []

    def populate(self, fq_chunks):
        self.fq_chunks = fq_chunks

    def run(self):
        self.started.emit(True)
        for chnk in self.fq_chunks:
            self.fqpr_instances.append(import_navigation(chnk[0], **chnk[1]))
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

    def populate(self, fq_chunks, export_type):
        self.fq_chunks = fq_chunks
        self.export_type = export_type

    def export_process(self, fq):
        fq.export_pings_to_file(file_format=self.export_type)
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
