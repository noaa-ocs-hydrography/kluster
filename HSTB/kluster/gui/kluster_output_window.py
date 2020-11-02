import sys
from queue import Queue
from PySide2 import QtGui, QtCore, QtWidgets


class WriteStream(object):
    """
    The new Stream Object which replaces the default stream associated with sys.stdout
    This object just puts data in a queue!

    Parameters
    ----------
    queue: queue.Queue object for holding stdout/stderr data
    """
    def __init__(self, queue):
        self.queue = queue

    def write(self, text):
        self.queue.put(text)

    def flush(self):
        pass


class MyReceiver(QtCore.QObject):
    """
    A QObject (to be run in a QThread) which sits waiting for data to come through a queue.Queue().  It blocks until
    data is available, and one it has got something from the queue, it sends it to the "MainThread" by emitting a Qt
    Signal

    Parameters
    ----------
    queue: queue.Queue object for holding stdout/stderr data, this will monitor
    """

    mysignal = QtCore.Signal(str)

    def __init__(self, queue, *args, **kwargs):
        QtCore.QObject.__init__(self, *args, **kwargs)
        self.queue = queue

    @QtCore.Slot()
    def run(self):
        while True:
            text = self.queue.get()
            self.mysignal.emit(text)


class KlusterOutput(QtWidgets.QTextEdit):
    """
    TextEdit widget for displaying stdout/stderr messages.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("Output")
        self.setObjectName('kluster_output')
        self.setReadOnly(True)

        self.queue = Queue()
        self.queue_thread = QtCore.QThread()
        self.queue_receiver = MyReceiver(self.queue)
        self.queue_receiver.mysignal.connect(self.append_text)
        self.queue_receiver.moveToThread(self.queue_thread)

        self.queue_thread.started.connect(self.queue_receiver.run)
        self.queue_thread.start()

        sys.stdout = WriteStream(self.queue)
        sys.stderr = WriteStream(self.queue)

    @QtCore.Slot(str)
    def append_text(self, text):
        cursor = self.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        cursor.insertText(text)
        self.setTextCursor(cursor)

    def __del__(self):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterExplorer for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.resize(800, 400)
        self.setWindowTitle('Kluster Output Window')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.k_output = KlusterOutput(self)
        self.k_output.moveCursor(QtGui.QTextCursor.Start)
        self.k_output.setLineWrapColumnOrWidth(500)
        self.k_output.setLineWrapMode(QtWidgets.QTextEdit.FixedPixelWidth)
        self.k_output.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        layout.addWidget(self.k_output)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()

        print('test stdout', file=sys.stdout)
        print('test stderr', file=sys.stderr)


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())
