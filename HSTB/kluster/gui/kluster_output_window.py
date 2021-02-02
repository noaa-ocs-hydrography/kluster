import sys, os
from queue import Queue
from PySide2 import QtGui, QtCore, QtWidgets


class OutputWrapper(QtCore.QObject):
    outputWritten = QtCore.Signal(object, object)

    def __init__(self, parent, stdout=True):
        QtCore.QObject.__init__(self, parent)
        if stdout:
            self._stream = sys.stdout
            sys.stdout = self
        else:
            self._stream = sys.stderr
            sys.stderr = self
        self._stdout = stdout

    def write(self, text):
        # self._stream.write(text)
        self.outputWritten.emit(text, self._stdout)

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __del__(self):
        try:
            if self._stdout:
                sys.stdout = self._stream
            else:
                sys.stderr = self._stream
        except AttributeError:
            pass


class KlusterOutput(QtWidgets.QTextEdit):
    """
    TextEdit widget for displaying stdout/stderr messages.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("Output")
        self.setObjectName('kluster_output')
        self.setReadOnly(True)
        # self.setStyleSheet(('font: 11pt "Consolas";'))

        self.stdout_obj = OutputWrapper(self, True)
        self.stdout_obj.outputWritten.connect(self.append_text)
        self.stderr_obj = OutputWrapper(self, False)
        self.stderr_obj.outputWritten.connect(self.append_text)

    def append_text(self, text, stdout):
        """
        add text as it shows up in the buffer, ordinarily this means moving the cursor to the end of the line and inserting
        the new text.

        For the dask progressbar though, we want to just overwrite the current line

        ex: '[#####                                   ] | 14% Completed |  1.6s'

        Parameters
        ----------
        text
            text to insert
        stdout
            if True, is stdout
        """
        cursor = self.textCursor()
        if text.lstrip()[0:2] in ['[#', '[ ']:
            # try and see if we need to continue a previous bar, so you dont get a list of 100% progress bars
            cursor.select(QtGui.QTextCursor.LineUnderCursor)
            cursor.removeSelectedText()
            cursor.deletePreviousChar()
            cursor.insertText(text)
        else:
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
