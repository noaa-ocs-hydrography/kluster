import sys
import os
from PySide2 import QtWidgets, QtGui, QtCore

from HSTB.shared import RegistryHelpers


# Current widgets that might be of interest:
#   BrowseListWidget - You need a list widget with browse buttons and removing of items built in?  Check this out

class DeletableListWidget(QtWidgets.QListWidget):
    """
    Inherit from the ListWidget and allow the user to press delete or backspace key to remove items
    """
    files_updated = QtCore.Signal(bool)

    def __init__(self, *args, **kwrds):
        super().__init__(*args, **kwrds)
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)

    def keyReleaseEvent(self, event):
        if event.matches(QtGui.QKeySequence.Delete) or event.matches(QtGui.QKeySequence.Back):
            for itm in self.selectedItems():
                self.takeItem(self.row(itm))
        self.files_updated.emit(True)


class BrowseListWidget(QtWidgets.QWidget):
    """
    List widget with insert/remove buttons to add or remove browsed file paths.  Will emit a signal on adding/removing
    items so you can connect it to other widgets.
    """
    files_updated = QtCore.Signal(bool)

    def __init__(self, parent):
        super().__init__(parent)

        self.layout = QtWidgets.QHBoxLayout()

        self.list_widget = DeletableListWidget(self)
        list_size_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Preferred)
        list_size_policy.setHorizontalStretch(2)
        self.list_widget.setSizePolicy(list_size_policy)
        self.layout.addWidget(self.list_widget)

        self.button_layout = QtWidgets.QVBoxLayout()
        self.button_layout.addStretch(1)
        self.insert_button = QtWidgets.QPushButton("Insert", self)
        self.button_layout.addWidget(self.insert_button)
        self.remove_button = QtWidgets.QPushButton("Remove", self)
        self.button_layout.addWidget(self.remove_button)
        self.button_layout.addStretch(1)
        self.layout.addLayout(self.button_layout)

        self.setLayout(self.layout)

        self.opts = {}
        self.setup()
        self.insert_button.clicked.connect(self.file_browse)
        self.remove_button.clicked.connect(self.remove_item)
        self.list_widget.files_updated.connect(self.files_changed)

    def setup(self, mode='file', registry_key='pydro', app_name='browselistwidget', supported_file_extension='*.*',
              multiselect=True, filebrowse_title='Select files', filebrowse_filter='all files (*.*)'):
        """
        keyword arguments for the widget.
        """
        self.opts = vars()

    def file_browse(self):
        """
        select a file and add it to the list.
        """
        fils = []
        if self.opts['mode'] == 'file':
            msg, fils = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey=self.opts['registry_key'],
                                                              Title=self.opts['filebrowse_title'],
                                                              AppName=self.opts['app_name'],
                                                              bMulti=self.opts['multiselect'], bSave=False,
                                                              fFilter=self.opts['filebrowse_filter'])
        elif self.opts['mode'] == 'directory':
            msg, fils = RegistryHelpers.GetDirFromUserQT(self, RegistryKey=self.opts['registry_key'],
                                                         Title=self.opts['filebrowse_title'],
                                                         AppName=self.opts['app_name'])
            fils = [fils]
        if fils:
            self.add_new_files(fils)
        self.files_changed()

    def add_new_files(self, files):
        """
        Add some new files to the widget, assuming they pass the supported extension option

        Parameters
        ----------
        files: list, list of string paths to files

        """
        files = sorted(files)
        supported_ext = self.opts['supported_file_extension']
        for f in files:
            if self.list_widget.findItems(f, QtCore.Qt.MatchExactly):  # no duplicates allowed
                continue

            if self.opts['mode'] == 'file':
                fil_extension = os.path.splitext(f)[1]
                if supported_ext == '*.*':
                    self.list_widget.addItem(f)
                elif type(supported_ext) is str and fil_extension == supported_ext:
                    self.list_widget.addItem(f)
                elif type(supported_ext) is list and fil_extension in supported_ext:
                    self.list_widget.addItem(f)
                else:
                    print('{} is not a supported file extension.  Must be a string or list of file extensions.'.format(
                        supported_ext))
                    return
            else:
                self.list_widget.addItem(f)

    def return_all_items(self):
        """
        Return all the items in the list widget

        Returns
        -------
        list
            list of strings for all items in the widget
        """
        items = [self.list_widget.item(i).text() for i in range(self.list_widget.count())]
        return items

    def remove_item(self):
        """
        remove a file from the list
        """
        for itm in self.list_widget.selectedItems():
            self.list_widget.takeItem(self.list_widget.row(itm))
        self.files_changed()

    def files_changed(self):
        self.files_updated.emit(True)


class CollapsibleWidget(QtWidgets.QWidget):
    """
    Transcribed to pyside from https://github.com/MichaelVoelkel/qt-collapsible-section/blob/master/Section.cpp
    """
    def __init__(self, parent: None, title: str, animation_duration: int):
        super().__init__(parent=parent)
        self.animation_duration = animation_duration
        self.title = title

        self.toggle_button = QtWidgets.QToolButton()
        self.header_line = QtWidgets.QFrame()
        self.toggle_animation = QtCore.QParallelAnimationGroup()
        self.content_area = QtWidgets.QScrollArea()
        self.main_layout = QtWidgets.QGridLayout()

        self.toggle_button.setStyleSheet("QToolButton { border: none; }")
        self.toggle_button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.toggle_button.setArrowType(QtCore.Qt.RightArrow)
        self.toggle_button.setText(str(title))
        self.toggle_button.setCheckable(True)
        self.toggle_button.setChecked(False)

        self.header_line.setFrameShape(QtWidgets.QFrame.HLine)
        self.header_line.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.header_line.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Maximum)

        self.content_area.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.content_area.setMaximumHeight(0)
        self.content_area.setMinimumHeight(0)

        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self, b'minimumHeight'))
        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self, b'maximumHeight'))
        self.toggle_animation.addAnimation(QtCore.QPropertyAnimation(self.content_area, b'maximumHeight'))

        self.main_layout.setVerticalSpacing(0)
        self.main_layout.setContentsMargins(0, 0, 0, 0)

        self.main_layout.addWidget(self.toggle_button, 0, 0, 1, 1, QtCore.Qt.AlignLeft)
        self.main_layout.addWidget(self.header_line, 0, 2, 1, 1)
        self.main_layout.addWidget(self.content_area, 1, 0, 1, 3)
        self.setLayout(self.main_layout)

        self.toggle_button.toggled.connect(self.toggle)

    def toggle(self, is_collapsed):
        if is_collapsed:
            self.toggle_button.setArrowType(QtCore.Qt.DownArrow)
            self.toggle_animation.setDirection(QtCore.QAbstractAnimation.Forward)
        else:
            self.toggle_button.setArrowType(QtCore.Qt.RightArrow)
            self.toggle_animation.setDirection(QtCore.QAbstractAnimation.Backward)
        self.toggle_animation.start()

    def setContentLayout(self, contentLayout):
        self.content_area.destroy()
        self.content_area.setLayout(contentLayout)
        collapsed_height = self.sizeHint().height() - self.content_area.maximumHeight()
        content_height = contentLayout.sizeHint().height()
        for i in range(self.toggle_animation.animationCount() - 1):
            collapse_animation = self.toggle_animation.animationAt(i)
            collapse_animation.setDuration(self.animation_duration)
            collapse_animation.setStartValue(collapsed_height)
            collapse_animation.setEndValue(collapsed_height + content_height)

        content_animation = self.toggle_animation.animationAt(self.toggle_animation.animationCount() - 1)
        content_animation.setDuration(self.animation_duration)
        content_animation.setStartValue(0)
        content_animation.setEndValue(content_height)


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the widget for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Test Window')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        # self.widg = BrowseListWidget(self)
        # self.widg.files_updated.connect(self.print_out_files)
        # layout.addWidget(self.widg)

        self.collapse = CollapsibleWidget(self, 'collapse', 300)
        self.datalayout = QtWidgets.QVBoxLayout()
        self.datalayout.addWidget(QtWidgets.QLabel('Some text in Section', self.collapse))
        self.datalayout.addWidget(QtWidgets.QPushButton('Some text in Section', self.collapse))
        self.collapse.setContentLayout(self.datalayout)
        layout.addWidget(self.collapse)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()

    def print_out_files(self):
        print([self.widg.list_widget.item(i).text() for i in range(self.widg.list_widget.count())])


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())