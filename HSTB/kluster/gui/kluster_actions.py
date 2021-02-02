import sys

from PySide2 import QtCore, QtGui, QtWidgets

from HSTB.kluster.fqpr_project import FqprProject


class KlusterActions(QtWidgets.QTreeView):
    """
    Tree view showing the currently available actions and files generated from fqpr_intelligence
    """

    execute_action = QtCore.Signal(int)
    exclude_queued_file = QtCore.Signal(str)
    exclude_unmatched_file = QtCore.Signal(str)
    undo_exclude_file = QtCore.Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.parent = parent
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.model = QtGui.QStandardItemModel()  # row can be 0 even when there are more than 0 rows
        self.setModel(self.model)
        self.setUniformRowHeights(False)
        self.setAcceptDrops(False)
        self.viewport().setAcceptDrops(False)  # viewport is the total rendered area, this is recommended from my reading

        # ExtendedSelection - allows multiselection with shift/ctrl
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        # set up the context menu per item
        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.right_click_menu_files = None
        self.setup_menu()

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.categories = ['Next Action', 'All Actions', 'Queued Files', 'Unmatched Files']
        self.tree_data = {}
        self.actions = None
        self.unmatched = None
        self.exclude_buffer = []

        self.start_button = QtWidgets.QPushButton('Start Process')
        self.start_button.setMaximumWidth(100)
        self.start_button.setMinimumHeight(25)
        self.start_button.clicked.connect(self.start_process)
        self.start_button.setDisabled(True)

        self.customContextMenuRequested.connect(self.show_context_menu)
        self.configure()

    def setup_menu(self):
        """
        Setup the menu that is generated on right clicking in the action tree.
        """
        self.right_click_menu_files = QtWidgets.QMenu('menu', self)

        exclude_dat = QtWidgets.QAction('Exclude File', self)
        exclude_dat.triggered.connect(self.exclude_file_event)
        undo_exclude_dat = QtWidgets.QAction('Undo Exclude', self)
        undo_exclude_dat.triggered.connect(self.undo_exclude)

        self.right_click_menu_files.addAction(exclude_dat)
        self.right_click_menu_files.addAction(undo_exclude_dat)

    def show_context_menu(self):
        """
        Open the right click menu if you right click a queued or unmatched file
        """
        index = self.currentIndex()
        parent_name = index.parent().data()
        if parent_name in ['Queued Files', 'Unmatched Files']:
            self.right_click_menu_files.exec_(QtGui.QCursor.pos())

    def exclude_file_event(self, e):
        """
        If user right clicks on a queued or unmatched file and selects exclude file, triggers this event.

        Emit signals depending on what kind of item the user selects.

        Parameters
        ----------
        e: QEvent on menu button click

        """
        selected_indexes = self.selectionModel().selectedIndexes()
        all_data = []
        xclude_data = []
        # allow multiselect, will emit for each selected and append the chunk to the buffer
        # have to do this in a first pass, as if we emit in the loop, the index will change when the line is removed
        for index in selected_indexes:
            parent_name = index.parent().data()
            sel_data = index.data()
            all_data.append([sel_data, parent_name])

        for sel_data, parent_name in all_data:
            if parent_name == 'Queued Files':
                self.exclude_queued_file.emit(sel_data)
                xclude_data.append(sel_data)
            elif parent_name == 'Unmatched Files':
                self.exclude_unmatched_file.emit(sel_data)
                xclude_data.append(sel_data)

        self.exclude_buffer.append(xclude_data)

    def undo_exclude(self, e):
        if self.exclude_buffer:
            self.undo_exclude_file.emit(self.exclude_buffer[-1])
            self.exclude_buffer = self.exclude_buffer[:-1]

    def configure(self):
        """
        Clears all data currently in the tree and repopulates with loaded actions

        """
        self.model.clear()
        self.model.setHorizontalHeaderLabels(['Actions'])
        for cnt, c in enumerate(self.categories):
            parent = QtGui.QStandardItem(c)
            self.tree_data[c] = [parent]
            self.model.appendRow(parent)
            self.setFirstColumnSpanned(cnt, self.rootIndex(), True)

            if c == 'Next Action':
                proj_child = QtGui.QStandardItem('')  # empty entry to overwrite with setIndexWidget
                parent.appendRow(proj_child)
                qindex_button = parent.child(0, 0).index()
                self.setIndexWidget(qindex_button, self.start_button)
                self.expand(parent.index())

    def _update_next_action(self, parent: QtGui.QStandardItem, actions: list):
        """
        Take the provided actions and populate the 'Next Action' Tree item

        Parameters
        ----------
        parent
            The parent item we are adding to
        actions
            list of FqprActions sorted by priority, we are only interested in the first (the next one)
        """

        parent.removeRows(1, parent.rowCount() - 1)
        if actions:
            next_action = actions[0]
            action_text = next_action.text
            if next_action.input_files:
                input_files = ['Input Files:'] + ['- ' + f for f in next_action.input_files]
            else:
                input_files = ['Input Files: None']
            data = [action_text] + input_files

            for d in data:
                proj_child = QtGui.QStandardItem(d)
                parent.appendRow(proj_child)
                self.tree_data['Next Action'].append(d)
            self.start_button.setDisabled(False)
            self.expand(parent.index())

    def _update_all_actions(self, parent: QtGui.QStandardItem, actions: list):
        """
        Take the provided actions and populate the 'All Actions' Tree item with the text attribute from each action

        Parameters
        ----------
        parent
            The parent item we are adding to
        actions
            list of FqprActions sorted by priority, we are only interested in the text attribute of each
        """

        parent.removeRows(0, parent.rowCount())
        self.tree_data['All Actions'] = [self.tree_data['All Actions'][0]]
        if actions:
            for act in actions:
                proj_child = QtGui.QStandardItem(act.text)
                if act.input_files:
                    ttip = '{}\n\nPriority:{}\nInput Files:\n-{}'.format(act.text, act.priority, '\n-'.join(act.input_files))
                elif act.priority == 5:  # process multibeam action
                    ttip = '{}\n\nPriority:{}\nRun Orientation:{}\nRun Correct Beam Vectors:{}\n'.format(act.text, act.priority, act.kwargs['run_orientation'], act.kwargs['run_beam_vec'])
                    ttip += 'Run Sound Velocity:{}\nRun Georeference/TPU:{}'.format(act.kwargs['run_svcorr'], act.kwargs['run_georef'])
                    if act.kwargs['run_georef']:
                        if act.kwargs['use_epsg']:
                            ttip += '\nEPSG: {}\nVertical Reference: {}'.format(act.kwargs['epsg'], act.kwargs['vert_ref'])
                        else:
                            ttip += '\nCoordinate System: {}\nVertical Reference: {}'.format(act.kwargs['coord_system'], act.kwargs['vert_ref'])
                else:
                    ttip = '{}\n\nPriority:{}'.format(act.text, act.priority)
                proj_child.setToolTip(ttip)
                parent.appendRow(proj_child)
                self.tree_data['All Actions'].append(act.text)

    def _update_queued_files(self, parent: QtGui.QStandardItem, actions: list):
        """
        Take the provided actions and populate the 'Queued Files' Tree item with the input_files attribute from each action

        Parameters
        ----------
        parent
            The parent item we are adding to
        actions
            list of FqprActions sorted by priority, we are only interested in the input_files attribute of each
        """

        parent.removeRows(0, parent.rowCount())
        self.tree_data['Queued Files'] = [self.tree_data['Queued Files'][0]]
        fils = []
        if actions:
            for act in actions:
                fils += act.input_files
            for f in fils:
                proj_child = QtGui.QStandardItem(f)
                parent.appendRow(proj_child)
                self.tree_data['Queued Files'].append(f)

    def _update_unmatched(self, parent: QtGui.QStandardItem, unmatched: dict):
        """
        Take the provided actions and populate the 'Queued Files' Tree item with the input_files attribute from each action

        Parameters
        ----------
        parent
            The parent item we are adding to
        unmatched
            dict of 'filename: reason not matched' for each unmatched file
        """

        parent.removeRows(0, parent.rowCount())
        self.tree_data['Unmatched Files'] = [self.tree_data['Unmatched Files'][0]]
        if unmatched:
            for unmatched_file, reason in unmatched.items():
                proj_child = QtGui.QStandardItem(unmatched_file)
                proj_child.setToolTip(reason)
                parent.appendRow(proj_child)
                self.tree_data['Unmatched Files'].append(unmatched_file)

    def update_actions(self, actions: list = None, unmatched: dict = None):
        """
        Method driven by kluster_intelligence, can be used to either update actions, unmatched, or both.

        Parameters
        ----------
        actions
            optional, list of FqprActions sorted by priority
        unmatched
            optional, dict of 'filename: reason not matched' for each unmatched file
        """

        # check against None here, as there are three possible states for actions/unmatched, ex:
        #  - actions is None -> do not update actions
        #  - actions is an empty list -> update actions with empty (clear actions)
        #  - actions is a populated list -> update actions with new actions

        if actions is not None:
            self.actions = actions
        if unmatched is not None:
            self.unmatched = unmatched

        for cnt, c in enumerate(self.categories):
            parent = self.tree_data[c][0]
            if c == 'Next Action' and actions is not None:
                self._update_next_action(parent, actions)
            elif c == 'All Actions' and actions is not None:
                self._update_all_actions(parent, actions)
            elif c == 'Queued Files' and actions is not None:
                self._update_queued_files(parent, actions)
            elif c == 'Unmatched Files' and unmatched is not None:
                self._update_unmatched(parent, unmatched)

    def start_process(self):
        """
        Emit the execute_action signal to trigger processing in kluster_main
        """
        self.start_button.setDisabled(True)
        self.execute_action.emit(0)


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterProjectTree for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.resize(450, 800)
        self.setWindowTitle('Actions')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.project = FqprProject()

        self.k_actions = KlusterActions(self)
        self.k_actions.setObjectName('kluster_actions')
        self.k_actions.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        self.k_actions.setMinimumWidth(300)
        layout.addWidget(self.k_actions)

        # self.k_actions.file_added.connect(self.update_ktree)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())