import os, sys


class FqprAction:
    def __init__(self, priority=None, action_type=None, text=None, tooltip_text=None, input_files=None, output_destination=None,
                 function=None, arguments=None):
        self.priority = priority
        self.action_type = action_type
        self.text = text
        self.tooltip_text = tooltip_text
        self.input_files = input_files
        self.output_destination = output_destination

        self.function = function
        self.arguments = arguments

        self.output = None
        self.is_running = False

    def set(self, key, value):
        self.__setattr__(key, value)

    def execute(self):
        self.is_running = True
        self.output = self.function(*self.arguments)
        self.is_running = False

    def print_summary(self):
        print(self.text)


class FqprActionContainer:
    def __init__(self, parent=None):
        self.parent = parent
        self.actions = []
        self._observers = []

    def _update_actions(self):
        """
        Sort the actions list and update all observers
        """
        self.actions = sorted(self.actions, key=lambda i: i.priority)
        for callback in self._observers:
            callback(self.actions)

    def add_action(self, action: FqprAction):
        """
        Add a new action, autosort by priority whenever we do

        Parameters
        ----------
        action
            new FqprAction instance to add
        """
        self.actions.append(action)
        self._update_actions()

    def remove_action(self, action: FqprAction):
        """
        Remove an action, autosort by priority whenever we do

        Parameters
        ----------
        action
            FqprAction instance to remove
        """
        self.actions.remove(action)
        self._update_actions()

    def update_action(self, action: FqprAction, **kwargs):
        if action in self.actions:
            action = self.actions[self.actions.index(action)]
            for key, value in kwargs.items():
                if value is not None:
                    try:
                        action.set(key, value)
                    except:
                        print('Unable to set action {} to {}'.format(key, value))
            self._update_actions()

    def clear_actions_by_type(self, action_type: str):
        """
        Remove all actions from the actions buffer that are of the provided type

        Parameters
        ----------
        action_type
            one of 'multibeam', 'svp', 'navigation'
        """

        if self.actions:
            self.actions = [a for a in self.actions if a.action_type != action_type]
            self._update_actions()

    def return_actions_by_type(self, action_type: str):
        """
        Return all actions from the actions buffer that are of the provided type

        Parameters
        ----------
        action_type
            one of 'multibeam', 'svp', 'navigation'

        Returns
        -------
        list
            list of actions that are of the provided type
        """

        actions = []
        if self.actions:
            actions = [a for a in self.actions if a.action_type == action_type]
        return actions

