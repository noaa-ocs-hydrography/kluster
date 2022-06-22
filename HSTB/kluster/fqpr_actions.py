import os, sys
from dask.distributed import Client
from HSTB.kluster import fqpr_convenience, fqpr_generation


class FqprAction:
    """
    Action class holds the function you are going to run and the arguments, as well as some helpful attributes to describe
    the action.  fqpr_intelligence builds the actions depending on the available files.  Functions within the action
    should always be one of the convenience functions, just to keep everything straight.

    fqpr = fully qualified ping record, the term for the datastore in kluster, see fqpr_generation

    EX: fqpr_intelligence holds multibeam files that do not exist in the given fqpr instances in the project.  fqpr_intelligence
    builds a new FqprAction that has function=fqpr_convenience.convert_multibeam, with the multibeam files as arguments.  The action is executed
    by fqpr_intelligence.FqprIntel and the files are converted.
    """
    def __init__(self, priority=None, action_type=None, text=None, tooltip_text=None, input_files=None, output_destination=None,
                 function=None, args=None, kwargs=None):
        self.priority = priority  # int, lower numbers executed first
        self.action_type = action_type  # str, something like 'multibeam' or 'navigation'
        self.text = text  # str, text description of the action
        self.tooltip_text = tooltip_text  # str, summary of the action for the tooltip
        self.input_files = input_files  # list, list of str paths to the input files, empty for actions like 'process multibeam' that has no files associated with it
        self.output_destination = output_destination  # str, folder path to the fqpr instance that this action is attached to

        self.function = function  # function to run
        self.args = args  # function args
        self.kwargs = kwargs  # function kwargs

        self.output = None  # return of the function
        self.is_running = False  # True if action is running

    def __str__(self):
        return '{}'.format(self.text)

    def __repr__(self):
        return 'FqprAction (Priority {}, {}): {}'.format(self.priority, self.action_type, self.text)

    def set(self, key, value):
        self.__setattr__(key, value)

    def execute(self):
        """
        Run the action.  See gui.kluster_worker.ActionWorker if you are running in gui, will use the worker to run
        in a thread.  Otherwise executed directly by FqprIntel.execute_action (which uses the actioncontainer execute method)
        """
        self.is_running = True
        if self.kwargs:
            self.output = self.function(*self.args, **self.kwargs)
        else:
            self.output = self.function(*self.args)
        self.is_running = False

    def print_summary(self):
        print(self.text)


class FqprActionContainer:
    """
    ActionContainer attached to the fqpr_intelligence.FqprIntel object.  Holds the FqprActions and manages the adding/
    removing/updating/executing of actions.

    fqpr = fully qualified ping record, the term for the datastore in kluster, see fqpr_generation
    """

    def __init__(self, parent=None):
        self.parent = parent  # fqpr_intelligence.FqprIntel
        self.actions = []  # list of FqprActions
        self.unmatched = {}  # dict of unmatched files: reason unmatched
        self._observers = []  # list of functions bound to the class that are executed when actions list changes

    def __repr__(self):
        unique_types = list(set([x.action_type for x in self.actions]))
        return 'FqprActionContainer: {} actions of types: {}'.format(len(self.actions), unique_types)

    def _update_actions(self):
        """
        Sort the actions list and update all observers
        """
        self.actions = sorted(self.actions, key=lambda i: i.priority)
        for callback in self._observers:
            callback(self.actions, None, self.parent.autoprocessing_mode)

    def update_unmatched(self, new_unmatched: dict):
        """
        Update the unmatched files dict, used to provide a tooltip in the action gui
        """
        self.unmatched = new_unmatched
        for callback in self._observers:
            callback(None, self.unmatched, self.parent.autoprocessing_mode)

    def update_actions_client(self, client: Client):
        """
        On executing an action, we trigger this to update the action with the new client that we might have
        generated AFTER building the action originally.

        Parameters
        ----------
        client
            dask distributed client instance
        """

        if client:
            for action in self.actions:
                for cnt, ar in enumerate(action.args):
                    if isinstance(ar, Client):
                        action.args[cnt] = client
                    elif isinstance(ar, fqpr_generation.Fqpr):
                        ar.client = client
                        ar.multibeam.client = client
                        action.args[cnt] = ar
                if action.kwargs:
                    if 'client' in action.kwargs:
                        action.kwargs['client'] = client
                    if 'fqpr_inst' in action.kwargs:
                        action.kwargs['fqpr_inst'].client = client
                        action.kwargs['fqpr_inst'].multibeam.client = client

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

    def clear(self):
        """
        Clear all actions and trigger any observers using the _update_actions method
        """
        self.actions = []
        self._update_actions()
        self.update_unmatched({})

    def update_action(self, action: FqprAction, **kwargs):
        """
        Update an existing action with new arguments.  Example would be if you have a conversion action and the
        quantity of multibeam files changes.  Action would be updated with the new list of files to convert.

        Parameters
        ----------
        action
            FqprAction to update
        kwargs
            dict of keyword arguments to update
        """
        if action in self.actions:
            action = self.actions[self.actions.index(action)]
            for key, value in kwargs.items():
                if value is not None:
                    try:
                        action.set(key, value)
                    except:
                        print('Unable to set action {} to {}'.format(key, value))
            self._update_actions()

    def update_action_from_list(self, action_type: str, action_destination_list: list):
        """
        Remove any actions that have destinations not in the action_destination_list.  This method allows us to remove
        actions that no longer apply.

        Parameters
        ----------
        action_type
            type of action, ex: 'multibeam'
        action_destination_list
            list of valid destinations to filter the actions by

        Returns
        -------
        list
            list of valid actions that match the provided action type (after removing actions)
        list
            list of destinations from all valid actions
        """

        existing_actions = self.return_actions_by_type(action_type)

        removed_actions = []
        for action in existing_actions:
            if action.output_destination not in action_destination_list:
                removed_actions.append(action)
        for removed_action in removed_actions:
            self.remove_action(removed_action)

        existing_actions = self.return_actions_by_type(action_type)
        existing_action_destinations = [a.output_destination for a in existing_actions]

        return existing_actions, existing_action_destinations

    def clear_actions_by_type(self, action_type: str):
        """
        Remove all actions from the actions buffer that are of the provided type

        Parameters
        ----------
        action_type
            one of 'multibeam', 'svp', 'navigation', 'processing', 'gridding'
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
            one of 'multibeam', 'svp', 'navigation', 'processing', 'gridding'

        Returns
        -------
        list
            list of actions that are of the provided type
        """

        actions = []
        if self.actions:
            actions = [a for a in self.actions if a.action_type == action_type]
        return actions

    def get_next_action(self):
        """
        Actions are sorted on update, so the first action in the list is always the highest priority one

        Returns
        -------
        FqprAction
            highest priority action
        """
        if self.actions:
            return self.actions[0]

    def execute_action(self, idx: int = 0):
        """
        Run the action selected from the action list using the provided index.  Removes the action after execution.
        Return is generally the affected fqpr_instance.

        Parameters
        ----------
        idx
            index of the action in the actions list

        Returns
        -------

        """
        action = self.actions.pop(idx)
        self._update_actions()
        if action:
            action.execute()
            output = action.output
            return output
        else:
            print('FqprActionContainer: no actions found.')


def build_multibeam_action(destination: str, line_list: list, client: Client = None, settings: dict = None):
    """
    Construct a convert multibeam action using the provided data

    Parameters
    ----------
    destination
        path to the output folder that the converted data will be saved to
    line_list
        list of multibeam file paths
    client
        optional, dask distributed client if using dask
    settings
        optional settings dictionary used to override kwargs (default processing options)

    Returns
    -------
    FqprAction
        newly generated multibeam conversion action
    """

    args = [line_list, None, destination, client, False, True]
    if settings:
        allowed_kwargs = ['parallel_write', 'vdatum_directory']
        existing_kwargs = list(settings.keys())
        [settings.pop(ky) for ky in existing_kwargs if ky not in allowed_kwargs]
    action = FqprAction(priority=1, action_type='multibeam', output_destination=destination, input_files=line_list,
                        text='Convert {} multibeam lines to {}'.format(len(line_list), destination),
                        tooltip_text='\n'.join(line_list), function=fqpr_convenience.convert_multibeam, args=args,
                        kwargs=settings)
    return action


def update_kwargs_for_multibeam(destination: str, line_list: list, client: Client = None, settings: dict = None):
    """
    Build a dictionary of updated settings for an existing multibeam action, use this with FqprActionContainer to
    update the action.

    Parameters
    ----------
    destination
        path to the output folder that the converted data will be saved to
    line_list
        list of multibeam file paths
    client
        optional, dask distributed client if using dask
    settings
        optional settings dictionary used to override kwargs (default processing options)

    Returns
    -------
    dict
        updated args and kwargs for the multibeam action
    """

    args = [line_list, None, destination, client, False, True]
    if settings:
        allowed_kwargs = ['parallel_write', 'vdatum_directory']
        existing_kwargs = list(settings.keys())
        [settings.pop(ky) for ky in existing_kwargs if ky not in allowed_kwargs]
    update_settings = {'input_files': line_list, 'text': 'Convert {} multibeam lines to {}'.format(len(line_list), destination),
                       'tooltip_text': '\n'.join(line_list), 'args': args, 'kwargs': settings}
    return update_settings


def build_nav_action(destination: str, fqpr_instance: fqpr_generation.Fqpr, navfiles: list, error_files: list, log_files: list):
    """
    Generate a new import navigation (from POSPac SBET) action

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    fqpr_instance
        Fqpr object that we are importing the nav into
    navfiles
        list of post processed navigation files (.sbet)
    error_files
        list of post processed navigation error files (.smrmsg)
    log_files
        list of post processed navigation export files (.log)

    Returns
    -------
    FqprAction
        newly generated import processed navigation action
    """

    args = [fqpr_instance, navfiles]
    kwargs = {'errorfiles': error_files, 'logfiles': log_files}
    action = FqprAction(priority=2, action_type='navigation', output_destination=destination,
                        input_files=navfiles + error_files + log_files, text='Import navigation to {}'.format(destination),
                        tooltip_text='\n'.join(navfiles), function=fqpr_convenience.import_processed_navigation,
                        args=args, kwargs=kwargs)
    return action


def update_kwargs_for_navigation(destination: str, fqpr_instance: fqpr_generation.Fqpr, navfiles: list, error_files: list,
                                 log_files: list):
    """
    Build a dictionary of updated settings for an existing import navigation action, use this with FqprActionContainer to
    update the action.

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    fqpr_instance
        Fqpr object that we are importing the nav into
    navfiles
        list of post processed navigation files (.sbet)
    error_files
        list of post processed navigation error files (.smrmsg)
    log_files
        list of post processed navigation export files (.log)

    Returns
    -------
    dict
        updated args and kwargs for the navigation action
    """

    args = [fqpr_instance, navfiles]
    kwargs = {'errorfiles': error_files, 'logfiles': log_files}
    update_settings = {'input_files': navfiles + error_files + log_files, 'text': 'Import navigation to {}'.format(destination),
                       'tooltip_text': '\n'.join(navfiles), 'args': args, 'kwargs': kwargs}
    return update_settings


def build_svp_action(destination: str, fqpr_instance: fqpr_generation.Fqpr, svfiles: list):
    """
    Generate a new sound velocity import action, supports caris svp files

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    fqpr_instance
        Fqpr object that we are importing the nav into
    svfiles
        list of sound velocity profile files (.svp)

    Returns
    -------
    FqprAction
        newly generated import sound velocity action
    """

    args = [fqpr_instance, svfiles]
    action = FqprAction(priority=3, action_type='svp', output_destination=destination,
                        input_files=svfiles, text='Import sound velocity to {}'.format(destination),
                        tooltip_text='\n'.join(svfiles), function=fqpr_convenience.import_sound_velocity,
                        args=args)
    return action


def update_kwargs_for_svp(destination: str, fqpr_instance: fqpr_generation.Fqpr, svfiles: list):
    """
    Build a dictionary of updated settings for an existing import sound velocity action, use this with FqprActionContainer to
    update the action.

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    fqpr_instance
        Fqpr object that we are importing the nav into
    svfiles
        list of sound velocity profile files (.svp)

    Returns
    -------
    dict
        updated args for the svp action
    """

    args = [fqpr_instance, svfiles]
    update_settings = {'input_files': svfiles, 'text': 'Import sound velocity to {}'.format(destination),
                       'tooltip_text': '\n'.join(svfiles), 'args': args}
    return update_settings


def build_processing_action(destination: str, args: list, kwargs: dict, settings: dict = None, force_epsg: bool = False):
    """
    Generate a new processing action, using the return from fqpr_generation.Fqpr.return_next_action.  This method will
    provide the correct sequence of processing steps that this Fqpr instance needs.

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    args
        args for the convert_multibeam function
    kwargs
        keyword args for the convert_multibeam function
    settings
        optional settings dictionary used to override kwargs (default processing options)
    force_epsg
        if True, forces EPSG instead of auto UTM, used with force_coordinate_match when you want to have all the days
        coordinate systems in a project match the first day

    Returns
    -------
    FqprAction
        newly generated multibeam processing action
    """

    # update the default processing kwargs for settings
    if settings:
        for ky, val in settings.items():
            if ky in ['use_epsg', 'use_coord', 'epsg', 'coord_system'] and force_epsg:  # rely on the existing chosen parameters
                continue
            kwargs[ky] = val

    if 'only_this_line' in kwargs and kwargs['only_this_line']:
        source = '{} ({})'.format(os.path.split(destination)[1], kwargs['only_this_line'])
    else:
        source = '{}'.format(os.path.split(destination)[1])

    if kwargs['run_orientation'] and kwargs['run_beam_vec'] and kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Run all processing on {}'.format(source)
    elif kwargs['run_beam_vec'] and kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with beam correction'.format(source)
    elif kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with sound velocity'.format(source)
    elif kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with georeferencing'.format(source)
    elif kwargs['run_tpu']:
        text = 'Process {} only computing TPU'.format(source)
    else:
        text = 'Process {} with custom setup'.format(source)

    action = FqprAction(priority=5, action_type='processing', output_destination=destination,
                        input_files=[], text=text,
                        tooltip_text='{}'.format(destination), function=fqpr_convenience.process_multibeam,
                        args=args, kwargs=kwargs)
    return action


def update_kwargs_for_processing(destination: str, args: list, kwargs: dict, settings: dict = None, force_epsg: bool = False):
    """
    Build a dictionary of updated settings for an existing processing action, use this with FqprActionContainer to
    update the action.

    Parameters
    ----------
    destination
        file path to the converted Fqpr instance
    args
        args for the convert_multibeam function
    kwargs
        keyword args for the convert_multibeam function
    settings
        optional settings dictionary used to override kwargs (default processing options)
    force_epsg
        if True, forces EPSG instead of auto UTM, used with force_coordinate_match when you want to have all the days
        coordinate systems in a project match the first day

    Returns
    -------
    dict
        updated args for the processing action
    """

    # update the default processing kwargs for settings
    if settings:
        for ky, val in settings.items():
            if ky in ['use_epsg', 'use_coord', 'epsg', 'coord_system'] and force_epsg:  # rely on the existing chosen parameters
                continue
            kwargs[ky] = val

    if 'only_this_line' in kwargs and kwargs['only_this_line']:
        source = '{} ({})'.format(os.path.split(destination)[1], kwargs['only_this_line'])
    else:
        source = '{}'.format(os.path.split(destination)[1])

    if kwargs['run_orientation'] and kwargs['run_beam_vec'] and kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Run all processing on {}'.format(source)
    elif kwargs['run_beam_vec'] and kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with beam correction'.format(source)
    elif kwargs['run_svcorr'] and kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with sound velocity'.format(source)
    elif kwargs['run_georef'] and kwargs['run_tpu']:
        text = 'Process {} starting with georeferencing'.format(source)
    elif kwargs['run_tpu']:
        text = 'Process {} only computing TPU'.format(source)
    else:
        text = 'Process {} with custom setup'.format(source)

    update_settings = {'text': text, 'args': args, 'kwargs': kwargs}
    return update_settings


def build_surface_action(destination: str, surface_instance: fqpr_convenience.BathyGrid, add_fqpr: list = None,
                         add_lines: list = None, remove_fqpr: list = None, remove_lines: list = None):
    """
    Generate a new update surface action, adding multibeam files (or removing multibeam files) and regridding

    Parameters
    ----------
    destination
        folder path to the bathygrid surface folder
    surface_instance
        a loaded Bathygrid instance
    add_fqpr
        Optional, a list of Fqpr instances to add to the surface
    add_lines
        Optional, if provided will only add lines that are in this list(s).  a list of lists of lines (when a list of
        fqpr instances is provided)
    remove_fqpr
        Optional, a list of Fqpr instances to remove from the surface
    remove_lines
        Optional, if provided will only add lines that are in this list(s)  a list of lists of lines (when a list of
        fqpr instances is provided)

    Returns
    -------
    FqprAction
        newly generated update surface action
    """

    args = [surface_instance]
    kwargs = {'add_fqpr': add_fqpr, 'add_lines': add_lines, 'remove_fqpr': remove_fqpr, 'remove_lines': remove_lines}
    action = FqprAction(priority=10, action_type='gridding', output_destination=destination,
                        input_files=[], text='Update surface {}'.format(destination),
                        tooltip_text='{}'.format(destination), function=fqpr_convenience.update_surface,
                        args=args, kwargs=kwargs)
    return action


def update_kwargs_for_surface(destination: str, surface_instance: fqpr_convenience.BathyGrid, add_fqpr: list = None,
                              add_lines: list = None, remove_fqpr: list = None, remove_lines: list = None):
    """
    Build a dictionary of updated settings for an existing update surface action, use this with FqprActionContainer to
    update the action.

    Parameters
    ----------
    destination
        folder path to the bathygrid surface folder
    surface_instance
        a loaded Bathygrid instance
    add_fqpr
        Optional, a list of Fqpr instances to add to the surface
    add_lines
        Optional, if provided will only add lines that are in this list(s).  a list of lists of lines (when a list of
        fqpr instances is provided)
    remove_fqpr
        Optional, a list of Fqpr instances to remove from the surface
    remove_lines
        Optional, if provided will only add lines that are in this list(s)  a list of lists of lines (when a list of
        fqpr instances is provided)

    Returns
    -------
    dict
        updated args for the surface action
    """

    args = [surface_instance]
    kwargs = {'add_fqpr': add_fqpr, 'add_lines': add_lines, 'remove_fqpr': remove_fqpr, 'remove_lines': remove_lines}
    ttext = '{}'.format(destination)
    update_settings = {'text': 'Update surface {}'.format(destination),
                       'tooltip_text': ttext, 'args': args, 'kwargs': kwargs}
    return update_settings
