from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileCreatedEvent, FileDeletedEvent
from threading import Thread, Event
from types import FunctionType
import numpy as np
import os


supported_mbes = ['.all', '.kmall']
supported_sbet = ['.out', '.sbet', '.smrmsg']  # people keep mixing up these extensions, so just check for the nav/smrmsg in both
supported_export_log = ['.txt', '.log']
supported_svp = ['.svp']
all_extensions = list(np.concatenate([supported_mbes, supported_sbet, supported_export_log, supported_svp]))


class DirectoryMonitor:
    """
    Use watchdog to monitor the provided directory and kick off events based on the type of file system event

    Watchdog will issue a FileCreatedEvent as soon as the file appears in a directory, NOT when it is finished copying
    and accessible.  We have to wait until the file is finished copying and is readable.  We include an event driven
    system (self.watch_buffer_timer) that will check the files to see if they are readable before pushing them on to
    the intelligence module.  The intelligence module will immediately read the file for data, so if the file hasn't
    finished writing, you get a permissions error.

    This file will push the new file using the _newfile setter to any class that has a method bound to this class,
    see self.bind_to

    dm = DirectoryMonitor(r'C:\data_dir\tj_patch_test', is_recursive=True)
    dm.start()
    """

    def __init__(self, directory_path: str, is_recursive: bool = True):
        """

        Parameters
        ----------
        directory_path
            absolute file path to the folder we want to monitor
        is_recursive
            If True, will use watchdog to search all subdirectories in directory_path
        """

        self.directory_path = directory_path
        self.is_recursive = is_recursive
        self.patterns = ['*' + ext for ext in all_extensions]
        self.my_event_handler = IntelligenceMonitorHandler(self,  # parent class that holds the files list
                                                           self.patterns,  # find all files matching these patterns
                                                           "",  # no ignore patterns to use here
                                                           False,  # do not ignore directories
                                                           False)  # do not make this case sensitive
        self.watchdog_observer = Observer()
        self.watchdog_observer.schedule(self.my_event_handler, directory_path, recursive=is_recursive)
        self._observers = []
        self.seen_files = []
        self.file_buffer = {}

        # apply WatchBuffer to only push to intelligence once the file has finished copying
        self.watch_buffer_event = Event()
        self.watch_buffer_timer = WatchBuffer(self.watch_buffer_event, self.push_to_kluster_intelligence, runtime=1)
        self.watch_buffer_timer.start()
        self._newfile = ''
        self.file_event = 'created'

    def build_initial_file_state(self):
        """
        Kicking off the watchdog observer will track all file system changes from this point forward, but it will
        not get you the current state.  Apply this method in addition to starting the observer to get the initial state.
        """

        self.file_event = 'created'
        if self.is_recursive:
            for root, direc, files in os.walk(self.directory_path):
                for fil in files:
                    filext = os.path.splitext(fil)[1]
                    if filext in all_extensions:
                        self.add_to_buffer(os.path.join(root, fil), self.file_event)
        else:
            for fil in os.listdir(self.directory_path):
                filext = os.path.splitext(fil)[1]
                if filext in all_extensions:
                    self.add_to_buffer(os.path.join(self.directory_path, fil), self.file_event)

    def add_to_buffer(self, filepath: str, file_event: str):
        """
        self.my_event_handler will use this method to update the file buffer.  We push to a buffer before pushing to
        the intelligence module, as we only want the intelligence buffer to see the file once it has finished copying,
        i.e. is readable.

        Parameters
        ----------
        filepath
            absolute file path to the file being added to the buffer
        file_event
            one of 'created', 'deleted'
        """

        filepath = os.path.normpath(filepath)
        if filepath in self.seen_files and file_event == 'deleted':
            self.seen_files.remove(filepath)
            self.file_event = 'deleted'
            self.newfile = filepath
        elif filepath not in self.seen_files:
            self.seen_files.append(filepath)
            self.file_buffer[filepath] = file_event

    def push_to_kluster_intelligence(self):
        """
        Method triggered on timer event.  Every second we check to see if a file is readable (has finished copying).
        If so, we use newfile to trigger any observers which then get the newly written (or deleted) file
        """

        for fil in list(self.file_buffer.keys()):
            try:
                tst = open(fil)
                tst.close()
                self.file_event = self.file_buffer[fil]
                self.newfile = fil
                self.file_buffer.pop(fil)
            except:
                pass

    def start(self):
        """
        Start the watchdog_observer
        """

        self.build_initial_file_state()
        self.watchdog_observer.start()

    def stop(self):
        """
        Stop the watchdog_observer
        """
        if self.watchdog_observer.is_alive():
            self.watchdog_observer.stop()
            self.watchdog_observer.join()

    @property
    def newfile(self):
        """
        newfile property
        """

        return self._newfile

    @newfile.setter
    def newfile(self, newfile: str):
        """
        newfile setter, triggers any observers

        Parameters
        ----------
        newfile
            absolute file path to the new file that has been created/deleted, see file_event to determine which
        """
        self._newfile = newfile
        for callback in self._observers:
            callback(self._newfile, self.file_event)

    def bind_to(self, callback: FunctionType):
        """
        Pass in a method as callback, method will be triggered on setting newfile

        Parameters
        ----------
        callback
            method that is run on setting newfile
        """

        self._observers.append(callback)


class IntelligenceMonitorHandler(PatternMatchingEventHandler):
    """
    Event handler for the fqpr_intelligence directory monitoring.
    """
    def __init__(self, parent: DirectoryMonitor, patterns: list, ignore_patterns: list, ignore_directories: bool,
                 case_sensitive: bool):
        """
        initialize the handler

        Parameters
        ----------
        parent
            Monitor class that holds the watchdog Observer
        patterns
            list of extension patterns you want to include in the search, ex: ['*.all', '*.kmall']
        ignore_patterns
            optional, list of extension patterns you want to exclude in the search, ex: ['*.all', '*.kmall']
        ignore_directories
            if True, will ignore directories in the search
        case_sensitive
            if True, will make the pattern search case sensitive
        """

        super().__init__(patterns, ignore_patterns, ignore_directories, case_sensitive)
        self.parent = parent

    def on_created(self, event: FileCreatedEvent):
        """
        When a file is created, we add it to the buffer.  Will be cleared from the buffer and added for real once the
        file has finished writing.

        Parameters
        ----------
        event
            watchdog filecreatedevent that gets generated on creating a file in the monitored directory
        """

        newfil = event.src_path
        self.parent.add_to_buffer(newfil, 'created')

    def on_deleted(self, event: FileDeletedEvent):
        """
        When a file is deleted (or cut and pasted) from a monitored directory, add it to the buffer with the 'deleted'
        event tag

        Parameters
        ----------
        event
            watchdog event created when a file is deleted in the monitored directory
        """

        newfil = event.src_path
        self.parent.add_to_buffer(newfil, 'deleted')


class WatchBuffer(Thread):
    """
    Employ this thread to run the provided function every (runtime) seconds.  Use the provided event to stop the thread.

    See DirectoryMonitor for usage.  We use this thread in that class to monitor the file buffer, to see when incoming
    files have finished being written (and are then ready to be read)
    """
    def __init__(self, event: Event, timed_func: FunctionType, runtime=1):
        """
        initialize

        Parameters
        ----------
        event
            event used to stop the thread
        timed_func
            function to run on timeout
        runtime
            timeout interval
        """
        Thread.__init__(self)
        self.stopped = event
        self.timed_func = timed_func
        self.runtime = runtime

    def run(self):
        while not self.stopped.wait(self.runtime):
            self.timed_func()