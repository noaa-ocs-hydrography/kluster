import os
import psutil
import numpy as np
import dask
from dask.distributed import get_client, Client, Lock
from xarray import DataArray
from fasteners import InterProcessLock


# we manually set the worker space (where spillover data goes during operations) here because I found some
#  users were starting the python console in the Windows folder, or some other write protected area.  Default worker
#  space is the current working directory.
worker_temp_space = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'dask-worker-space')
if not os.path.exists(worker_temp_space):
    os.mkdir(worker_temp_space)
dask.config.set(temporary_directory=worker_temp_space)


class DaskProcessSynchronizer:
    """Provides synchronization using file locks via the
    `fasteners <http://fasteners.readthedocs.io/en/latest/api/process_lock.html>`_
    package.

    Parameters
    ----------
    path
        Path to a directory on a file system that is shared by all processes.
    """

    def __init__(self, path: str):
        self.path = path

    def __getitem__(self, item):
        path = os.path.join(self.path, item)
        try:  # this will work if dask.distributed workers exist
            lock = Lock(path)
        except AttributeError:  # otherwise default to the interprocesslock used by zarr
            lock = InterProcessLock(path)
        return lock


def dask_find_or_start_client(address: str = None, silent: bool = False):
    """
    Either start or return Dask client in local/networked cluster mode

    Parameters
    ----------
    address
        ip address for existing or desired new dask server instance
    silent
        whether or not to print messages

    Returns
    -------
    dask.distributed.client.Client
        Client instance representing Local Cluster/Networked Cluster operations
    """

    client = None
    try:
        if address is None:
            client = get_client()
            if not silent:
                print('Using existing local cluster client...')
        else:
            client = get_client(address=address)
            if not silent:
                print('Using existing client on address {}...'.format(address))
    except ValueError:  # no global client found and no address provided
        logical_core_count = psutil.cpu_count(True)
        mem_total_gb = psutil.virtual_memory().total / 1000000000
        # currently trying to support >8 workers is a mem hog.  Limit to 8, maybe expose this in the gui somewhere

        if mem_total_gb > 24:  # basic test to see if we have enough memory, using an approx necessary amount of memory
            num_workers = min(logical_core_count, 8)
        else:  # if you have less, just limit to 4 workers
            num_workers = min(logical_core_count, 4)

        if address is None:
            if not silent:
                print('Starting local cluster client...')
            client = Client(n_workers=num_workers)
        else:
            if not silent:
                print('Starting client on address {}...'.format(address))
            client = Client(address=address, n_workers=num_workers)
    if client is not None:
        print(client)
    return client


def get_max_cluster_allocated_memory(client: Client):
    """
    Retrieve the max memory across all workers in the cluster added together

    Parameters
    ----------
    client
        dask client needed to get number of workers and memory limit

    Returns
    -------
    float
        sum of max memory across all workers
    """

    worker_ids = list(client.scheduler_info()['workers'].keys())
    mem_per_worker = [client.scheduler_info()['workers'][wrk]['memory_limit'] for wrk in worker_ids]
    return np.sum(mem_per_worker) / (1024 ** 3)


def get_number_of_workers(client: Client):
    """
    Retrieve the total number of workers from the dask cluster

    Parameters
    ----------
    client
        client used to determine number of workers

    Returns
    -------
    int
        total number of workers
    """

    return len(client.scheduler_info()['workers'])


def determine_optimal_chunks(client: Client, beams_per_ping: float, safety_margin: float = 0.75,
                             chunks_per_worker: int = 2, mem_per_beam: float = 0.000015):
    """
    A very rudimentary placeholder-esque way to determine the chunk size and number of chunks for an array to process
    in memory.  Too many chunks/Too big chunks and you run out of memory.  Too few and you aren't utilizing the resources
    adequately.  Here we scale mainly based off of the number of workers and the amount of memory available in the cluster.

    Parameters
    ----------
    client
        dask distributed client
    beams_per_ping
        avg number of beams per ping
    safety_margin
        made up number to ensure we don't expect 100% of the memory to be available
    chunks_per_worker
        determines the number of chunks to build and process
    mem_per_beam
        metric I came up with looking at processes run, approx amount of memory used per beam in the svcorr process.

    Returns
    -------
    int
        length in time dimension of each chunk
    int
        total number of chunks to process
    """

    nworker = get_number_of_workers(client)
    memsize = get_max_cluster_allocated_memory(client)  # in GB

    mem_per_worker = (memsize / nworker) * safety_margin
    beams_per_chunk = (mem_per_worker / mem_per_beam) / chunks_per_worker
    pings_per_chunk = beams_per_chunk / beams_per_ping
    tot_chunks = nworker * chunks_per_worker

    return int(pings_per_chunk), int(tot_chunks)


def split_array_by_number_of_workers(client: Client, dataarray: DataArray, max_len: int = None):
    """
    In order to operate on an array in a parallelized way, we need to split the array into equal chunks to pass to each
    worker.  Here we do that by just dividing by the number of workers.

    Optional parameter is to restrict the size of the chunks by an int max_len.  This of course only applies if the
    chunks were going to be larger than max_len anyway.

    Drop empty if the length of the array is greater than the number of workers.

    Parameters
    ----------
    client
        dask distributed client
    dataarray
        one dimensional array
    max_len
        max number of values per chunk, if None, ignored

    Returns
    -------
    list
        list of numpy arrays representing chunks of the original array
    list
        list of numpy arrays representing indexes of new values from original array
    """

    numworkers = get_number_of_workers(client)
    split = None

    if max_len is not None:
        if len(dataarray) > max_len * numworkers:  # here we apply max_len, but only if necessary based on the length of the array
            max_split_count = np.ceil(len(dataarray) / max_len)  # round up to ensure you are below max_len
            split = np.array_split(dataarray, max_split_count)
    if split is None:
        split = np.array_split(dataarray, numworkers)
    data_out = [s for s in split if s.size != 0]

    # search sorted to build the index gets messy with very long arrays and/or lots of splits. Plus we should just know
    #   the index without having to search for it....
    # data_idx = np.searchsorted(dataarray, data_out)
    data_idx = []
    cnt = 0
    for i in data_out:
        datalen = len(i)
        data_idx.append(np.arange(cnt, datalen + cnt, 1))
        cnt += datalen

    return data_out, data_idx
