import numba
import numpy as np


@numba.njit(nogil=True, parallel=False)
def hist2d_numba_seq(x: np.array, y: np.array, bins: np.ndarray, ranges: np.ndarray):
    """
    Custom function to build a histogram2d using numba.  Take provided bins and ranges and return count at each 2d bin
    location.

    Parameters
    ----------
    x
        numpy array, 1d x value
    y
        numpy array, 1d y value
    bins
        numpy ndarray, 2d bin locations
    ranges
        numpy ndarray, 2d ranges

    Returns
    -------
    np.ndarray
        2d histogram with counts per bin for x and y
    """

    hist = np.zeros((bins[0], bins[1]), dtype=np.float64)
    delta = 1 / ((ranges[:, 1] - ranges[:, 0]) / bins)

    for t in range(x.shape[0]):
        i = (x[t] - ranges[0, 0]) * delta[0]
        j = (y[t] - ranges[1, 0]) * delta[1]
        if 0 <= i < bins[0] and 0 <= j < bins[1]:
            hist[int(i), int(j)] += 1
    return hist


def _hist2d_add(list_results: list):
    """
    Quick helper function that we can submit to dask cluster to sum the results of running hist2d_numba_seq on multiple
    chunks of data.

    Parameters
    ----------
    list_results
        list, list of numpy ndarray histograms that we want to sum to get global result

    Returns
    -------
    np.ndarray
        combined results of histogram across chunks
    """

    return np.sum(list_results, axis=0)
