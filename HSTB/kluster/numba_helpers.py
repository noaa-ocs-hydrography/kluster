import numba
import numpy as np


def bin2d(x: np.array, y: np.array, xbins: np.array, ybins: np.array):
    # this is still slower than sorting and using searchsorted in numpy
    x_idx = bin1d(x, xbins)
    y_idx = bin1d(y, ybins)
    return x_idx, y_idx


@numba.njit(nogil=True, fastmath=True)
def bin1d(x: np.array, xbins: np.array):
    ans = np.zeros_like(x)
    for i in range(len(x)):
        ans[i] = _digitize(x[i], xbins)
    return ans


@numba.njit(nogil=True, fastmath=True)
def _digitize(x: np.array, bins: np.array, right=False):
    # bins are monotonically-increasing
    n = len(bins)
    lo = 0
    hi = n

    if right:
        if np.isnan(x):
            # Find the first nan (i.e. the last from the end of bins,
            # since there shouldn't be many of them in practice)
            for i in range(n, 0, -1):
                if not np.isnan(bins[i - 1]):
                    return i
            return 0
        while hi > lo:
            mid = (lo + hi) >> 1
            if bins[mid] < x:
                # mid is too low => narrow to upper bins
                lo = mid + 1
            else:
                # mid is too high, or is a NaN => narrow to lower bins
                hi = mid
    else:
        if np.isnan(x):
            # NaNs end up in the last bin
            return n
        while hi > lo:
            mid = (lo + hi) >> 1
            if bins[mid] <= x:
                # mid is too low => narrow to upper bins
                lo = mid + 1
            else:
                # mid is too high, or is a NaN => narrow to lower bins
                hi = mid

    return lo


@numba.njit(nogil=True, parallel=False)
def hist2d_numba_seq(x: np.array, y: np.array, bins: np.ndarray, ranges: np.ndarray):
    """
    Custom function to build a histogram2d using numba.  Take provided bins and ranges and return count at each 2d bin
    location.

    x = np.random.uniform(0, 100, size=1000000)
    y = np.random.uniform(0, 100, size=1000000)

    bins = np.array([99, 99])
    ranges = np.array([[0, 100], [0, 100]])

    hist2d_numba_seq(x, y, bins, ranges)

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


if __name__ == '__main__':
    x = np.random.uniform(0, 100, size=1000000)
    x_bins = np.arange(100)
    x_idx = bin1d(x, x_bins)
