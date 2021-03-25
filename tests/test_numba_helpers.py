from HSTB.kluster.numba_helpers import *


def test_bin2d():
    x = np.arange(10)
    y = np.arange(10)
    xbins = np.array([0, 3, 6, 10])
    ybins = np.array([0, 5, 10])

    x_index, y_index = bin2d(x, y, xbins, ybins)

    assert np.array_equal(x_index, np.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 3]))
    assert np.array_equal(y_index, np.array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2]))


def test_bin1d():
    x = np.arange(10)
    xbins = np.array([0, 3, 6, 10])
    x_index = bin1d(x, xbins)

    assert np.array_equal(x_index, np.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 3]))


def test_hist2d_numba_seq():
    x = np.arange(10)
    y = np.arange(10)
    bins = np.array([2, 2])
    ranges = np.array([[0, 10], [0, 10]])

    hist2d = hist2d_numba_seq(x, y, bins, ranges)

    assert np.array_equal(hist2d, np.array([[5., 0.], [0., 5.]]))
