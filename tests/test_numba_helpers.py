import numpy as np
import unittest

from HSTB.kluster.numba_helpers import bin2d, bin1d, hist2d_numba_seq


class TestNumbaHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.x = np.arange(10)
        cls.y = np.arange(10)
        cls.xbins = np.array([0, 3, 6, 10])
        cls.ybins = np.array([0, 5, 10])

    def test_bin2d(self):
        x_index, y_index = bin2d(self.x, self.y, self.xbins, self.ybins)
        assert np.array_equal(x_index, np.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 3]))
        assert np.array_equal(y_index, np.array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2]))

    def test_bin1d(self):
        x_index = bin1d(self.x, self.xbins)
        assert np.array_equal(x_index, np.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 3]))

    def test_hist2d_numba_seq(self):
        hist2d = hist2d_numba_seq(self.x, self.y, np.array([2, 2]), np.array([[0, 10], [0, 10]]))
        assert np.array_equal(hist2d, np.array([[5., 0.], [0., 5.]]))
