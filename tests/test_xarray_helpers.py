from HSTB.kluster.xarray_helpers import *


def test_search_not_sorted():
    master = np.array([3, 4, 5, 6, 1, 9, 0, 2, 7, 8])
    search = np.array([6, 4, 3, 1, 1])

    final_inds = search_not_sorted(master, search)

    assert (np.array_equal(master[final_inds], search))
