from HSTB.kluster.fqpr_helpers import *


def get_testfile_paths():
    testfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', '0009_20170523_181119_FA2806.all')
    return testfile


def test_epsg_determinator():
    assert epsg_determinator('nad83(2011)', 12, 'N') == 6341
    assert epsg_determinator('NAD83(2011)', 5, 'N') == 6334
    try:
        epsg_determinator('nad83(2011)', 12, 'S')
    except ValueError:  # this is the expected result since this is out of bounds for nad83
        assert True

    assert epsg_determinator('wgs84', 12, 'N') == 32612
    assert epsg_determinator('wgs84', 12, 'S') == 32712
    assert epsg_determinator('wgs84', 25, 'N') == 32625
    assert epsg_determinator('wgs84', 25, 'S') == 32725


def test_return_files_from_path():
    fil = get_testfile_paths()
    assert return_files_from_path(fil) == [fil]