import re
import numpy as np


def dms2dd(d: float, m: float, s: float):
    """
    convert between deg-min-sec and decimal degrees

    Parameters
    ----------
    d
        degrees
    m
        minutes
    s
        seconds

    Returns
    -------
    float
        decimal degrees

    """
    sign = 1
    try:
        if float(d) < 0:
            sign = -1
    except TypeError:
        d = float(d)
        m = float(m)
        s = float(s)

    dd = abs(float(d)) + float(m)/60 + float(s)/(60 * 60)
    return dd * sign


def dd2dms(deg: float):
    """
    convert between decimal degrees and deg-min-sec

    Parameters
    ----------
    deg
        decimal degrees

    Returns
    -------
    list
        [degrees as float, minutes as float, seconds as float]

    """
    try:
        d, m = divmod(abs(deg), 1)
    except TypeError:
        deg = float(deg)
        d, m = divmod(abs(deg), 1)
    m, s = divmod(m * 60, 1)
    s = s * 60

    if float(deg) < 0:
        d = d * -1

    return [d, m, s]


def parse_dms_to_dd(dms: str):
    """
    Take in deg-min-sec string in a couple different formats and return the decimal degrees representation.

    Supported formats:
    "80:38:06.57 W"
    "80:38:06.57W"
    "-80:38:06.57"
    "-80:38:06"

    Parameters
    ----------
    dms
        deg-min-sec string

    Returns
    -------
    float
        decimal degrees

    """
    # split by any non-digit, non-letter character except - sign
    parts = re.split(r"[^\w-]+", dms)
    direct = 1
    directions_included = {'N': 1, 'E': 1, 'W': -1, 'S': -1}
    if parts[-1] in directions_included:  # someone included dir with space, ex: "80:38:06.57 W"
        direct = directions_included[parts[-1]]
        parts = parts[:-1]
    elif parts[-1][-1] in directions_included:  # someone included dir without space, ex: "80:38:06.57W"
        direct = directions_included[parts[-1][-1]]
        parts[-1] = parts[-1][:-1].rstrip()

    if parts[0][0] != '-':
        parts[0] = int(parts[0]) * direct  # add negative if direction was included as a letter but not as sign for deg

    dd = ''
    if len(parts) == 4:  # milliseconds given, ex: "-80:38:06.57"
        dec_secs = int(parts[2]) + (int(parts[3]) / (10.0 ** len(parts[3].rstrip())))
        dd = dms2dd(float(parts[0]), float(parts[1]), float(dec_secs))
    elif len(parts) == 3:  # milliseconds not given, ex: "-80:38:06"
        dd = dms2dd(float(parts[0]), float(parts[1]), float(parts[2]))
    return dd


def return_zone_from_min_max_long(minlon: float, maxlon: float, minlat: float):
    """
    Takes min longitude / max longitude and returns the zone that encompasses both.  If min/max are in different zones,
    prints warning message and returns the higher zone number

    Parameters
    ----------
    minlon
        the minimum longitude value of the dataset
    maxlon
        the maximum longitude value of the dataset
    minlat
        the minimum latitude value of the dataset

    Returns
    -------
    str
        zone number with N/S identifier

    """
    maxlon_zone = str(int(np.ceil((maxlon + 180) / 6)))
    minlon_zone = str(int(np.ceil((minlon + 180) / 6)))

    if minlat > 0:
        zone_ident = 'N'
    else:
        zone_ident = 'S'

    if int(maxlon_zone) != int(minlon_zone):
        print('Spanning more than one UTM zone: MIN {}, MAX {}'.format(minlon_zone, maxlon_zone))

    return maxlon_zone + zone_ident
