from datetime import datetime, timezone
import time
import calendar

# Make sure to not add any HSTB module imports so this is standalone and old HSTP code can also use it

# Caris 1980 epoch
_epoch = calendar.timegm(datetime.strptime('1980-001', '%Y-%j').timetuple())


def julian_day_time_to_utctimestamp(j_year, j_day, h, m, s):
    """
    Convert Julian day and hours-min-sec to UTC timestamp.  NOT using Caris 1980 epoch here!

    Parameters
    ----------
    j_year: int, julian year, ex: 2019
    j_day: int, julian day, ex: 123
    h: int, hours
    m: int, seconds
    s: float, seconds

    Returns
    -------
    float, utc timestamp in seconds
    """
    month, day = PyTmYJDtoMD(j_year, j_day)
    return calendar_day_time_to_utctimestamp(j_year, month, day, h, m, s)


def calendar_day_time_to_utctimestamp(c_year, c_mon, c_day, h, m, s):
    """
    Convert calendar year-month-day and hours-min-sec to UTC timestamp.  NOT using Caris 1980 epoch here!

    Parameters
    ----------
    c_year: int, year, ex: 2019
    c_mon: int, month, ex: 5
    c_day: int, day, ex: 12
    h: int, hours
    m: int, seconds
    s: float, seconds

    Returns
    -------
    float, utc timestamp in seconds
    """
    secs, frac_secs = divmod(s, 1)
    microsecs = frac_secs * 1000 * 1000
    dt = datetime(c_year, c_mon, c_day, h, m, int(secs), int(microsecs))
    return dt.replace(tzinfo=timezone.utc).timestamp()


def PyTmStoHMSX(secs):
    """
    Convert seconds to hours-minutes-seconds-milliseconds
    
    Parameters
    ----------
    secs: float, time in seconds

    Returns
    -------
    list, [hours as int, minutes as int, seconds as int, milliseconds as float]
    """

    h, secs = divmod(secs, 3600)
    m, secs = divmod(secs, 60)
    s, x = divmod(secs, 1)
    return [int(h), int(m), int(s), x]


def PyTmYJDtoMD(yr, day):
    """
    Convert julian year-day to month-day

    Parameters
    ----------
    yr: int, year
    day: int, julian day number

    Returns
    -------
    list, [month as int, day as int]
    """

    tt = datetime.strptime('%04d-%03d' % (yr, day), '%Y-%j').timetuple()
    return [tt.tm_mon, tt.tm_mday]


def PyTmYMDtoJD(year, month, day):
    """
    Convert year-month-day to julian day

    Parameters
    ----------
    year: int, year as integer
    month: int, month as integer
    day: int, day as integer

    Returns
    -------
    int, julian day number
    """

    tt = datetime.strptime('%04d-%02d-%02d' % (year, month, day), '%Y-%m-%d').timetuple()
    return tt.tm_yday


def PyTmHMSXtoS(h, m, s, x):
    """
    Convert hours-minutes-seconds-milliseconds to seconds as float
    Parameters
    ----------
    h: int, hours
    m: int, minutes
    s: int, seconds
    x: float, milliseconds

    Returns
    -------
    float, seconds
    """

    return h * 3600.0 + m * 60.0 + s + x


def PyTmYDStoUTCs80(y, d, s):
    """
    Converts Julian year-day-seconds to timestamp using Caris 1980 epoch, includes leap seconds

    PyTmYDStoUTCs80(2012, 183,0)
    1025568016
    PyTmYDStoUTCs80(2009, 1,0)
    915235215
    PyTmYDStoUTCs80(2006, 1,0)
    820540814
    PyTmYDStoUTCs80(1999, 1,0)
    599616013
    PyTmYDStoUTCs80(1997, 182,0)
    552182412
    PyTmYDStoUTCs80(1996, 1,0)
    504921611
    PyTmYDStoUTCs80(1994, 182,0)
    457488010
    PyTmYDStoUTCs80(1993, 182,0)
    425952009
    PyTmYDStoUTCs80(1992, 182,0)
    394329608
    PyTmYDStoUTCs80(1991, 1,0)
    347155207
    PyTmYDStoUTCs80(1990, 1,0)
    315619206
    PyTmYDStoUTCs80(1988, 1,0)
    252460805
    PyTmYDStoUTCs80(1985, 182,0)
    173491204
    PyTmYDStoUTCs80(1983, 182,0)
    110332803
    PyTmYDStoUTCs80(1982, 182,0)
    78796802
    PyTmYDStoUTCs80(1981, 182,0)
    47260801

    Parameters
    ----------
    y: int, year
    d: int, day
    s: int, seconds

    Returns
    -------
    timestamp: int, seconds since 1980 epoch
    """
    h, m, s, x = PyTmStoHMSX(s)
    dt = datetime.strptime('%04d-%03dT%02d:%02d:%02d' % (y, d, h, m, s), '%Y-%jT%H:%M:%S')
    tt = dt.timetuple()

    if y > 2012 or (y == 2012 and d >= 183):
        leaps = 16  # leap year makes it day number 183
    elif y >= 2009:
        leaps = 15
    elif y >= 2006:
        leaps = 14
    elif y >= 1999:
        leaps = 13
    elif y > 1997 or (y == 1997 and d >= 182):
        leaps = 12
    elif y >= 1996:
        leaps = 11
    elif y > 1994 or (y == 1994 and d >= 182):
        leaps = 10
    elif y > 1993 or (y == 1993 and d >= 182):
        leaps = 9
    elif y > 1992 or (y == 1992 and d >= 182):
        leaps = 8
    elif y >= 1991:
        leaps = 7
    elif y >= 1990:
        leaps = 6
    elif y >= 1988:
        leaps = 5
    elif y > 1985 or (y == 1985 and d >= 182):
        leaps = 4
    elif y > 1983 or (y == 1983 and d >= 182):
        leaps = 3
    elif y > 1982 or (y == 1982 and d >= 182):
        leaps = 2
    elif y > 1981 or (y == 1981 and d >= 182):
        leaps = 1
    else:
        leaps = 0

    return calendar.timegm(tt) + x + leaps - _epoch


def PyTmUTCs80toYDS(s):
    """
    Converts timestamp using Caris 1980 epoch to Julian year-day-seconds

    Parameters
    ----------
    s: float, seconds since Caris 1980 epoch

    Returns
    -------
    list, [year as int, day as int, seconds as int]
    """
    if s >= 1025568016:
        s = s - 16  # leap year makes it day number 183
    elif s >= 915235215:
        s = s - 15
    elif s >= 820540814:
        s = s - 14
    elif s >= 599616013:
        s = s - 13
    elif s >= 552182412:
        s = s - 12
    elif s >= 504921611:
        s = s - 11
    elif s >= 457488010:
        s = s - 10
    elif s >= 425952009:
        s = s - 9
    elif s >= 394329608:
        s = s - 8  # elif y > 1992 or (y == 1992 and d >= 182): leaps = 8
    elif s >= 347155207:
        s = s - 7
    elif s >= 315619206:
        s = s - 6
    elif s >= 252460805:
        s = s - 5
    elif s >= 173491204:
        s = s - 4
    elif s >= 110332803:
        s = s - 3
    elif s >= 78796802:
        s = s - 2
    elif s >= 47260801:
        s = s - 1
    tt = time.gmtime(s + _epoch)
    x = divmod(s, 1)[1]
    s_of_day = PyTmHMSXtoS(tt.tm_hour, tt.tm_min, tt.tm_sec, x)
    return [tt.tm_year, tt.tm_yday, s_of_day]


def PyTmYDSminusYDS(y1, d1, s1, y2, d2, s2):
    """
    Get difference between two year-day-seconds entries

    NOTE: the original Caris function doesn't take leap seconds into account

    Parameters
    ----------
    y1: int, year 1
    d1: int, day 1
    s1: int, seconds 1
    y2: int, year 2
    d2: int, day 2
    s2: int, seconds 2

    Returns
    -------
    int, difference in entries as timestamp since Caris 1980 epoch
    """
    return PyTmYDStoUTCs80(y2, d2, s2) - PyTmYDStoUTCs80(y1, d1, s1)


def PyTmYDSplusS(y, d, s, add_sec):
    """
    Take year-day-seconds entry and add add_sec to it, returning  timestamp since Caris 1980 epoch

    NOTE: the original Caris function doesn't seem to work as expected
      _PyPeekXTF70.PyTmYDSplusS(1997,2,200,1)
       [1997, 2, -86199.0]
      PyTmYDSplusS(1997,2,200,1)
       [1997, 2, 201.0]

    Parameters
    ----------
    y: int, year
    d: int, day
    s: float, seconds
    add_sec: float, seconds to add

    Returns
    -------
    list, [year as int, day as int, seconds as int]
    """

    s80 = PyTmYDStoUTCs80(y, d, s)
    s80 += add_sec
    return PyTmUTCs80toYDS(s80)    


def UTCs80ToDateTime(t):
    """
    Takes in UTC timestamp since Caris 1980 epoch, outputs datetime object

    Parameters
    ----------
    t: float, seconds since Caris 1980 epoch

    Returns
    -------
    datetime object representing timestamp

    """
    year, d, s = PyTmUTCs80toYDS(t)
    hour, minute, sec, x = PyTmStoHMSX(s)
    month, day = PyTmYJDtoMD(year,d)
    try:
        return datetime(*[year, month, day, hour, minute, sec, int(x*1000000)])
    except ValueError:
        while sec > 59:
            sec -= 60
            minute += 1
        while minute > 59:
            minute -= 60
            hour += 1
        while hour > 23:
            hour -= 24
            d = d + 1
            month, day = PyTmYJDtoMD(year, d)
        return datetime(*[year, month, day, hour, minute, sec, int(x*1000000)])


def DateTimeToUTCs80(dt):
    """
    Takes in datetime object, outputs UTC timestamp since Caris 1980 epoch

    Parameters
    ----------
    dt: datetime object representing timestamp

    Returns
    -------
    float, seconds since Caris 1980 epoch

    """
    y = dt.year
    d = PyTmYMDtoJD(y, dt.month, dt.day)
    s = PyTmHMSXtoS(dt.hour, dt.minute, dt.second, dt.microsecond/1000000.0)
    return PyTmYDStoUTCs80(y, d, s)
