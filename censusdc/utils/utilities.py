import os
import requests
import pandas as pd
import threading
import datetime
from difflib import SequenceMatcher
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError


STATE_FIPS = ("01", "02", "04", "05", "06",
              "08", "09", "10", "11", "12",
              "13", "15", "16", "17", "18",
              "19", "20", "21", "22", "23",
              "24", "25", "26", "27", "28",
              "29", "30", "31", "32", "33",
              "34", "35", "36", "37", "38",
              "39", "40", "41", "42", "44",
              "45", "46", "47", "48", "49",
              "50", "51", "53", "54", "55",
              "56", "60", "66", "69", "70",
              "72", "78", "99", "74", "81",
              "84", "86", "67", "89", "71",
              "76", "95", "79")


def isnumeric(s):
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def isdatetime(s):
    if isinstance(s, (datetime.date, datetime.datetime)):
        return True
    else:
        return False


def isbytes(s):
    if isinstance(s, bytes):
        return True
    else:
        return False


def census_cache_builder(level='tract', apikey="",
                         multithread=False, thread_pool=4,
                         profile=False, summary=False, refresh=False):
    """
    Method to build out cache for all supported census years for a
    specific census "level"

    Parameters
    ----------
    level : str
        census discretization
    apikey : str
        census api key
    multithread : bool
        flag to enable multithreaded cache building
    thread_pool : int
        number of threads to use for multithreading.
    profile : bool
        boolean flag to indicate that economic profile data is
        being collected and cached
    summary : bool
        boolean flag to get data from census summary tables
    refresh : bool
        boolean flag to refresh the existing cached data

    Returns
    -------
        None
    """
    level = level.lower()
    years = ()
    if level == "tract":
        years = (2000, 2009, 2010, 2011,
                 2012, 2013, 2014, 2015,
                 2016, 2017, 2018, 2019)

    if level == "place":
        years = (2000,) + tuple(range(2005, 2021))

    elif level == "block_group":
        years = (2000, 2010) + tuple(range(2013, 2022))

    if multithread:
        container = threading.BoundedSemaphore(thread_pool)
        thread_list = []
        for year in years:
            x = RestartableThread(target=_threaded_get_cache,
                                  args=(year, level, apikey, True, 100,
                                        True, profile, summary, container))
            thread_list.append(x)

        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

    else:
        for year in years:
            get_cache(year, level=level, apikey=apikey, refresh=refresh,
                      verbose=True, profile=profile, summary=summary)


def _threaded_get_cache(year, level, apikey, refresh,
                        retry, verbose, profile, summary, container):
    """
    Multithreaded method to build and load cache tables of census data to
    improve performance

    Parameters
    ----------
    year : int
        census year
    level : str
        census discretization
    apikey : str
        census api key
    refresh : boolean
        option to refresh existing cache
    profile : bool
        option to collect economic data

    Returns
    -------
        pd.DataFrame
    """
    container.acquire()
    get_cache(year, level, apikey, refresh, retry, verbose, profile, summary)
    container.release()


def get_cache(year, level='tract', apikey="", refresh=False,
              retry=100, verbose=False, profile=False, summary=False):
    """
    Method to build and load cache tables of census data to
    improve performance

    Parameters
    ----------
    year : int
        census year
    level : str
        census discretization
    apikey : str
        census api key
    refresh : boolean
        option to refresh existing cache
    profile : bool
        option to collect economic profile data
    Returns
    -------
        pd.DataFrame
    """
    if verbose:
        print("Building census cache for {}, {}".format(year, level))
    level = level.lower()
    if level not in ("tract", "place", "block_group"):
        raise NotImplementedError()

    utils_dir = os.path.dirname(os.path.abspath(__file__))
    if profile:
        table_file = os.path.join(utils_dir, '..', 'cache',
                                  "{}_{}profile.dat".format(level, year))
    elif summary:
        table_file = os.path.join(utils_dir, "..", "cache",
                                  "{}_{}summary.dat".format(level, year))
    else:
        table_file = os.path.join(utils_dir, '..', 'cache',
                                  "{}_{}.dat".format(level, year))

    if not os.path.isfile(table_file) or refresh:
        from .servers import Acs1Server, Acs5Server, Sf1Server, \
            Acs5ProfileServer, Acs1ProfileServer, Acs5SummaryServer, \
            Sf3Server

        fips_co = None
        if level == "place":
            if profile:
                if year in (2000,):
                    return
                elif year in range(2005, 2009):
                    server = Acs1ProfileServer
                else:
                    server = Acs5ProfileServer
            elif summary:
                if year in (2000,):
                    return
                elif year in range(2005, 2009):
                    return
                elif year == 2009:
                    server = Acs5SummaryServer
                else:
                    return
            else:
                if year in (2000,):
                    server = Sf1Server
                elif year in range(2005, 2009):
                    server = Acs1Server
                else:
                    server = Acs5Server
        elif level == "block_group":
            if year in (2000,):
                server = Sf3Server
                fips_co = pd.read_csv(
                    os.path.join(utils_dir, "fips_county_table.dat"),
                    dtype=str
                ).to_numpy()
            elif year in (2010,):
                server = Sf1Server
            else:
                server = Acs5Server
        else:
            if profile:
                if year == 2000:
                    return
                else:
                    server = Acs5ProfileServer
            elif summary:
                if year == 2000:
                    return
                elif year == 2009:
                    server = Acs5SummaryServer
                else:
                    return
            else:
                if year in (2000,):
                    server = Sf1Server
                else:
                    server = Acs5Server

        url = server.base.format(year)
        server_dict = {}
        if level == "tract":
            server_dict = server.cache_tract[year]
        elif level == "place":
            server_dict = server.cache_place[year]
        elif level == "block_group":
            server_dict = server.cache_block_group[year]

        fmt = server_dict['fmt']
        variables = server_dict['variables']


        if fips_co is None:
            iterator = STATE_FIPS
        else:
            iterator = fips_co

        df = None
        for itr in iterator:
            if fips_co is None:
                state = itr
                loc = fmt.format(state)
                if verbose:
                    print("building cache for {}, FIPS code {}".format(year,
                                                                       state))
            else:
                state, county = tuple(itr)
                loc = fmt.format(state, county)
                if verbose:
                    print("building cache for {}, "
                          "FIPS code {}, County {}".format(year, state, county))

            s = requests.session()
            payload = {"get": "NAME," + variables,
                       "for": loc,
                       "key": apikey}

            payload = "&".join(
                '{}={}'.format(k, v) for k, v in payload.items())

            n = 0
            e = "Unknown connection error"
            while n < retry:
                try:
                    r = s.get(url, params=payload)
                    r.raise_for_status()
                    break
                except (requests.exceptions.HTTPError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ReadTimeout) as e:
                    excpt = e
                    n += 1
                    print("Connection Error: Retry number "
                          "{}".format(n))

            if n == retry:
                raise requests.exceptions.HTTPError(excpt)

            try:
                data = r.json()
            except JSONDecodeError:
                data = []

            if data:
                try:
                    tdf = pd.DataFrame(data[1:], columns=data[0])
                except TypeError:
                    continue

                if df is None:
                    df = tdf
                else:
                    df = df.append(tdf, ignore_index=True)

        df.to_csv(table_file, index=False)

    if level == "tract":
        fmter = "{:06d}"
    elif level == "place":
        fmter = "{:05d}"
    elif level == "block_group":
        fmter = "{:01d}"
    else:
        fmter = "{}"

    df = pd.read_csv(table_file)
    if level == "block_group":
        df[level] = [fmter.format(i) for i in df["block group"].values]
    else:
        df[level] = [fmter.format(i) for i in df[level].values]
    df['state'] = ["{:02d}".format(i) for i in df['state'].values]
    if level == "tract":
        df['county'] = ["{:03d}".format(i) for i in df['county'].values]
        df['geoid'] = df['state'] + df['county'] + df[level]
    elif level == "block_group":
        df['county'] = ["{:03d}".format(i) for i in df['county'].values]
        df['tract'] = ["{:06d}".format(i) for i in df['tract'].values]
        df["geoid"] = df["state"] + df["county"] + df["tract"] + df[level]
        df.drop(columns=["block group"], inplace=True)
    else:
        df['geoid'] = df['state'] + df[level]

    return df


def thread_count():
    """
    Method to estimate the thread count on a user's machine

    Returns
    -------
        int : number of threads
    """
    import os
    nthreads = os.cpu_count()
    return nthreads


class RestartableThread(threading.Thread):
    """
    Restartable instance of a thread
    """
    def __init__(self, *args, **kwargs):
        self.myargs, self.mykwargs = args, kwargs
        super().__init__(*args, **kwargs)

    def clone(self):
        return RestartableThread(*self.myargs, **self.mykwargs)



def sequence_matcher(s, valid, fail_ratio=0.33):
    """
    Generalized simple fuzzy string matcher. Calculates sequence ratios for
    a string based on possible valid inputs

    Parameters
    ----------
    s : str
        sting to match to a valid parameter
    valid : list or tuple
        list or tuple possbile valid parameters
    fail_ratio : float
        failure ratio where no match will be found an error is raised

    Returns
    -------
        valid string
    """
    s = s.lower()
    scores = []
    for vu in valid:
        score = SequenceMatcher(None, vu, s).ratio()
        scores.append(score)

    max_score = max(scores)
    if max_score < fail_ratio:
        raise AssertionError(
            f"cannot determine valid parameter from {s}; valid entries include {valid}"
        )
    uidx = scores.index(max(scores))
    vs = valid[uidx]
    return vs
