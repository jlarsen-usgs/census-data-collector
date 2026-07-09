from pathlib import Path
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


def census_cache_builder(
    dataset,
    geography,
    variables=None,
    apikey="",
    multithread=False,
    thread_pool=4,
    refresh=False
):
    """
    Method to build out cache for all supported census years for a
    specific census "geography"

    Parameters
    ----------
    dataset : str
        census data product, e.g. acs-acs5
        supported data products can be found by using the
        data_discovery.get_supported_products() method
    geography : str
        census discretization
    variables : str, list, or None
        string or a list of census variables for a given data product. If None
        are provided, code will try to pull variables from stored defaults
    apikey : str
        census api key
    multithread : bool
        flag to enable multithreaded cache building
    thread_pool : int
        number of threads to use for multithreading.
    refresh : bool
        boolean flag to refresh the existing cached data

    Returns
    -------
        None
    """
    from ..datacollector import data_discovery

    geography = geography.lower()
    geography = geography.replace("_", " ")
    dataset = dataset.lower()
    # todo: fuzzy match geographies....

    products = data_discovery.get_supported_products()
    if dataset not in products.dataset.values:
        raise NotImplementedError(f"censusdc does not yet have support for {dataset}")

    products = products[products.dataset == dataset]
    iyears = list(sorted(products.vintage.values))

    years = []
    for year in iyears:
        geographies = data_discovery.get_geographies(dataset, year)
        if geography in geographies["name"].values:
            years.append(year)

    if not years:
        raise AssertionError(
            f"Geography {geography} not currently supported or not available for {dataset}"
        )

    if multithread:
        container = threading.BoundedSemaphore(thread_pool)
        thread_list = []
        for year in years:
            x = RestartableThread(target=_threaded_get_cache,
                                  args=(dataset, year, geography, apikey, True, 100,
                                        True, container))
            thread_list.append(x)

        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

    else:
        for year in years:
            get_cache(
                dataset, year, geography=geography, variables=variables, apikey=apikey, refresh=refresh, verbose=True
            )


def _threaded_get_cache(dataset, year, geography, variables, apikey, refresh,
                        retry, verbose, container):
    """
    Multithreaded method to build and load cache tables of census data to
    improve performance

    Parameters
    ----------
    dataset : str
        census dataset string
    year : int
        census year
    geography : str
        census discretization
    variables : CensusDefaults, list
        variables list or object
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
    get_cache(dataset, year, geography, variables, apikey, refresh, retry, verbose)
    container.release()


def get_cache(dataset, year, geography, variables=None, apikey="", refresh=False,
              retry=100, verbose=False):
    """
    Method to build and load cache tables of census data to
    improve performance

    Parameters
    ----------
    dataset : str
        census dataset string. Supported datasets can be discovered with the
        census data_discovery tools.
    year : int
        census year
    geography : str
        census discretization
    variables : None, CensusDefaults, list
        variables can pass None for grabbing dataset from file
    apikey : str
        census api key
    refresh : boolean
        option to refresh existing cache

    Returns
    -------
        pd.DataFrame
    """
    from .servers import get_base_url, get_cache_format_str
    from ..defaults.census_defaults import CensusDefaults, DefaultInterface

    if verbose:
        print("Building census cache for {}, {}".format(year, geography))
    geography = geography.lower()

    utils_dir = Path(__file__).parent
    table_file = utils_dir / "../cache/{}_{}_{}.dat".format(year, dataset, geography)

    if not table_file.exists() or refresh:
        url_base = get_base_url(dataset, year)

        if isinstance(variables, (list, tuple)):
            pass
        elif isinstance(variables, str):
            variables = [variables,]
        if variables is None:
            varobj = CensusDefaults(dataset)
            variables = varobj.parameter_codes
        elif isinstance(variables, DefaultInterface):
            variables = variables.parameter_codes
        else:
            raise TypeError(
                f"Unsupported type {type(variables)} for variables parameter"
            )

        variables = ','.join(variables)

        fmt = get_cache_format_str(geography)
        fips_co = None
        if dataset == "dec-sf3" and year == 2000 and geography == "block group":
            fips_co = pd.read_csv(
                utils_dir / "fips_county_table.dat", dtype=str
            ).to_numpy()
            fmt = "block%20group:*&in=state:{}&in=county:{}&in=tract:*"

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
                    r = s.get(url_base, params=payload)
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
                    df = pd.concat((df, tdf), ignore_index=True)

        df.to_csv(table_file, index=False)

    if geography == "tract":
        fmter = "{:06d}"
    elif geography == "place":
        fmter = "{:05d}"
    else:
        fmter = "{}"

    df = pd.read_csv(table_file)
    if geography == "block_group":
        df[geography] = [fmter.format(i) for i in df["block group"].values]
    else:
        df[geography] = [fmter.format(i) for i in df[geography].values]

    df['state'] = ["{:02d}".format(i) for i in df['state'].values]
    if geography == "tract":
        df['county'] = ["{:03d}".format(i) for i in df['county'].values]
        df['GEOID'] = df['state'] + df['county'] + df[geography]
    elif geography == "block group":
        df['county'] = ["{:03d}".format(i) for i in df['county'].values]
        df['tract'] = ["{:06d}".format(i) for i in df['tract'].values]
        df["GEOID"] = df["state"] + df["county"] + df["tract"] + df[geography]
        df.drop(columns=["block group"], inplace=True)
    else:
        df['GEOID'] = df['state'] + df[geography]

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
