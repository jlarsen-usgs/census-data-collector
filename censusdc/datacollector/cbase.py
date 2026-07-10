import requests
from .tigerweb import TigerWebVariables
from ..utils import  RestartableThread, thread_count, get_cache
import threading
import geopandas as gpd
import pandas as pd
import warnings
warnings.simplefilter('always', UserWarning)
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError

try:
    import ray
    ENABLE_MULTIPROC = True
except ImportError:
    # fake ray wrapper function for windows
    from ..utils import ray

    ENABLE_MULTIPROC = False


class CensusBase(object):
    """
    Base class for Sf3, Acs1 and Acs5 data

    Parameters
    ----------
    features: GeoDataFrame
        features from TigerWeb data collections
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)
    dataset : str
        dataset name e.g. "acs-acs5"

    """
    def __init__(self, features, year, apikey, dataset):
        from .data_discovery import get_supported_products
        from ..utils.servers import identify_census_discretization

        if not isinstance(features, gpd.GeoDataFrame):
            raise TypeError("Features must be supplied as a geodataframe")

        self._features = features
        self._year = year
        self.__apikey = apikey
        self._dataset = dataset.lower()

        # check the year and get a record for the requested dataset
        products = get_supported_products()
        products = products[products.dataset == self._dataset]
        if len(products) == 0:
            raise NotImplementedError(f"No support implemented for {self._dataset}")

        if year not in products.vintage.values:
            raise AssertionError(
                f"{year} does not exist for {dataset}. "
                f"Valid years include {products.vintage.values()} "
            )

        self._geography = identify_census_discretization(
            self._features["GEOID"].values[0]
        )
        # todo: check the geography against the product and year

        self._census_features = {}
        self.__thread_fail = {}

    @property
    def year(self):
        """
        Method to return the ACS query year
        """
        return self._year

    @property
    def features(self):
        """
        Method to return the feature dictionary
        """
        if isinstance(self._census_features, pd.DataFrame):
            df = pd.merge(self._features, self._census_features, on="GEOID", how="left")
            return df
        else:
            raise AssertionError("Please run get_data() prior to grabbing features")

    @property
    def geography(self):
        """
        Geography level of requested data

        """
        return self._geography

    def join(self, cenobj):
        """
        Method to left join two census data objects into a single object
        with all varaibles.

        Parameters
        ----------
        cenobj : CensusBase object
            census object to join data from

        Returns
        -------
            None
        """
        cen_feats = self._census_features
        other = cenobj._census_features

        new_cen_feats = pd.merge(cen_feats, other, how="left", on="GEOID")
        self._census_features = new_cen_feats

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
        variables : list, tuple
            user specified Acs5 variables, default pulls variables from
            the Acs5Variables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multiproc : bool
            multiprocessing support using ray, linux only!
        multithread : bool
            flag to use a multithreaded method of collecting census data
        thread_pool : int
            number of threads to perform data collection while multithreading
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        from ..utils.servers import get_format_str, get_base_url

        self._census_features = {}
        url = get_base_url(self._dataset, self.year)

        if use_cache:
            cache = get_cache(
                self._dataset,
                self.year,
                self._geography,
                variables=variables,
                apikey=self.__apikey,
                verbose=verbose
            )

            cache_variables = ["GEOID",] + [var for var in variables if var in list(cache)]
            cache = cache[cache_variables]
            cache_features = pd.merge(self._features, cache, how="inner", on="GEOID")
            cache_features[cache_features[cache_variables[1:]] < 0] = float("nan")
            cached_geoids = cache_features["GEOID"].unique()
            pull_features = self._features[~self._features["GEOID"].isin(cached_geoids)]
            self._census_features = {var : cache_features[var].to_list() for var in cache_variables}
        else:
            pull_features = self._features
            self._census_features = {var: [] for var in ["GEOID"] + list(variables)}

        if variables:
            if isinstance(variables, str):
                variables = (variables,)
            variables = ",".join(variables)

        fmt = get_format_str(self._geography)

        if multiproc and not ENABLE_MULTIPROC:
            multiproc = False
            multithread = True
            thread_pool = thread_count() - 1

        if multiproc:
            year = self.year
            apikey = self.__apikey
            actors = []
            twv = {k: v for k, v in TigerWebVariables.__dict__.items()
                   if not k.startswith("__")}
            for feature in pull_features.itertuples():
                feature = feature._asdict()
                actor = multiproc_data_request.remote(
                    year, apikey, feature, self._geography,
                    fmt, variables, url,
                    retry, verbose, twv)

                actors.append(actor)

            output = ray.get(actors)

            for out in output:
                if out is None:
                    continue
                else:
                    geoid, data = out
                    self._census_features["GEOID"].append(geoid)
                    for dix, header in enumerate(data[0]):
                        if header == "NAME":
                            continue
                        elif header not in variables:
                            continue
                        else:
                            try:
                                value = float(data[1][dix])
                                if value < 0:
                                    value = float("nan")

                                self._census_features[header].append(value)
                            except (TypeError, ValueError):
                                self._census_features[header].append(float('nan'))

        elif multithread:
            container = threading.BoundedSemaphore(thread_pool)
            thread_list = []
            thread_id = 0
            for feature in pull_features.itertuples():
                feature = feature._asdict()
                x = RestartableThread(target=self.threaded_request_data,
                                      args=(feature, self._geography, fmt, variables,
                                            url, retry, verbose, thread_id, container))
                thread_list.append(x)
                thread_id += 1

            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()

            restart = []
            for thread_id, fail in self.__thread_fail.items():
                if fail:
                    restart.append(thread_list[thread_id].clone())
            for thread in restart:
                thread.start()
            for thread in restart:
                thread.join()

        else:
            for feature in pull_features.itertuples():
                feature = feature._asdict()
                self.__request_data(
                    feature, self._geography, fmt, variables, url, retry, verbose
                )

        self._census_features = pd.DataFrame.from_dict(self._census_features)

    def __request_data(self, feature, geography, fmt, variables, url, retry, verbose):
        """
        Request data method for serial and multithreaded applications

        Parameters
        ----------
        feature : dict
            dictionary of geopandas record
        geography : str
            census data geography
        fmt : str
            format of geography request
        variables : str
            string of census variables
        url : str
            string of census url
        retry : int
            number of retries based on connection error
        verbose : bool
            verbose operation flag

        Returns
        -------

        """
        loc = ""
        if geography == "block":
            loc = fmt.format(
                feature[TigerWebVariables.block],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county],
                feature[TigerWebVariables.tract]
            )
        elif geography == "block_group":
            loc = fmt.format(
                feature[TigerWebVariables.blkgrp],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county],
                feature[TigerWebVariables.tract]
            )
        elif geography == "tract":
            loc = fmt.format(
                feature[TigerWebVariables.tract],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county]
            )
        elif geography == "place":
            loc = fmt.format(
                feature[TigerWebVariables.place],
                feature[TigerWebVariables.state]
            )
        elif geography == "county_subdivision":
            loc = fmt.format(
                feature[TigerWebVariables.cousub],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county]
            )
        elif geography == "county":
            loc = fmt.format(
                feature[TigerWebVariables.county],
                feature[TigerWebVariables.state]
            )
        elif geography == "state":
            loc = fmt.format(
                feature[TigerWebVariables.state]
            )
        else:
            raise AssertionError("geography is undefined")

        s = requests.session()

        if self.year == 1990:
            payload = {"get": variables,
                       "for": loc,
                       "key": self.__apikey}
        else:
            payload = {'get': "NAME," + variables,
                       'for': loc,
                       'key': self.__apikey}

        payload = "&".join(
            '{}={}'.format(k, v) for k, v in payload.items())

        n = 0
        err = "Unknown connection error"
        while n < retry:
            try:
                r = s.get(url, params=payload)
                r.raise_for_status()
                break
            except (requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout) as e:
                err = e
                n += 1
                if verbose:
                    print("Connection Error: Retry number "
                          "{}".format(n))

        if n == retry:
            if "unknown/unsupported" in r.text:
                return
            else:
                raise requests.exceptions.HTTPError(err)

        if verbose:
            print(f'Getting {geography} data for GEOID {feature["GEOID"]}')
        try:
            data = r.json()
        except JSONDecodeError:
            data = []
            if verbose:
                print(f'Error getting {geography} data for GEOID {feature["GEOID"]}')

        if len(data) == 2:
            self._census_features["GEOID"].append(feature["GEOID"])
            for dix, header in enumerate(data[0]):
                if header == "NAME":
                    continue
                elif header not in variables:
                    continue
                else:
                    try:
                        value = float(data[1][dix])
                        if value < 0:
                            value = float("nan")

                        self._census_features[header].append(value)
                    except (TypeError, ValueError):
                        self._census_features[header].append(float('nan'))

    def threaded_request_data(self, feature, geography, fmt, variables,
                              url, retry, verbose, thread_id,
                              container):
        """
        Multithread method for requesting census data

        Parameters
        ----------
        feature : geoJSON
            geoJSON feature
        geography : str
            census data geography
        fmt : str
            format of geography request
        variables : str
            string of census variables
        url : str
            string of census url
        retry : int
            number of retries based on connection error
        verbose : bool
            verbose operation flag
        thread_id : int
            identifier for the thread
        container : BoundedSemaphore
            bound semaphore instance for thread pool management

        """
        container.acquire()
        self.__thread_fail[thread_id] = True
        self.__request_data(
            feature, geography, fmt, variables, url, retry, verbose
        )
        self.__thread_fail[thread_id] = False
        container.release()

    def check_variables(self, variables, defaults=None, validate=False):
        """
        General method to check variable inputs from ACS, Decennial, and other
        Census objects

        Parameters
        ----------
        variables : list, tuple, np.ndarray, DefaultInterface object
            user supplied variable codes
        defaults : DefaultInterface object
            DefaultInterface object for getting variables
        validate : bool
            flag to implement variable checking and validation prior to pulling census
            data. Useful for first run or so when using defined census variables.

        Returns
        -------

        """
        from ..utils.utilities import check_variables
        variables = check_variables(self._dataset, self.year, variables, validate=validate)
        return variables


@ray.remote
def multiproc_data_request(year, apikey, feature,
                           geography, fmt, variables, url, retry, verbose,
                           TigerwebVariables):
    """
    Multithread method for requesting census data

    Parameters
    ----------
    year : int
        data year
    apikey : str
        census apikey string
    feature : geoJSON
        geoJSON feature
    geography : str
        census data geography
    fmt : str
        format of geography request
    variables : str
        string of census variables
    url : str
        string of census url
    retry : int
        number of retries based on connection error
    verbose : str
        verbose operation flag
    """
    loc = ""
    if geography == "block":
        loc = fmt.format(
            feature[TigerWebVariables.block],
            feature[TigerWebVariables.state],
            feature[TigerWebVariables.county],
            feature[TigerWebVariables.tract]
        )
    elif geography == "block_group":
        loc = fmt.format(
            feature[TigerWebVariables.blkgrp],
            feature[TigerWebVariables.state],
            feature[TigerWebVariables.county],
            feature[TigerWebVariables.tract]
        )
    elif geography == "tract":
        loc = fmt.format(
            feature[TigerWebVariables.tract],
            feature[TigerWebVariables.state],
            feature[TigerWebVariables.county]
        )
    elif geography == "place":
        loc = fmt.format(
            feature[TigerWebVariables.place],
            feature[TigerWebVariables.state]
        )
    elif geography == "county_subdivision":
        loc = fmt.format(
            feature[TigerWebVariables.cousub],
            feature[TigerWebVariables.state],
            feature[TigerWebVariables.county]
        )
    elif geography == "county":
        loc = fmt.format(
            feature[TigerWebVariables.county],
            feature[TigerWebVariables.state]
        )
    elif geography == "state":
        loc = fmt.format(
            feature[TigerWebVariables.state]
        )
    else:
        raise AssertionError("geography is undefined")

    s = requests.session()

    if year == 1990:
        payload = {"get": variables,
                   "for": loc,
                   "key": apikey}
    else:
        payload = {'get': "NAME," + variables,
                   'for': loc,
                   'key': apikey}

    payload = "&".join(
        '{}={}'.format(k, v) for k, v in payload.items())

    n = 0
    err = "Unknown connection error"
    while n < retry:
        try:
            r = s.get(url, params=payload)
            r.raise_for_status()
            break
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            err = e
            n += 1
            if verbose:
                print("Connection Error: Retry number {}".format(n))

    if n == retry:
        if "unknown/unsupported" in r.text:
            return
        raise requests.exceptions.HTTPError(err)

    if verbose:
        print(f'Getting {geography} data for {feature["GEOID"]}')
    try:
        data = r.json()
    except JSONDecodeError:
        data = []
        if verbose:
            print(f'Error getting {geography} data for {feature["GEOID"]}')
    if len(data) == 2:
        return feature["GEOID"], data
    else:
        return None
