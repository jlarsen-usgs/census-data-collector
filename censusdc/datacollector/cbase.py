import requests
from .tigerweb import TigerWebVariables
from ..utils import Acs5Server, Acs1Server, Sf3Server, RestartableThread, \
    thread_count, Sf1Server, get_cache, Acs5ProfileServer, Acs1ProfileServer, \
    Acs5SummaryServer
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

        self._census_table_rec = products[products["vintage"] == year]
        self._base_url = "/".join(
            self._census_table_rec["geographyLink"].values[0].split("/")[:-1]
        )
        self._geography = identify_census_discretization(
            self._features["GEOID"].values[0]
        )

        # todo: should be able to remove this set of if/elif statements
        #  server objects should be unnecessary now!
        if dataset == 'acs-acs1':
            self._server = Acs1Server
            self.__level_dict = {0: 'place', 1: 'state', 2: 'county',
                                 3: 'county_subdivision'}
            self.__ilevel_dict = {'place': 0, 'state': 1, 'county': 2,
                                  'county_subdivision': 3}

        elif dataset == 'acs-acs1-profile':
            self._server = Acs1ProfileServer
            self.__level_dict = {0: 'place', 1: 'state', 2: 'county',
                                 3: 'county_subdivision'}
            self.__ilevel_dict = {'place': 0, 'state': 1, 'county': 2,
                                  'county_subdivision': 3}

        elif dataset == 'acs-acs5':
            self._server = Acs5Server
            self.__level_dict = {0: 'place', 1: 'state', 2: 'county',
                                 3: 'tract', 4: 'block_group'}
            self.__ilevel_dict = {'place': 0, "state": 1, "county": 2,
                                  "tract": 3, "block_group": 4}
        elif dataset == 'acs-acs5-profile':
            self._server = Acs5ProfileServer
            self.__level_dict = {0: 'place', 1: 'state', 2: 'county',
                                 3: 'tract', 4: 'block_group'}
            self.__ilevel_dict = {'place': 0, "state": 1, "county": 2,
                                  "tract": 3, "block_group": 4}

        elif dataset == 'acs5summary':
            self._server = Acs5SummaryServer
            self.__level_dict = {0: 'place', 1: 'state', 2: 'county',
                                 3: 'tract', 4: 'block_group'}
            self.__ilevel_dict = {'place': 0, "state": 1, "county": 2,
                                  "tract": 3, "block_group": 4}

        elif dataset == "dec-sf3":
            self._server = Sf3Server
            self.__level_dict = {0: 'place', 1: "state", 2: "county",
                                 3: 'tract', 4: 'block_group'}
            self.__ilevel_dict = {'place': 0, "state": 1, "county": 2,
                                  "tract": 3, "block_group": 4}
        elif dataset == "dec-sf1":
            self._server = Sf1Server
            self.__level_dict = {0: 'place', 1: "state", 2: "county",
                                 3: 'tract', 4: 'block_group'}
            self.__ilevel_dict = {'place': 0, "state": 1, "county": 2,
                                  "tract": 3, "block_group": 4}
        else:
            raise AssertionError("unknown server type")

        # self._geography = 'undefined'
        self._census_features = {}
        self.__thread_fail = {}
        # self.__set_geography()

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
        Minimum common level (maximum resolution) of all the
        features supplied to Acs

        """
        return self._geography

    def join(self, cenobj):
        """
        Method to join two census data objects into a single object
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

    def __set_geography(self):
        """
        Internal method to determine a common 'finest' discretization
        of all supplied featues

        DEPRECATED Remove after new code testing
        """
        level = []
        feature_cols = list(self._features)

        tmp = 0
        for key in (TigerWebVariables.state,):
            if key in feature_cols:
                tmp = 1
            else:
                raise AssertionError("State number must be in "
                                     "properties dict")

        if tmp == 1:
            for key in (TigerWebVariables.place,
                        TigerWebVariables.state):
                if key in feature_cols:
                    tmp = 0
                else:
                    break

        if tmp == 1:
            for key in (TigerWebVariables.county,
                        TigerWebVariables.state):
                if key in feature_cols:
                    tmp = 2
                else:
                    tmp = 1
                    break

        if tmp == 2:
            if self._dataset == 'acs1':
                for key in (TigerWebVariables.cousub,
                            TigerWebVariables.state,
                            TigerWebVariables.county):
                    if key in feature_cols:
                        tmp = 3
                    else:
                        tmp = 2
                        break
            else:
                for key in (TigerWebVariables.tract,
                            TigerWebVariables.state,
                            TigerWebVariables.county):
                    if key in feature_cols:
                        tmp = 3
                    else:
                        tmp = 2
                        break
        if tmp == 3:
            for key in (TigerWebVariables.blkgrp,
                        TigerWebVariables.state,
                        TigerWebVariables.county,
                        TigerWebVariables.tract):
                if key in feature_cols:
                    tmp = 4
                else:
                    tmp = 3
                    break

        level.append(tmp)

        if not level:
            # hack around canadian huc12's for now. print a warning in future
            msg = "Cannot determine census data level: setting to 'tract'"
            print(f"WARNING: {msg}")
            self._geography = self.__level_dict[3]
        else:
            self._geography = self.__level_dict[min(level)]

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
        from .data_discovery import get_geographies
        from ..utils.servers import get_format_str
        self._census_features = {}
        url = self._base_url
        # url = self._server.base.format(self.year)

        lut = None
        # todo: we should be able to figure out the "level" by geoid length
        """
        if level == 'finest':
            for level in self._server.levels:
                lut = self._server.__dict__[level]
                if self.year in lut:
                    break
                else:
                    lut = None

            if level != self.geography:
                level = self.geography
                lut = self._server.__dict__[level]
                if self.year not in lut:
                    lut = None

        else:
            if level in self._server.__dict__:
                lut = self._server.__dict__[level]
                if self.year in lut:
                    pass
                else:
                    lut = None

            if self.__ilevel_dict[level] > \
                    self.__ilevel_dict[self.geography]:
                raise AssertionError("Cannot grab level data finer than {}"
                                     .format(self.geography))

        if lut is None:
            raise KeyError("No {} server could be found for {} and {}"
                           .format(self._dataset, self.year, level))
        """

        cache = None
        if use_cache and self._geography in ("tract", "place"):
            profile = False
            if 'profile' in self._dataset:
                profile = True
            cache = get_cache(self.year, self._geography, self.__apikey,
                              verbose=verbose, profile=profile, summary=True)

        # todo: continue clean ups here and remove variables stuff from "server"
        #  may need to create custom defaults for water-use prior to implementing this
        #  work here.
        if variables:
            if isinstance(variables, str):
                variables = (variables,)
            variables = ",".join(variables)

        self._census_features = {var: [] for var in ["GEOID"] + list(variables.split(","))}
        # todo: check the geography using the data discovery stuff...

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
            for feature in self._features.itertuples():
                feature = feature._asdict()
                if cache is not None:
                    geoid = feature["GEOID"]
                    record = cache.loc[cache['geoid'] == geoid]
                    if len(record) != 1:
                        record = None
                else:
                    record = None
                actor = multiproc_data_request.remote(
                    year, apikey, feature, self._geography,
                    fmt, variables, url,
                    retry, verbose, record, twv)

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
            for feature in self._features.itertuples():
                feature = feature._asdict()
                if cache is not None:
                    geoid = feature["GEOID"]
                    record = cache.loc[cache['geoid'] == geoid]
                    if len(record) != 1:
                        record = None
                else:
                    record = None

                x = RestartableThread(target=self.threaded_request_data,
                                      args=(feature, self._geography, fmt, variables,
                                            url, retry, verbose, record,
                                            thread_id, container))
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
            for feature in self._features.itertuples():
                feature = feature._asdict()
                if cache is not None:
                    geoid = feature["GEOID"]
                    record = cache.loc[cache['geoid'] == geoid]
                    if len(record) != 1:
                        record = None
                else:
                    record = None

                self.__request_data(
                    feature, self._geography, fmt, variables, url, retry, verbose, record
                )

        self._census_features = pd.DataFrame.from_dict(self._census_features)

    def __request_data(self, feature, level, fmt, variables,
                       url, retry, verbose, cache_record):
        """
        Request data method for serial and multithreaded applications

        Parameters
        ----------
        feature : dict
            dictionary of geopandas record
        level : str
            census data level
        fmt : str
            format of level request
        variables : str
            string of census variables
        url : str
            string of census url
        retry : int
            number of retries based on connection error
        verbose : bool
            verbose operation flag
        cache_record : pd.DataFrame or None
            cached census data record

        Returns
        -------

        """
        if cache_record is not None:
            self._census_features["GEOID"].append(feature["GEOID"])
            for column in list(cache_record):
                if column in ("NAME", 'state', 'county',
                              'tract', 'geoid', 'place'):
                    continue

                try:
                    self._census_features[column].append(
                        float(cache_record[column].values[0])
                    )
                except (TypeError, ValueError):
                    self._census_features[column].append(float("nan"))
            return

        loc = ""
        if level == "block_group":
            loc = fmt.format(
                feature[TigerWebVariables.blkgrp],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county],
                feature[TigerWebVariables.tract])
        elif level == "tract":
            loc = fmt.format(
                feature[TigerWebVariables.tract],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county])
        elif level == "place":
            loc = fmt.format(
                feature[TigerWebVariables.place],
                feature[TigerWebVariables.state]
            )
        elif level == "county_subdivision":
            loc = fmt.format(
                feature[TigerWebVariables.cousub],
                feature[TigerWebVariables.state],
                feature[TigerWebVariables.county])
        elif level == "county":
            loc = fmt.format(
                feature[TigerWebVariables.county],
                feature[TigerWebVariables.state])
        elif level == "state":
            loc = fmt.format(
                feature[TigerWebVariables.state])
        else:
            raise AssertionError("level is undefined")

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
            print(f'Getting {level} data for GEOID {feature["GEOID"]}')
        try:
            data = r.json()
        except JSONDecodeError:
            data = []
            if verbose:
                print(f'Error getting {level} data for GEOID {feature["GEOID"]}')

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

    def threaded_request_data(self, feature, level, fmt, variables,
                              url, retry, verbose, cache_record, thread_id,
                              container):
        """
        Multithread method for requesting census data

        Parameters
        ----------
        feature : geoJSON
            geoJSON feature
        level : str
            census data level
        fmt : str
            format of level request
        variables : str
            string of census variables
        url : str
            string of census url
        retry : int
            number of retries based on connection error
        verbose : bool
            verbose operation flag
        cache : pd.DataFrame or None
            dataframe of cached census data
        thread_id : int
            identifier for the thread
        container : BoundedSemaphore
            bound semaphore instance for thread pool management
        cache_record : pd.DataFrame or None
            cached census data record

        """
        container.acquire()
        self.__thread_fail[thread_id] = True
        self.__request_data(
            feature, level, fmt, variables, url, retry, verbose, cache_record
        )
        self.__thread_fail[thread_id] = False
        container.release()

    def check_variables(self, variables, defaults=None):
        """
        General method to check variable inputs from ACS, Decennial, and other
        Census objects

        Parameters
        ----------
        variables : list, tuple, np.ndarray, DefaultInterface object
            user supplied variable codes

        defaults : DefaultInterface object
            DefaultInterface object for getting variables

        Returns
        -------

        """
        from ..defaults.census_defaults import DefaultInterface

        if isinstance(variables, DefaultInterface):
            return variables.parameter_codes

        elif not variables:
            if defaults is not None:
                return defaults.parameter_codes
            else:
                raise AssertionError("Census variable codes must be provided")

        elif variables is None:
            if defaults is not None:
                return defaults.parameter_codes
            else:
                raise AssertionError("Census variable codes must be provided")

        elif isinstance(variables, (list, tuple)):
            return list(variables)

        elif isinstance(variables, str):
            return [variables,]

        else:
            raise TypeError(f"{type(variables)} not supported for variables parameter")



@ray.remote
def multiproc_data_request(year, apikey, feature,
                           level, fmt, variables, url, retry, verbose,
                           cache_record, TigerwebVariables):
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
    level : str
        census data level
    fmt : str
        format of level request
    variables : str
        string of census variables
    url : str
        string of census url
    retry : int
        number of retries based on connection error
    verbose : str
        verbose operation flag
    cache_record : pd.Dataframe or None
        a record of cached data from a pandas dataframe
    """
    if cache_record is not None:
        l0 = []
        l1 = []
        for column in list(cache_record):
            if column in ('state', 'county',
                          'tract', 'geoid',
                          'blkgrp', 'place'):
                continue
            else:
                l0.append(column)
                l1.append(cache_record[column].values[0])
        data = [l0, l1]
        return feature["GEOID"], data

    loc = ""
    if level == "block_group":
        loc = fmt.format(
            feature[TigerwebVariables['blkgrp']],
            feature[TigerwebVariables['state']],
            feature[TigerwebVariables['county']],
            feature[TigerwebVariables['tract']])
    elif level == "tract":
        loc = fmt.format(
            feature[TigerwebVariables['tract']],
            feature[TigerwebVariables['state']],
            feature[TigerwebVariables['county']])
    elif level == "county_subdivision":
        loc = fmt.format(
            feature[TigerwebVariables['cousub']],
            feature[TigerwebVariables['state']],
            feature[TigerwebVariables['county']])
    elif level == "place":
        loc = fmt.format(
            feature[TigerWebVariables.place],
            feature[TigerWebVariables.state])
    elif level == "county":
        loc = fmt.format(
            feature[TigerwebVariables['county']],
            feature[TigerwebVariables['state']])
    elif level == "state":
        loc = fmt.format(
            feature[TigerwebVariables['state']])
    else:
        raise AssertionError("level is undefined")

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
        print(f'Getting {level} data for {feature["GEOID"]}')
    try:
        data = r.json()
    except JSONDecodeError:
        data = []
        if verbose:
            print(f'Error getting {level} data for {feature["GEOID"]}')
    if len(data) == 2:
        return feature["GEOID"], data
    else:
        return None
