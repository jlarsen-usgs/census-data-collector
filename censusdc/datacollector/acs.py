from .cbase import CensusBase
from ..defaults.census_defaults import DefaultInterface


class Acs5Defaults(DefaultInterface):
    """
    Container for loading and manipulating default variables for
    American Community Survey census product data pulls

    f : None or PathLike
        Optional file name or None. If None, code will load in the
        default file for acs5 variables (subproduct dependent)
    subproduct : None or str
        Optional sub-product name (e.g., "profile", "summary")

    """
    def __init__(self, f=None, subproduct=None):
        super().__init__(product="acs5", subproduct=subproduct)
        if f is None:
            if subproduct is None:
                f = self._base_path / "acs5_variables.dat"
            elif subproduct in ("profile", "summary"):
                f = self._base_path / f"acs5_{subproduct}_variables.dat"

        self._file = f
        self._load_dataframe()


class Acs1(CensusBase):
    """
    Class to collect data from the Acs1 census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs1, self).__init__(features, year, apikey, 'acs1')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs1 servers and set it to feature
        properties!

        Parameters
        ----------
        level : str
            determines the geographic level of data queried
            default is 'finest' available based on census dataset and
            the geoJSON feature information
        variables : list, tuple
            user specified Acs1 variables, default pulls variables from
            the AcsVariables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multithread : bool
            boolean flag to allow multithreading of data collection
        multiproc : bool
            multiprocessing support using ray, linux only!
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        super(Acs1, self).get_data(level=level, variables=variables,
                                   retry=retry, verbose=verbose,
                                   multiproc=multiproc,
                                   multithread=multithread,
                                   thread_pool=thread_pool,
                                   use_cache=use_cache)


class Acs5(CensusBase):
    """
    Class to collect data from the Acs5 census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs5, self).__init__(features, year, apikey, 'acs5')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
        level : str
            determines the geographic level of data queried
            default is 'finest' available based on census dataset and
            the geoJSON feature information
        variables : list, tuple, DefaultInterface object
            user specified Acs5 variables, default pulls variables from
            the AcsVariables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multiproc : bool
            multiprocessing support using ray, linux only!
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        super(Acs5, self).get_data(level=level, variables=variables,
                                   retry=retry, verbose=verbose,
                                   multiproc=multiproc,
                                   multithread=multithread,
                                   thread_pool=thread_pool,
                                   use_cache=use_cache)


class Acs5Profile(CensusBase):
    """
    Class to collect data from the Acs5 profile census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs5Profile, self).__init__(features, year,
                                          apikey, 'acs5profile')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
        level : str
            determines the geographic level of data queried
            default is 'finest' available based on census dataset and
            the geoJSON feature information
        variables : list, tuple
            user specified Acs5 variables, default pulls variables from
            the AcsVariables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multiproc : bool
            multiprocessing support using ray, linux only!
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        super(Acs5Profile, self).get_data(level=level, variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)


class Acs1Profile(CensusBase):
    """
    Class to collect data from the Acs5 profile census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs1Profile, self).__init__(features, year,
                                          apikey, 'acs1profile')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
        level : str
            determines the geographic level of data queried
            default is 'finest' available based on census dataset and
            the geoJSON feature information
        variables : list, tuple
            user specified Acs5 variables, default pulls variables from
            the AcsVariables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multiproc : bool
            multiprocessing support using ray, linux only!
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        super(Acs1Profile, self).get_data(level=level, variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)


class Acs5Summary(CensusBase):
    """
    Class to collect data from the Acs5 profile census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}
    year : int
        census data year of the TigerWeb data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs5Summary, self).__init__(features, year,
                                          apikey, 'acs5summary')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
        level : str
            determines the geographic level of data queried
            default is 'finest' available based on census dataset and
            the geoJSON feature information
        variables : list, tuple
            user specified Acs5 variables, default pulls variables from
            the AcsVariables class
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multiproc : bool
            multiprocessing support using ray, linux only!
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        super(Acs5Summary, self).get_data(level=level, variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)
