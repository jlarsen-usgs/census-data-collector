from .cbase import CensusBase
from ..defaults.census_defaults import CensusDefaults


class Acs1(CensusBase):
    """
    Class to collect data from the Acs1 census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs1, self).__init__(features, year, apikey, 'acs-acs1')

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs1 servers and set it to feature
        properties!

        Parameters
        ----------
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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs1, self).get_data(variables=variables,
                                   retry=retry, verbose=verbose,
                                   multiproc=multiproc,
                                   multithread=multithread,
                                   thread_pool=thread_pool,
                                   use_cache=use_cache)


class Acs3(CensusBase):
    """
    Class to collect data from the Acs3 census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs3, self).__init__(features, year, apikey, 'acs-acs1')

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs1 servers and set it to feature
        properties!

        Parameters
        ----------
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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs3, self).get_data(variables=variables,
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
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs5, self).__init__(features, year, apikey, 'acs-acs5')

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Acs5 servers and set it to feature
        properties!

        Parameters
        ----------
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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs5, self).get_data(variables=variables,
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
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs5Profile, self).__init__(features, year,
                                          apikey, 'acs-acs5-profile')

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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs5Profile, self).get_data(variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)


class Acs1Profile(CensusBase):
    """
    Class to collect data from the Acs1 profile census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs1Profile, self).__init__(features, year,
                                          apikey, 'acs-acs1-profile')

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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs1Profile, self).get_data(variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)


class Acs3Profile(CensusBase):
    """
    Class to collect data from the Acs3 profile census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, apikey):
        super(Acs3Profile, self).__init__(features, year,
                                          apikey, 'acs-acs3-profile')

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
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Acs3Profile, self).get_data(variables=variables,
                                          retry=retry, verbose=verbose,
                                          multiproc=multiproc,
                                          multithread=multithread,
                                          thread_pool=thread_pool,
                                          use_cache=use_cache)