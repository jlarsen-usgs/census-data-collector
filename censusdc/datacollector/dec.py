from .cbase import CensusBase
from ..defaults.census_defaults import CensusDefaults


class Sf3(CensusBase):
    """
    Class to collect data from the Sf3 census using geojson features
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
        super(Sf3, self).__init__(features, year, apikey, 'dec-sf3')

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Sf3 servers and set it to feature
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
        multiproc : bool
            multiprocessing support using ray
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.

        """
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Sf3, self).get_data(variables=variables,
                                  retry=retry, verbose=verbose,
                                  multiproc=multiproc,
                                  multithread=multithread,
                                  thread_pool=thread_pool,
                                  use_cache=False)


class Sf1(CensusBase):
    """
    Class to collect data from the Sf1 census using geojson features
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
        super(Sf1, self).__init__(features, year, apikey, 'dec-sf1')

    def get_data(self, variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Sf3 servers and set it to feature
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
        multiproc : bool
            multiprocessing support using ray
        multithread : bool
            boolean flag to allow multithreading of data collection
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census api data over real time data
            collection.
        """
        variables = self.check_variables(variables, CensusDefaults(self._dataset))
        super(Sf1, self).get_data(variables=variables,
                                  retry=retry, verbose=verbose,
                                  multiproc=multiproc,
                                  multithread=multithread,
                                  thread_pool=thread_pool,
                                  use_cache=use_cache)
