from .cbase import CensusBase
from ..defaults.census_defaults import DefaultInterface


class Sf3Defaults(DefaultInterface):
    """
    Container for loading and manipulating default variables for
    the Decennial Census summary file 3 census product data pulls

    f : None or PathLike
        Optional file name or None. If None, code will load in the
        default file for sf3 variables
    subproduct : None
        Unused variable in SF3

    """
    def __init__(self, f=None, subproduct=None):
        super().__init__("sf3", subproduct=None)
        if f is None:
            f = self._base_path / "sf3_variables.dat"

        self._file = f
        self._load_dataframe()


class Sf1Defaults(DefaultInterface):
    """
    Container for loading and manipulating default variables for
    the Decennial Census summary file 1 census product data pulls

    f : None or PathLike
        Optional file name or None. If None, code will load in the
        default file for sf1 variables
    subproduct : None
        Unused variable in SF1

    """
    def __init__(self, f=None, subproduct=None):
        super().__init__("sf1", subproduct=None)
        if f is None:
            f = self._base_path / "sf1_variables.dat"

        self._file = f
        self._load_dataframe()


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
        super(Sf3, self).__init__(features, year, apikey, 'sf3')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Sf3 servers and set it to feature
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
        variables = self.check_variables(variables, Sf3Defaults())
        super(Sf3, self).get_data(level=level, variables=variables,
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
        super(Sf1, self).__init__(features, year, apikey, 'sf1')

    def get_data(self, level='finest', variables=(), retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from the Sf3 servers and set it to feature
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
        variables = self.check_variables(variables, Sf1Defaults())
        super(Sf1, self).get_data(level=level, variables=variables,
                                  retry=retry, verbose=verbose,
                                  multiproc=multiproc,
                                  multithread=multithread,
                                  thread_pool=thread_pool,
                                  use_cache=use_cache)
