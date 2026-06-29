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


class Sf3Variables1990(object):
    """
    Small listing of common census variable names for querying data
    from the 1990 decennial census
    """
    population = "P0010001"
    income_lt_5k = "P0800001"
    income_5K_10k = "P0800002"
    income_10k_12k = "P0800003"
    income_13k_15k = "P0800004"
    income_15k_17k = "P0800005"
    income_18k_20k = "P0800006"
    income_20k_22k = "P0800007"
    income_22k_25k = "P0800008"
    income_25k_27k = "P0800009"
    income_27k_30k = "P0800010"
    income_30k_32k = "P0800011"
    income_32k_35k = "P0800012"
    income_35k_37k = "P0800013"
    income_37k_40k = "P0800014"
    income_40k_42k = "P0800015"
    income_42k_45k = "P0800016"
    income_45k_47k = "P0800017"
    income_47k_50k = "P0800018"
    income_50K_55k = "P0800019"
    income_55k_60k = "P0800020"
    income_60k_75k = "P0800021"
    income_75k_100k = "P0800022"
    income_100k_125k = "P0800023"
    income_125k_150k = "P0800024"
    income_gt_200k = "P0800025"
    median_income = "P080A001"


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
        super(Sf1, self).get_data(level=level, variables=variables,
                                  retry=retry, verbose=verbose,
                                  multiproc=multiproc,
                                  multithread=multithread,
                                  thread_pool=thread_pool,
                                  use_cache=use_cache)
