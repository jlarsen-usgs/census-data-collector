from .cbase import CensusBase
from ..defaults.census_defaults import CensusDefaults


class CensusApi(CensusBase):
    """
    Generalized class to collect data from any supported Census datasets
    using geopandas features from TigerWeb

    Parameters
    ----------
    features: GeoDataFrame
        GeoDataFrame of features from TigerWeb data collections
    year : int
        census data year to pull data
    dataset : str
        census data product string. Examples: "acs-acs5", "dec-sf3".
        The censusdc.data_discovery module's get_supported_products() method
        returns a dataframe listing currently supported datasets
    apikey : str
        users specific census apikey (obtained from
        https://api.census.gov/data/key_signup.html)

    """
    def __init__(self, features, year, dataset, apikey):
        super(CensusApi, self).__init__(features, year, apikey, dataset)


    def get_data(self, variables=(), validate_variables=False, retry=100, verbose=True,
                 multiproc=False, multithread=False, thread_pool=4,
                 use_cache=False):
        """
        Method to get data from census api servers and set it to feature
        properties

        Parameters
        ----------
        variables : list, tuple
            user specified Acs1 variables, default pulls variables from
            the AcsVariables class
        validate_variables : bool
            method to validate user provided variables. Useful when
            performing a new data pull to check variable codes against valid ones
            for the census data product.
        retry : int
            number of retries for HTTP connection issues before failure
        verbose : bool
            verbose operation mode
        multithread : bool
            boolean flag to allow multithreading of data collection
        multiproc : bool
            multiprocessing support using ray; useful for performing large data pulls
            on a cluster system
        thread_pool : int
            number of CPU threads to use during multithread operations
        use_cache : bool
            method to prefer cached census data over pulling data from the census api
            at run time. This speeds up data collection time considerably and should be
            used whenever possible.
        """
        variables = self.check_variables(
            variables, CensusDefaults(self._dataset), validate=validate_variables
        )
        super(CensusApi, self).get_data(
            variables=variables,
            retry=retry,
            verbose=verbose,
            multiproc=multiproc,
            multithread=multithread,
            thread_pool=thread_pool,
            use_cache=use_cache
        )