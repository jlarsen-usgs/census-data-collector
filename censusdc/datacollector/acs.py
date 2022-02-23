from .cbase import CensusBase


class AcsVariables(object):
    """
    Small listing of common census variable names for querying data
    """
    population = 'B01003_001E'
    households = 'B11001_001E'
    households2 = "B19001_001E"
    income_lt_10k = "B19001_002E"
    income_10K_15k = "B19001_003E"
    income_15k_20k = "B19001_004E"
    income_20k_25k = "B19001_005E"
    income_25k_30k = "B19001_006E"
    income_30k_35k = "B19001_007E"
    income_35k_40k = "B19001_008E"
    income_40k_45k = "B19001_009E"
    income_45k_50k = "B19001_010E"
    income_50k_60k = "B19001_011E"
    income_60k_75k = "B19001_012E"
    income_75k_100k = "B19001_013E"
    income_100k_125k = "B19001_014E"
    income_125k_150k = "B19001_015E"
    income_150k_200k = "B19001_016E"
    income_gt_200k = "B19001_017E"
    median_income = "B19013_001E"
    tot_h_age = "B25034_001E"
    h_age_newer_2005 = "B25034_002E"
    h_age_2000_2004 = "B25034_003E"
    h_age_1990_1999 = "B25034_004E"
    h_age_1980_1989 = "B25034_005E"
    h_age_1970_1979 = "B25034_006E"
    h_age_1960_1969 = "B25034_007E"
    h_age_1950_1959 = "B25034_008E"
    h_age_1940_1949 = "B25034_009E"
    h_age_older_1939 = "B25034_010E"
    median_h_year = "B25035_001E"
    median_p_owner_cost_to_income = "B25092_001E"

    # acs profile data product variables are included below this line
    n_employed = "DP03_0001E"
    n_occupation = "DP03_0026E"
    n_occ_management = "DP03_0027E"
    n_occ_service = "DP03_0028E"
    n_occ_sales_office = "DP03_0029E"
    n_occ_farm_fish_forest = "DP03_0030E"
    n_occ_const_maint_repair = "DP03_0031E"
    n_occ_prod_trans_material = "DP03_0032E"
    n_industry = "DP03_0033E"
    n_ind_ag_forest_fish_mining = "DP03_0034E"
    n_ind_construction = "DP03_0035E"
    n_ind_manufacturing = "DP03_0036E"
    n_ind_wholesale_trade = "DP03_0037E"
    n_ind_retail_trade = "DP03_0038E"
    n_ind_trans_warehouse_utilities = "DP03_0039E"
    n_ind_information = "DP03_0040E"
    n_ind_finance = "DP03_0041E"
    n_ind_prof_sci_admin_waste = "DP03_0042E"
    n_ind_education_healthcare = "DP03_0043E"
    n_ind_arts_entertain_foodservice = "DP03_0044E"
    n_ind_other = "DP03_0045E"
    n_ind_publicadmin = "DP03_0046E"
    n_tot_ed_attain = "DP02_0058E"
    n_lt_ninth_gr = "DP02_0059E"
    n_ninth_to_twelth_gr = "DP02_0060E"
    n_hs_grad = "DP02_0061E"
    n_some_college = "DP02_0062E"
    n_associates = "DP02_0063E"
    n_bachelors = "DP02_0064E"
    n_masters_phd = "DP02_0065E"

    # summary tables variables
    gini = "B19083_001E"


AcsHR = {v: k for k, v in AcsVariables.__dict__.items()
         if not k.startswith("__")}


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
