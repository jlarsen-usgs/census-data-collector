from .cbase import CensusBase


class Sf3Variables(object):
    """
    Small listing of common census variable names for querying data
    """
    population = "P001001"
    households = "P052001"
    income_lt_10k = "P052002"
    income_10K_15k = "P052003"
    income_15k_20k = "P052004"
    income_20k_25k = "P052005"
    income_25k_30k = "P052006"
    income_30k_35k = "P052007"
    income_35k_40k = "P052008"
    income_40k_45k = "P052009"
    income_45k_50k = "P052010"
    income_50k_60k = "P052011"
    income_60k_75k = "P052012"
    income_75k_100k = "P052013"
    income_100k_125k = "P052014"
    income_125k_150k = "P052015"
    income_150k_200k = "P052016"
    income_gt_200k = "P052017"


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

    def get_data(self, level='finest', variables=(), retry=100):
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

        """
        super(Sf3, self).get_data(level=level, variables=variables,
                                  retry=retry)