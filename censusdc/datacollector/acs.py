import requests
from ..utils import Acs5Server, Acs1Server


class Acs5Variables(object):
    """
    Small listing of common census variable names for querying data
    """
    population = 'B01003_001E'
    households = 'B11005_001E'
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


class AcsBase(object):
    """
    Base class for Acs1 and Acs5 data
    """
    def __init__(self):
        pass


class Acs5(object):
    """
    Class to collect data from the Acs5 census using geojson features
    from TigerWeb

    Parameters
    ----------
    features: dict
        features from TigerWeb data collections
        {polygon_name: [geojson, geojson,...]}

    """
    def __init__(self, features,):
        self._features = features