import requests
from .tigerweb import TigerWebVariables
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
    def __init__(self, features, year, apikey, server):
        if not isinstance(features, dict):
            raise TypeError('features must be in a dictionary format')
        self._features = features
        self._year = year
        self.__apikey = apikey
        self._text = server

        if server == 'acs5':
            self._server = Acs5Server
        elif server == 'acs1':
            self._server = Acs1Server
        else:
            raise AssertionError("unknown server type")

        self.__level_dict = {1: 'state', 2: 'county',
                            3: 'tract', 4: 'block_group'}

        self._features_level = 'undefined'


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
        return self._features

    @property
    def feature_names(self):
        """
        Method to return the feature names
        """
        return list(self._features.keys())

    @property
    def features_level(self):
        """
        Minimum common level (maximum resolution) of all the
        features supplied to Acs

        """
        return self._features_level

    def __determine_discretization(self):
        """
        Internal method to determine a common 'finest' discretization
        of all supplied featues

        """
        level = []
        for name in self.feature_names:
            for feature in self.features[name]:
                tmp = 0
                for key in (TigerWebVariables.state,):
                    if key in feature.properties:
                        tmp = 1
                    else:
                        raise AssertionError("State number must be in "
                                             "properties dict")
                if tmp == 1:
                    for key in (TigerWebVariables.county,
                                TigerWebVariables.state):
                        if key in feature.properties:
                            tmp = 2
                        else:
                            tmp = 1
                            break

                if tmp == 2:
                    for key in (TigerWebVariables.tract,
                                TigerWebVariables.state,
                                TigerWebVariables.county):
                        if key in feature.properties:
                            tmp = 3
                        else:
                            tmp = 2
                            break
                if tmp == 3:
                    for key in (TigerWebVariables.blkgrp,
                                TigerWebVariables.state,
                                TigerWebVariables.county,
                                TigerWebVariables.tract):
                        if key in feature.properties:
                            tmp = 4
                        else:
                            tmp = 3
                            break

                level.append(tmp)

        self._features_level = self.__level_dict[min(level)]

    def get_data(self, level='finest', variables=()):
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
            the Acs5Variables class

        """
        url = self._server.base.format(self.year)

        lut = None
        if level == 'finest':
            for level in self._server.levels:
                lut = self._server.__dict__[level]
                if self.year in lut:
                    break
                else:
                    lut = None

            if level != self.features_level:
                level = self.features_level
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

            if self.__level_dict[level] > \
                    self.__level_dict[self.features_level]:
                raise AssertionError("Cannot grab level data finer than {}"
                                     .format(self.features_level))

        if lut is None:
            raise KeyError("No {} server could be found for {} and {}"
                           .format(self._text, self.year, level))

        if variables:
            if isinstance(variables, str):
                variables = (variables,)
            variables = ",".join(variables)
        else:
            variables = lut[self.year]["variables"]

        fmt = lut[self.year]['fmt']

        for name in self.feature_names:
            for featix, feature in enumerate(self.features[name]):
                loc = ""
                if level == "block_group":
                    loc = fmt.format(
                        feature.properties[TigerWebVariables.blkgrp],
                        feature.properties[TigerWebVariables.state],
                        feature.properties[TigerWebVariables.county],
                        feature.properties[TigerWebVariables.tract])
                elif level == "tract":
                    loc = fmt.format(
                        feature.properties[TigerWebVariables.tract],
                        feature.properties[TigerWebVariables.state],
                        feature.properties[TigerWebVariables.county])
                elif level == "county":
                    loc = fmt.format(
                        feature.properties[TigerWebVariables.county],
                        feature.properties[TigerWebVariables.state])
                elif level == "state":
                    loc = fmt.format(
                        feature.properties[TigerWebVariables.state])
                else:
                    raise AssertionError("level is undefined")

                s = requests.session()

                payload = {'get': "NAME," + variables,
                           'for': loc,
                           'key': self.__apikey}

                payload = "&".join(
                    '{}={}'.format(k, v) for k, v in payload.items())
                r = s.get(url, params=payload)
                r.raise_for_status()
                print('Getting data for {} feature # {}'.format(name, featix))

                data = r.json()
                if len(data) == 2:
                    for dix, header in enumerate(data[0]):
                        if header == "NAME":
                            continue
                        elif header not in variables:
                            continue
                        else:
                            self._features[name][featix].properties[header] = \
                                float(data[1][dix])


class Acs5(AcsBase):
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

    """
    def __init__(self, features, year, apikey):
        super(Acs5, self).__init__(features, year, apikey, 'acs5')

    def get_data(self, level='finest', variables=()):
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
            the Acs5Variables class

        """
        super(Acs5, self).get_data(level=level, variables=variables)