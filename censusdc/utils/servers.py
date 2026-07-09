

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __2000 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/tigerWMS_Census2000/MapServer'
    __2010 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/tigerWMS_Census2010/MapServer'
    __2020 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/Tracts_Blocks/MapServer'
    __2020pl = "https://tigerweb.geo.census.gov/arcgis/rest/services/" + \
               "Census2020/Places_CouSub_ConCity_SubMCD/MapServer"

    levels = ('block', 'block_group', 'tract', 'place',
              'county_subdivision', 'county')

    base = {
        1990: __2000,
        2000: __2000,
        2010: __2010,
        2020: __2020
    }

    place_base = {
        2000: __2000,
        2010: __2010,
        2020: __2020pl
    }

    cousub_base = {
        2000: __2000,
        2010: __2010,
        2020: __2020
    }

    cobase = {
        2000: __2000,
        2010: __2010,
        2020: __2020
    }

    __county_subdivision = 'GEOID,STATE,COUNTY,COUSUB,AREALAND,AREAWATER'
    county_subdivision = {
        2000: {"mapserver": 20,
               "outFields": __county_subdivision},
        2010: {"mapserver": 24,
               "outFields": __county_subdivision},
        2020: {"mapserver": 20,
               "outFields": __county_subdivision},
    }

    __county = 'GEOID,STATE,COUNTY,AREALAND,AREAWATER'
    county = {
        2000: {"mapserver": 74,
               "outFields": __county},
        2010: {"mapserver": 90,
               "outFields": __county},
        2020: {"mapserver": 82,
               "outFields": __county},
    }

    __place = 'GEOID,STATE,PLACE,AREALAND,AREAWATER'
    place = {2000: {'mapserver': [28, 26],
                    'outFields': __place},
             2010: {'mapserver': [32, 30],
                    'outFields': __place},
             2020: {'mapserver': [4, 5],
                    'outFields': __place}
        ,}

    __dec_tract = 'GEOID,STATE,COUNTY,TRACT,AREAWATER'
    __acs_tract = 'GEOID,STATE,COUNTY,TRACT,AREALAND'
    tract = {1990: {'mapserver': 6,
                    'outFields': __dec_tract},
             2000: {'mapserver': 8,
                    'outFields': __dec_tract},
             2010: {'mapserver': 10,
                    'outFields': __dec_tract},
             2020: {'mapserver': 0,
                    'outFields': __dec_tract}}

    __dec_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    __acs_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    block_group = {2000: {'mapserver': 10,
                          'outFields': __dec_blkgrp},
                   2010: {'mapserver': 12,
                          'outFields': __dec_blkgrp},
                   2020: {'mapserver': 1,
                          'outFields': __dec_blkgrp}}

    __dec_block = 'GEOID,BLOCK,BLKGRP,STATE,COUNTY,TRACT,' \
                  'AREALAND,AREAWATER'
    block = {2000: {'mapserver': 10,
                    'outFields': __dec_block},
             2010: {'mapserver': 14,
                    'outFields': __dec_block},
             2020: {'mapserver': 2,
                    'outFields': __dec_block}}


def identify_census_discretization(geoid):
    """
    Method to identify the specific census discretization level
    by GEOID size

    Parameters
    ----------
    geoid : str
        U.S. Census Bureau GeoId

    Returns
    -------
        str: census level (e.g., "tract")
    """
    # Note as the census data collector grows this may need to be changed/replaced
    #  to handle other geographies that may have similar geoid lengths. One way could be
    #  to tag tigerweb records with a geography key.
    geoid = str(geoid)
    geoid_len = len(geoid)
    if geoid_len == 2:
        level = "state"
    elif geoid_len == 5:
        level = "county"
    elif geoid_len == 7:
        level = "place"
    elif geoid_len == 10:
        level = "county subdivision"
    elif geoid_len == 11:
        level = "tract"
    elif geoid_len == 12:
        level = "block group"
    elif geoid_len >= 15:
        level = "block"
    else:
        raise NotImplementedError(
            "Cannot determine census discretization from the geoid length"
        )
    return level


def get_format_str(geography):
    """
    Method to get the geography formatting string for census API data pulls

    Parameters
    ----------
    geography : str
        census geography level

    Returns
    -------
        str: formatting string
    """
    formatters = {
        "state": "state:{}",
        "county": "county:{}&in=state:{}",
        "place": "place:{}&in=state:{}",
        "tract": "tract:{}&in=state:{}&in=county:{}",
        "block_group": "block%20group:{}&in=state:{}&in=county:{}&in=tract:{}"
    }
    if geography not in formatters:
        raise NotImplementedError(
            f"Census geography data pull formatter has not been implemented for: {geography}"
        )
    return formatters[geography]


def get_cache_format_str(geography):
    """
    Method to get the geography formatting string for census API data pulls to build
    the cache.

    Parameters
    ----------
    geography : str
        census geography level

    Returns
    -------
        str: formatting string
    """
    formatters = {
        "state": "state:{}",
        "county": "county:*&in=state:{}",
        "place": "place:*&in=state:{}",
        "tract": "tract:*&in=state:{}",
        "block group": "block%20group:*&in=state:{}&in=county:*&in=tract:*"
    }
    if geography not in formatters:
        raise NotImplementedError(
            f"Census geography data pull formatter has not been implemented for: {geography}"
        )
    return formatters[geography]


def get_base_url(dataset, year):
    """
    Method to get the base url for a census data product

    Parameters
    ----------
    dataset : str
        dataset string
    year : int
        year

    Returns
    -------
        str
    """
    from ..datacollector.data_discovery import get_supported_products
    # get cached data products dataframe
    products = get_supported_products()
    dsrec = products[(products["dataset"] == dataset) & (products["vintage"] == year)]
    base_url = "/".join(dsrec["geographyLink"].values[0].split("/")[:-1])
    return base_url


'''
class Sf3Server(object):
    """
    Class to store map server information for Decennial Sf3 census data queries
    """
    base = "https://api.census.gov/data/{}/dec/sf3"

    levels = ("block_group", "block", "tract", "place", "county", "state")

    __income = ["P0520{:02d}".format(i) for i in range(1, 18)]
    __variables = "P001001," + ",".join(__income) + ",HCT012001" + ",H035001"

    __income90 = ["P08000{:02d}".format(i) for i in range(1, 25)]
    __variables90 = "P0010001," + ",".join(__income90) + ",P080A001"

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in (1990, 2000)}
    state = state(__variables)
    state[1990]["variables"] = __variables90

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in (1990, 2000)}
    county = county(__variables)
    county[1990]["variables"] = __variables90

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in (1990, 2000)}
    tract = tract(__variables)
    tract[1990]["variables"] = __variables90

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in (1990, 2000)}
    cache_tract = cache_tract(__variables)
    cache_tract[1990]['variables'] = __variables90

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2000,)}
    block_group = block_group(__variables)

    def cache_block_group(variables):
        return {i: {"fmt":
                    'block%20group:*&in=state:{}&in=county:{}&in=tract:*',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2000, 2010)}
    cache_block_group = cache_block_group(__variables)


class Sf1Server(object):
    """
    Class to store map server information for Decennial Sf1 census data queries
    """
    base = "https://api.census.gov/data/{}/dec/sf1"

    levels = ("block", "block_group", "tract", "county", "state")
    __variables = "P001001,P015001" #  population & households

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in (2000, 2010)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in (2000, 2010)}
    county = county(__variables)

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}", "variables": variables}
                for i in (2000, 2010)}
    place = place(__variables)

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}", "variables": variables}
                for i in (2000, 2010)}
    cache_place = cache_place(__variables)

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in (2000, 2010)}
    tract = tract(__variables)

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in (2000, 2010)}
    cache_tract = cache_tract(__variables)

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2000, 2010)}
    block_group = block_group(__variables)

    def cache_block_group(variables):
        return {i: {"fmt":
                    'block%20group:*&in=state:{}&in=county:*&in=tract:*',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2000, 2010)}
    cache_block_group = cache_block_group(__variables)


class Acs5Server(object):
    """
    Class to store map server information for ACS5 census data queries
    """
    base = "https://api.census.gov/data/{}/acs/acs5?"

    levels = ("block_group", "tract", "place", "county", "state")

    _gini = "B19083_001E"
    __income = ["B19001_0{:02d}E".format(i) for i in range(1,18)]
    __house_age = ["B25034_0{:02d}E".format(i) for i in range(1, 11)]
    __variables = "B01003_001E," + ",".join(__income) + ",B19013_001E," + \
                  ",".join(__house_age) + ",B25035_001E"

    __variables2 = __variables + "," + _gini

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2009, 2022)}
    state = state(__variables)

    for i in range(2010, 2022):
        state[i]['variables'] = __variables2

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2009, 2022)}
    county = county(__variables)

    for i in range(2010, 2022):
        county[i]['variables'] = __variables2

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in range(2009, 2022)}
    tract = tract(__variables)

    for i in range(2010, 2022):
        tract[i]['variables'] = __variables2

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2022)}
    cache_tract = cache_tract(__variables)

    for i in range(2010, 2022):
        cache_tract[i]['variables'] = __variables2

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}",
                    "variables": variables} for i in range(2009, 2022)}

    place = place(__variables)

    for i in range(2010, 2022):
        place[i]['variables'] = __variables2

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2022)}

    cache_place = cache_place(__variables)

    for i in range(2010, 2022):
        cache_place[i]['variables'] = __variables2

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021)}
    block_group = block_group(__variables)

    for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021):
        block_group[i]['variables'] = __variables2

    def cache_block_group(variables):
        return {i: {"fmt":
                    'block%20group:*&in=state:{}&in=county:*&in=tract:*',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021)}
    cache_block_group = cache_block_group(__variables)

    for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021):
        cache_block_group[i]['variables'] = __variables2


class Acs5SummaryServer(object):
    base = "https://api.census.gov/data/{}/acs5?"

    levels = ("block_group", "tract", "place", "county", "state")

    _gini = "B19083_001E"

    __variables = _gini

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2009, 2010)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2009, 2010)}
    county = county(__variables)

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in range(2009, 2010)}
    tract = tract(__variables)

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2010)}
    cache_tract = cache_tract(__variables)

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}",
                    "variables": variables} for i in range(2009, 2010)}
    place = place(__variables)

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2010)}
    cache_place = cache_place(__variables)

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020)}
    block_group = block_group(__variables)

    def cache_block_group(variables):
        return {i: {"fmt":
                    'block%20group:*&in=state:{}&in=county:*&in=tract:*',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020)}
    cache_block_group = cache_block_group(__variables)


class Acs5ProfileServer(object):
    base = "https://api.census.gov/data/{}/acs/acs5/profile?"

    levels = ("block_group", "tract", "place", "county", "state")

    __employment = "DP03_0001E"
    __occupation = ["DP03_00{}E".format(i) for i in range(26, 33)]
    __industry = ["DP03_00{}E".format(i) for i in range(33, 47)]
    __ed_attain = ["DP02_00{}E".format(i) for i in range(58, 66)]

    __variables = __employment + "," + ",".join(__occupation) + \
                  "," + ",".join(__industry) + "," + ",".join(__ed_attain)

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2009, 2021)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2009, 2021)}
    county = county(__variables)

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in range(2009, 2021)}
    tract = tract(__variables)

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2021)}
    cache_tract = cache_tract(__variables)

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}",
                    "variables": variables} for i in range(2009, 2021)}
    place = place(__variables)

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}",
                    "variables": variables} for i in range(2009, 2021)}
    cache_place = cache_place(__variables)

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2013, 2014, 2015, 2017, 2018, 2019, 2020)}
    block_group = block_group(__variables)

    def cache_block_group(variables):
        return {i: {"fmt":
                    'block%20group:*&in=state:{}&in=county:{}&in=tract:{}',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2013, 2014, 2015, 2017, 2018, 2019, 2020)}
    cache_block_group = cache_block_group(__variables)


class Acs1Server(object):
    """
    Class to store map server information for ACS1 census data queries
    """
    base = "https://api.census.gov/data/{}/acs/acs1?"

    levels = ("county_subdivision", "place", "county", "state")

    __income = ["B19001_0{:02d}E".format(i) for i in range(1, 18)]
    __house_age = ["B25034_0{:02d}E".format(i) for i in range(1, 11)]
    __variables = "B01003_001E," + ",".join(__income) + ",B19013_001E," + \
                  ",".join(__house_age) + ",B25035_001E"

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2005, 2019)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}
    county = county(__variables)

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}
    place = place(__variables)

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}
    cache_place = cache_place(__variables)

    def county_subdivision(variables):
        return {i: {"fmt": "county%20subdivision:{}&in=state:{}&in=county:{}",
                    "variables": variables}
                for i in range(2005, 2019)}
    county_subdivision = county_subdivision(__variables)


class Acs1ProfileServer(object):
    """
    Class to store map server information for ACS1 census profile data queries
    """
    base = "https://api.census.gov/data/{}/acs/acs1/profile?"

    levels = ("block_group", "tract", "place", "county", "state")

    __employment = "DP03_0001E"
    __occupation = ["DP03_00{}E".format(i) for i in range(26, 33)]
    __industry = ["DP03_00{}E".format(i) for i in range(33, 47)]

    __variables = __employment + "," + ",".join(__occupation) + \
                  "," + ",".join(__industry)

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2005, 2019)}

    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}

    county = county(__variables)

    def place(variables):
        return {i: {"fmt": "place:{}&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}

    place = place(__variables)

    def cache_place(variables):
        return {i: {"fmt": "place:*&in=state:{}",
                    "variables": variables}
                for i in range(2005, 2019)}

    cache_place = cache_place(__variables)

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in range(2005, 2019)}

    tract = tract(__variables)

    def cache_tract(variables):
        return {i: {"fmt": "tract:*&in=state:{}",
                    "variables": variables} for i in range(2005, 2019)}

    cache_tract = cache_tract(__variables)

    def cache_block_group(variables):
        return {i: {"fmt":
                        'block%20group:*&in=state:{}&in=county:{}&in=tract:{}',
                    "fmt_co": "county:*&in=state:{}",
                    "variables": variables}
                for i in (2013, 2014, 2015, 2017, 2018, 2019)}

    cache_block_group = cache_block_group(__variables)
'''