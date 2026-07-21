

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __2000 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/tigerWMS_Census2000/MapServer'
    __2010 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/tigerWMS_Census2010/MapServer'
    __2020 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2020/tigerWMS_Census2020/MapServer'

    geographies = (
        'block',
        'block_group',
        'tract',
        'place',
        'county_subdivision',
        'county'
    )

    base = {
        1990: __2000,
        2000: __2000,
        2010: __2010,
        2020: __2020
    }

    place_base = {
        2000: __2000,
        2010: __2010,
        2020: __2020
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
             2020: {'mapserver': [28, 26],
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
             2020: {'mapserver': 6,
                    'outFields': __dec_tract}}

    __dec_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    __acs_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    block_group = {2000: {'mapserver': 10,
                          'outFields': __dec_blkgrp},
                   2010: {'mapserver': 12,
                          'outFields': __dec_blkgrp},
                   2020: {'mapserver': 8,
                          'outFields': __dec_blkgrp}}

    __dec_block = 'GEOID,BLOCK,BLKGRP,STATE,COUNTY,TRACT,' \
                  'AREALAND,AREAWATER'
    block = {2000: {'mapserver': 10,
                    'outFields': __dec_block},
             2010: {'mapserver': 14,
                    'outFields': __dec_block},
             2020: {'mapserver': 10,
                    'outFields': __dec_block}}


def identify_census_discretization(geoid):
    """
    Method to identify the specific census geography by GEOID size

    Parameters
    ----------
    geoid : str
        U.S. Census Bureau GeoId

    Returns
    -------
        str: census geography (e.g., "tract")
    """
    # Note as the census data collector grows this may need to be changed/replaced
    #  to handle other geographies that may have similar geoid lengths. One way could be
    #  to tag tigerweb records with a geography key.
    geoid = str(geoid)
    geoid_len = len(geoid)
    if geoid_len == 2:
        geography = "state"
    elif geoid_len == 5:
        geography = "county"
    elif geoid_len == 7:
        geography = "place"
    elif geoid_len == 10:
        geography = "county subdivision"
    elif geoid_len == 11:
        geography = "tract"
    elif geoid_len == 12:
        geography = "block group"
    elif geoid_len >= 15:
        geography = "block"
    else:
        raise NotImplementedError(
            "Cannot determine census discretization from the geoid length"
        )
    return geography


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
        "tract": "tract:{}&in=state:{}%20county:{}",
        "block group": "block%20group:{}&in=state:{}%20county:{}%20tract:{}",
        "block": "block:{}&in=state:{}%20county:{}%20tract:{}"
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
        "block group": "block%20group:*&in=state:{}&in=county:*&in=tract:*",
        "block": "block:*&in=state:{}&in=county:{}&in=tract:*"
    }
    if geography not in formatters:
        raise NotImplementedError(
            f"Census geography data pull formatter has not been implemented for: {geography}"
        )
    return formatters[geography]


def get_geography_fmt_str(name):
    """
    Get the specific format for different geography codes for creating GEOIDS.
    e.g., tract = {:06d}

    Parameters
    ----------
    name : str
        geography name

    Returns
    -------
        str : formatter
    """
    from ..datacollector.tigerweb import TigerWebVariables

    name = name.lower()
    if name in ("state", TigerWebVariables.state.lower()):
        fmt = "{:02d}"
    elif name in ("county", TigerWebVariables.county.lower()):
        fmt = "{:03d}"
    elif name in (
            "cousub",
            "county subdivision",
            "county_subdivision",
            TigerWebVariables.cousub.lower(),
            "place",
            TigerWebVariables.place.lower()
    ):
        fmt = "{:05d}"
    elif name in ("tract", TigerWebVariables.tract.lower()):
        fmt = "{:06d}"
    elif name in ("block_group", "block group", TigerWebVariables.blkgrp.lower()):
        fmt = "{:01d}"
    elif name in ("block", TigerWebVariables.block.lower()):
        fmt = "{:04d}"
    else:
        fmt = None
    return fmt


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


