

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __gacs = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Generalized_ACS{}/Tracts_Blocks/MapServer'
    __gacspl = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' \
               'Generalized_ACS{}/Places_CouSub_ConCity_SubMCD/MapServer'
    __acs1 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer'
    __acs1co = "https://tigerweb.geo.census.gov/arcgis/rest/services/" + \
               "TIGERweb/State_County/MapServer/"
    __base = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Tracts_Blocks/MapServer'
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

    base = {1990: __2000,
            2000: __2000,
            2005: __acs1,
            2006: __acs1,
            2007: __acs1,
            2008: __acs1,
            2009: __2000,
            2010: __2010,
            2011: __gacs.format(2015),
            2012: __gacs.format(2015),
            2013: __gacs.format(2015),
            2014: __gacs.format(2015),
            2015: __gacs.format(2015),
            2016: __gacs.format(2016),
            2017: __gacs.format(2017),
            2018: __gacs.format(2018),
            2019: __gacs.format(2019),
            2020: __2020}

    place_base = {2000: __2000,
                  2005: __acs1,
                  2006: __acs1,
                  2007: __acs1,
                  2008: __acs1,
                  2009: __acs1,
                  2010: __2010,
                  2011: __gacspl.format(2015),
                  2012: __gacspl.format(2015),
                  2013: __gacspl.format(2015),
                  2014: __gacspl.format(2015),
                  2015: __gacspl.format(2015),
                  2016: __gacspl.format(2016),
                  2017: __gacspl.format(2017),
                  2018: __gacspl.format(2018),
                  2019: __gacspl.format(2019),
                  2020: __2020pl}

    def lcdbase(acs1):
        return {i: acs1 for i in range(2005, 2020)}
    lcdbase = lcdbase(__acs1)

    def cobase(acs1):
        return {i: acs1 for i in range(2005, 2020)}
    cobase = cobase(__acs1co)

    __acs1_county = 'GEOID,STATE,COUNTY,AREALAND,AREAWATER'
    __acs1_co_server = {'mapserver': 1,
                        'outFields': __acs1_county}

    def county(acs1_server):
        return {i: acs1_server for i in range(2005, 2020)}
    county = county(__acs1_co_server)

    __acs1_county_subdivision = 'GEOID,STATE,COUNTY,COUSUB,AREALAND,AREAWATER'
    __acs1_server = {'mapserver': 22,
                     'outFields': __acs1_county_subdivision}

    def county_subdivision(acs1_server):
        return {i: acs1_server for i in range(2005, 2020)}
    county_subdivision = county_subdivision(__acs1_server)

    __place = 'GEOID,STATE,PLACE,AREALAND,AREAWATER'
    place = {2000: {'mapserver': [28, 26],
                    'outFields': __place},
             2005: {'mapserver': [26, 25],
                    'outFields': __place},
             2006: {'mapserver': [26, 25],
                    'outFields': __place},
             2007: {'mapserver': [26, 25],
                    'outFields': __place},
             2008: {'mapserver': [26, 25],
                    'outFields': __place},
             2009: {'mapserver': [26, 25],
                    'outFields': __place},
             2010: {'mapserver': [32, 30],
                    'outFields': __place},
             2011: {'mapserver': [11, 10],
                    'outFields': __place},
             2012: {'mapserver': [11, 10],
                    'outFields': __place},
             2013: {'mapserver': [11, 10],
                    'outFields': __place},
             2014: {'mapserver': [11, 10],
                    'outFields': __place},
             2015: {'mapserver': [11, 10],
                    'outFields': __place},
             2016: {'mapserver': [11, 10],
                    'outFields': __place},
             2017: {'mapserver': [11, 10],
                    'outFields': __place},
             2018: {'mapserver': [11, 10],
                    'outFields': __place},
             2019: {'mapserver': [11, 10],
                    'outFields': __place},
             2020: {'mapserver': [18, 17],
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
             2011: {'mapserver': 3,
                    'outFields': __acs_tract},
             2012: {'mapserver': 3,
                    'outFields': __acs_tract},
             2013: {'mapserver': 3,
                    'outFields': __acs_tract},
             2014: {'mapserver': 3,
                    'outFields': __acs_tract},
             2015: {'mapserver': 3,
                    'outFields': __acs_tract},
             2016: {'mapserver': 3,
                    'outFields': __acs_tract},
             2017: {'mapserver': 3,
                    'outFields': __acs_tract},
             2018: {'mapserver': 3,
                    'outFields': __acs_tract},
             2019: {'mapserver': 3,
                    'outFields': __acs_tract},
             2020: {'mapserver': 0,
                    'outFields': __dec_tract}}

    __dec_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    __acs_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    block_group = {2000: {'mapserver': 8,
                          'outFields': __dec_blkgrp},
                   2010: {'mapserver': 12,
                          'outFields': __dec_blkgrp},
                   2013: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2014: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2015: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2017: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2018: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2019: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
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

    levels = ("block_group", "block", "tract", "county", "state")
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