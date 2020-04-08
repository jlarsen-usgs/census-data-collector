

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __gacs = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Generalized_ACS{}/Tracts_Blocks/MapServer'
    __acs1 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer'
    __base = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Tracts_Blocks/MapServer'
    __2000 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2010/tigerWMS_Census2000/MapServer'

    levels = ('block', 'block_group', 'tract', 'county_subdivision')

    base = {2000: __2000,
            2005: __acs1,
            2006: __acs1,
            2007: __acs1,
            2008: __acs1,
            2009: __acs1,
            2010: __base,
            2011: __gacs.format(2015),
            2012: __gacs.format(2015),
            2013: __gacs.format(2015),
            2014: __gacs.format(2015),
            2015: __gacs.format(2015),
            2016: __gacs.format(2016),
            2017: __gacs.format(2017),
            2018: __gacs.format(2018)}
            # 2019: __base}

    def lcdbase(acs1):
        return {i: acs1 for i in range(2005, 2019)}
    lcdbase = lcdbase(__acs1)

    __acs1_county_subdivision = 'GEOID,STATE,COUNTY,COUSUB,AREALAND,AREAWATER'
    __acs1_server = {'mapserver': 22,
                     'outFields': __acs1_county_subdivision}

    def county_subdivision(acs1_server):
        return {i: acs1_server for i in range(2005, 2019)}
    county_subdivision = county_subdivision(__acs1_server)

    __dec_tract = 'GEOID,STATE,COUNTY,TRACT,POP100,AREAWATER'
    __acs_tract = 'GEOID,STATE,COUNTY,TRACT,AREALAND'
    tract = {2000: {'mapserver': 6,
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
                    'outFields': __acs_tract}}
             # 2019: {'mapserver': 4,
             #        'outFields': __acs_tract}}

    __dec_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,POP100,AREALAND,AREAWATER'
    __acs_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,AREALAND,AREAWATER'
    block_group = {2000: {'mapserver': 10,
                          'outFields': __dec_blkgrp},
                   2010: {'mapserver': 11,
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
                          'outFields': __acs_blkgrp}}
                   # 2019: {'mapserver': 5,
                   #        'outFields': __acs_blkgrp}}

    __dec_block = 'GEOID,BLOCK,BLKGRP,STATE,COUNTY,TRACT,POP100,' \
                  'AREALAND,AREAWATER'
    block = {2000: {'mapserver': 10,
                    'outFields': __dec_block},
             2010: {'mapserver': 12,
                    'outFields': __dec_block}}


class Acs5Server(object):
    """
    Class to store map server information for ACS5 census data queries
    """
    base = "https://api.census.gov/data/{}/acs/acs5?"

    levels = ("block_group", "tract", "county", "state")

    __income = ["B19001_0{:02d}E".format(i) for i in range(1,18)]
    __variables = "B01003_001E," + ",".join(__income)

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2009, 2019)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2009, 2019)}
    county = county(__variables)

    def tract(variables):
        return {i: {"fmt": "tract:{}&in=state:{}&in=county:{}",
                    "variables": variables} for i in range(2009, 2019)}
    tract = tract(__variables)

    def block_group(variables):
        return {i: {"fmt":
                    'block%20group:{}&in=state:{}&in=county:{}&in=tract:{}',
                    "variables": variables}
                for i in (2013, 2014, 2015, 2017, 2018,)}
    block_group = block_group(__variables)


class Acs1Server(object):
    """
    Class to store map server information for ACS1 census data queries
    """
    base = "https://api.census.gov/data/{}/acs/acs1?"

    levels = ("county_subdivision", "county", "state")

    __income = ["B19001_0{:02d}E".format(i) for i in range(1, 18)]
    __variables = "B01003_001E," + ",".join(__income)

    def state(variables):
        return {i: {"fmt": "state:{}", "variables": variables}
                for i in range(2005, 2019)}
    state = state(__variables)

    def county(variables):
        return {i: {"fmt": "county:{}&in=state:{}", "variables": variables}
                for i in range(2005, 2019)}
    county = county(__variables)

    def county_subdivision(variables):
        return {i: {"fmt": "county%20subdivision:{}&in=state:{}%20county:{}",
                    "variables": variables}
                for i in range(2005, 2019)}
    county_subdivision = county_subdivision(__variables)
