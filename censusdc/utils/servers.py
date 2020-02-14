

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __gacs = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Generalized_ACS{}/Tracts_Blocks/MapServer'
    __base = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Tracts_Blocks/MapServer'
    __2000 = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Census2010/tigerWMS_Census2000/MapServer'

    levels = ('block', 'block_group', 'tract')

    base = {2000: __2000,
            2010: __base,
            2011: __gacs.format(2015),
            2012: __gacs.format(2015),
            2013: __gacs.format(2015),
            2014: __gacs.format(2015),
            2015: __gacs.format(2015),
            2016: __gacs.format(2016),
            2017: __gacs.format(2017),
            2018: __gacs.format(2018),
            2019: __base}

    __dec_tract = 'GEOID,STATE,COUNTY,TRACT,POP100'
    __acs_tract = 'GEOID,STATE,COUNTY,TRACT'
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
                    'outFields': __acs_tract},
             2019: {'mapserver': 4,
                    'outFields': __acs_tract}}

    __dec_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT,POP100'
    __acs_blkgrp = 'GEOID,BLKGRP,STATE,COUNTY,TRACT'
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
                   2016: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2017: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2018: {'mapserver': 4,
                          'outFields': __acs_blkgrp},
                   2019: {'mapserver': 5,
                          'outFields': __acs_blkgrp}}

    __dec_block = 'GEOID,BLOCK,BLKGRP,STATE,COUNTY,TRACT,POP100'
    block = {2000: {'mapserver': 10,
                    'outFields': __dec_block},
             2010: {'mapserver': 12,
                    'outFields': __dec_block}}
