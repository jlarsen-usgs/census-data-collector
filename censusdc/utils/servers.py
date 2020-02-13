

class TigerWebMapServer(object):
    """
    Class to store map server information for TigerWeb queries
    """
    __gacs = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'Generalized_ACS{}/Tracts_Blocks/MapServer'
    __base = 'https://tigerweb.geo.census.gov/arcgis/rest/services/' + \
             'TIGERweb/Tracts_Blocks/MapServer'

    levels = ('block', 'block_group', 'tract')

    base = {2010: __base,
            2011: __gacs.format(2015),
            2012: __gacs.format(2015),
            2013: __gacs.format(2015),
            2014: __gacs.format(2015),
            2015: __gacs.format(2015),
            2016: __gacs.format(2016),
            2017: __gacs.format(2017),
            2018: __gacs.format(2018),
            2019: __base}

    tract = {2010: {'mapserver': 10,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT,POP100'},
             2011: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2012: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2013: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2014: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2015: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2016: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2017: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2018: {'mapserver': 3,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'},
             2019: {'mapserver': 4,
                    'outFields': 'GEOID,STATE,COUNTY,TRACT'}}

    block_group = {2010: {'mapserver': 11,
                          'outFields': 'GEOID,BLKGRP,STATE,'
                                       'COUNTY,TRACT,POP100'},
                   2013: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2014: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2015: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2016: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2017: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2018: {'mapserver': 4,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
                   2019: {'mapserver': 5,
                          'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'}}

    block = {2010: {'mapserver': 12,
                    'outFields': 'GEOID,BLOCK,BLKGRP,STATE,'
                                 'COUNTY,TRACT,POP100'},}