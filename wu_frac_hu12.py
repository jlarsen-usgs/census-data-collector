import shapefile
from censusdc.utils import CensusTimeSeries, create_filter
from censusdc.utils import GeoFeatures
from censusdc.utils import geojson_to_shapefile
import os
import time
import pickle
import requests
import numpy as np
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError

try:
    import ray
except:
    ray = None


def load_pickle(f):
    """
    Method to load a pickle object for restart mode

    Parameters
    ----------
    f : str
        pickle file
    Returns
    -------
        dict : dictionary of features
    """
    with open(f, 'rb') as foo:
        data = pickle.load(foo)
    return data


def clean_restart_data(census_filter, features):
    """
    Method to clean the filter in restart mode

    Parameters
    ----------
    filter : tuple
        tuple of feature names
    features : dict
        dictionary data that has previously been collected

    Returns
    -------
        tuple : cleaned filter containing features that have not yet been
                collected
    """
    census_filter = list(census_filter)
    for k in geofeats.keys():
        if k in census_filter:
            idx = census_filter.index(k)
            census_filter.pop(idx)

    return tuple(census_filter)


if __name__ == "__main__":
    restart = True
    ws = os.path.abspath(os.path.dirname(__file__))
    if ray is not None:
        ray.init(address='auto')

    strt = time.time()
    allhuc2 = ["{:02d}".format(h) for h in range(22, 23)]
    for huc2 in allhuc2:
        start_time = time.time()

        huc12_shapes = os.path.join(ws, 'huc12_export', 'huc2_{}.shp'.format(huc2))
        pickle_file = os.path.join(ws, "huc12_output_shapefiles",
                                   "huc{}.pickle".format(huc2))
        out_shapes = os.path.join(ws, "huc12_output_shapefiles",
                                  "huc{}_censusdc.shp".format(huc2))
        out_path = os.path.join(ws, "huc12_output", 'huc{}'.format(huc2))

        api_key = os.path.join(ws, 'api_key.dat')
        with open(api_key) as foo:
            api_key = foo.readline().strip()

        cfilter = create_filter(huc12_shapes, {"huc2" : [huc2]},
                                'huc12')

        chunksize = 25
        if huc2 == "04":
            chunksize = 3

        chunk0 = 0

        if restart and os.path.isfile(pickle_file):
            geofeats = load_pickle(pickle_file)
            cfilter = clean_restart_data(cfilter, geofeats)
        else:
            geofeats = {}

        while chunk0 < len(cfilter):
            chunk1 = chunk0 + chunksize
            if chunk1 > len(cfilter):
                nfilter = cfilter[chunk0:]
            else:
                nfilter = cfilter[chunk0:chunk1]

            chunk0 += chunksize

            ts = CensusTimeSeries(huc12_shapes, api_key, field="huc12", filter=nfilter)
            # years = ts.available_years[0:2] + ts.available_years[8:]
            years = (2015, )

            for feature in nfilter:
                df = ts.get_timeseries(feature, verbose=2, multithread=True,
                                       thread_pool=16, multiproc=False,
                                       years=years)

                #df.to_csv(os.path.join(out_path, "{}_yearly.csv".format(feature)),
                #          index=False)
                if 'population' not in list(df):
                    df['population'] = [np.nan] * len(df)
                temp = GeoFeatures.compiled_feature(2015, ts.get_shape(feature),
                                                    feature, df=df)
                geofeats[feature] = temp

                #try:
                #    df2 = CensusTimeSeries.interpolate(df,
                #                                       min_extrapolate=1989,
                #                                       max_extrapolate=2021,
                #                                       kind='slinear',
                #                                       discretization='daily')
                #    df2.to_csv(os.path.join(out_path, "{}.csv".format(feature)),
                #               index=False)
                #except:
                #    pass

            if restart:
                print("Writing huc{} Pickle object".format(huc2))
                with open(pickle_file, "wb") as foo:
                    pickle.dump(geofeats, foo)

        geofeats = [v for k, v in geofeats.items()]
        geojson_to_shapefile(out_shapes, geofeats)

        prjname = out_shapes[:-4] + ".prj"
        with open(prjname, 'w') as prj:
            prj.write('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
                      'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
                      'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]')

        end_time = time.time()
        print(end_time - start_time)

    end_time = time.time()
    print(end_time - strt)
