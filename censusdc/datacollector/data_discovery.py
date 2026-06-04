import difflib

import requests
import pandas as pd
import difflib
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError

# TODO:
# 1) get supported census products
# 2) get variables for specified census product
# 3) get geographies for specified census product


def get_supported_products():
    """

    Returns
    -------

    """
    s = requests.session()
    n = 0
    while n < 100:
        try:
            r = s.get("https://api.census.gov/data.json")
            r.raise_for_status()
            break

        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            err = e
            n += 1

    try:
        data = r.json()
        data = data['dataset']

        df = pd.DataFrame(data)
        df.columns = df.columns.str.removeprefix('c_')
        df = df[['vintage', 'dataset', 'title', 'description', 'geographyLink', 'variablesLink']].reset_index(drop=True)

        df['dataset'] = df['dataset'].apply(lambda x: '-'.join(x))
        supported_products = ['acs-acs1', 'acs-acs3', 'acs-acs5', 'acs-acs5-profile', 'dec-sf1', 'dec-sf3']
        df_supported = df[df['dataset'].isin(supported_products)].reset_index(drop=True)
        df_supported['vintage'] = df_supported['vintage'].round().astype(int)

    except JSONDecodeError:
        raise("Cannot connect to U.S. Census API")

    return df_supported



def get_variables(df_supported, dataset, year):

    # TODO: make this work for multiple and/or all years

    # fuzzy match of dataset name
    dataset_match = difflib.get_close_matches(dataset, df_supported['dataset'], n=1, cutoff=0.01)

    # get variable link
    mask = (df_supported['dataset'] == dataset_match[0]) & (df_supported['vintage'] == year)
    variables_link = df_supported.loc[mask, 'variablesLink'].values[0]

    # request data
    s = requests.session()
    n = 0
    while n < 100:
        try:
            r = s.get(variables_link)
            r.raise_for_status()
            break

        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            err = e
            n += 1

    try:
        data = r.json()
        data = data['variables']

        df_variables = pd.DataFrame(data)  # TODO: why is this not formatted properly?

    except JSONDecodeError:
        raise("Cannot connect to U.S. Census API")

    return df_variables




def get_geographies(df_supported, dataset, year):

    # TODO: make this work for multiple and/or all years

    # fuzzy match of dataset name
    dataset_match = difflib.get_close_matches(dataset, df_supported['dataset'], n=1, cutoff=0.01)

    # get geography link
    mask = (df_supported['dataset'] == dataset_match[0]) & (df_supported['vintage'] == year)
    geography_link = df_supported.loc[mask, 'geographyLink'].values[0]

    # request data
    s = requests.session()
    n = 0
    while n < 100:
        try:
            r = s.get(geography_link)
            r.raise_for_status()
            break

        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            err = e
            n += 1

    try:
        data = r.json()
        data = data['fips']

        df_geographies = pd.DataFrame(data)
        # TODO: reformat this

    except JSONDecodeError:
        raise("Cannot connect to U.S. Census API")

    return df_geographies
