import requests
import numpy as np
import pandas as pd
import difflib
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError


_SUPPORTED = [
    'acs-acs1',
    'acs-acs3',
    'acs-acs5',
    'acs-acs5-profile',
    'dec-sf1',
    'dec-sf3'
]

_CACHE = {
    "supported": None,
    "geographies": {},
    "variables": {}
}

# specify supported geographies
_GEOGRAPHIES = { 'Sf3': ['state', 'county', 'tract', 'block_group'],
                 'Sf1': ['state', 'county', 'tract', 'place', 'block_group'],
                 'Acs5': ['state', 'county', 'tract', 'place', 'block_group'],
                 'Acs5Summary': ['state', 'county', 'tract', 'place', 'block_group'],
                 'Acs5Profile': ['state', 'county', 'tract', 'place', 'block_group'],
                 'Acs1': ['state', 'county', 'place', 'county_subdivision'],
                 'Acs1Profile': ['state', 'county', 'place', 'tract', 'block_group'],
}

def get_supported_products():
    """

    Returns
    -------
    Dataframe of supported products with the following columns:
        - vintage: dataset year
        - dataset: dataset name
        - title: dataset title
        - description: dataset description
        - geographyLink: link to available geographies for this dataset
        - variablesLink: link to available variables for this dataset

    """
    if _CACHE["supported"] is None:
        s = requests.session()
        n = 0
        while n < 100:
            try:
                r = s.get("https://api.census.gov/data")
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
            df_supported = df[df['dataset'].isin(_SUPPORTED)].reset_index(drop=True)
            df_supported['vintage'] = df_supported['vintage'].round().astype(int)

        except JSONDecodeError:
            raise Exception("Cannot connect to U.S. Census API")

        _CACHE["supported"] = df_supported

    return _CACHE["supported"]


# todo: docstrings!
def get_variables(dataset, year):
    """

    Parameters
    ----------
    dataset: dataset name (formatted in manner provided by get_supported_products())
    year: year to use for dataset query

    Returns
    -------
    Dataframe of available variables for this dataset and year with the following columns:
        - dataset
        - year
        - name
        - label
        - concept
        - predicateType
        - group
        - limit
        - predicateOnly
        - hasGeoCollectionSupport
        - attributes
        - required

    """

    if isinstance(year, (float, str)):
        year = int(year)

    df_supported = get_supported_products()

    use_cache = False
    if dataset in _CACHE["variables"]:
        if year in _CACHE["variables"][dataset]:
            df_variables = _CACHE["variables"][dataset][year]
            use_cache = True

    if not use_cache:

        # fuzzy match of dataset name
        dataset_match = difflib.get_close_matches(dataset, df_supported['dataset'], n=1, cutoff=0.01)

        # get variable link
        mask = (df_supported['dataset'] == dataset_match[0]) & (df_supported['vintage'] == year)
        variables_link = df_supported.loc[mask, 'variablesLink'].values[0]

        # request and reformat data
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

            df_variables = pd.DataFrame.from_dict(data, orient='index')
            df_variables.reset_index(inplace=True, names='name')

            df_variables['year'] = year
            col = df_variables.pop('year')
            df_variables.insert(0, 'year', col)

            df_variables['dataset'] = dataset
            col = df_variables.pop('dataset')
            df_variables.insert(0, 'dataset', col)
            df_variables.sort_values(by=["name"], inplace=True)
            df_variables.reset_index(drop=True, inplace=True)

            df_variables['label'] = df_variables['label'].str.replace(':!!', ', ', regex=False)
            df_variables['label'] = df_variables['label'].str.replace('!!', ', ', regex=False)

            if dataset in _CACHE["variables"]:
                _CACHE["variables"][dataset][year] = df_variables
            else:
                _CACHE["variables"][dataset] = {year: df_variables}


        except JSONDecodeError:
            raise("Cannot connect to U.S. Census API")


    return df_variables



# todo: docstrings!!!!
def get_geographies(dataset, year):
    """

    Parameters
    ----------
    dataset: dataset name (formatted in manner provided by get_supported_products())
    year: year to use for dataset query

    Returns
    -------
    DataFrame of available variables for this dataset and year with the following columns:
        - dataset: dataset name
        - year: dataset year
        - name: geography name
        - geoLevelDisplay: code for the geography level displayed

    """
    if isinstance(dataset, str):
        dataset = [dataset]

    if isinstance(year, (float, str)):
        year = int(year)

    if not isinstance(year, (np.ndarray, tuple, list)):
        year = [int(year)]

    df_supported = get_supported_products()

    df_geographies_list = []
    for this_dataset in dataset:
        for this_year in year:
            use_cache = False
            if this_dataset in _CACHE["geographies"]:
                if this_year in _CACHE["geographies"][this_dataset]:
                    df_geographies = _CACHE["geographies"][this_dataset][this_year]
                    use_cache = True

            if not use_cache:
                # fuzzy match of dataset name
                dataset_match = difflib.get_close_matches(this_dataset, df_supported['dataset'], n=1, cutoff=0.01)

                # get geography link
                mask = (df_supported['dataset'] == dataset_match[0]) & (df_supported['vintage'] == this_year)
                geography_link = df_supported.loc[mask, 'geographyLink'].values[0]

                # request and reformat data
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
                except JSONDecodeError:
                    raise ("Cannot connect to U.S. Census API")

                df_geographies = pd.DataFrame(data)
                df_geographies = df_geographies[['name', 'geoLevelDisplay']]

                df_geographies['year'] = this_year
                col = df_geographies.pop('year')
                df_geographies.insert(0, 'year', col)

                df_geographies['dataset'] = this_dataset
                col = df_geographies.pop('dataset')
                df_geographies.insert(0, 'dataset', col)

                if this_dataset in _CACHE["geographies"]:
                    _CACHE["geographies"][this_dataset][this_year] = df_geographies
                else:
                    _CACHE["geographies"][this_dataset] = {this_year: df_geographies}

            df_geographies_list.append(df_geographies)

    # convert list of dataframes to dataframe
    df_geographies = pd.concat(df_geographies_list, axis=0)

    return df_geographies
