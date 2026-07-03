import difflib

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

# todo: code in supported geographies
_GEOGRAPHIES = {

}

# todo: docstrings!!!!
def get_supported_products():
    """

    Returns
    -------

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
    dataset
    year

    Returns
    -------

    """
    if isinstance(dataset, str):
        dataset = [dataset]

    if isinstance(year, (float, str)):
        year = int(year)

    if not isinstance(year, (np.ndarray, tuple, list)):
        year = [int(year)]

    df_supported = get_supported_products()

    df_variables_list = []
    # todo: I think we should remove looping from here and force the user to get variables
    #  for one product at a time
    for this_dataset in dataset:

        for this_year in year:

            # fuzzy match of dataset name
            dataset_match = difflib.get_close_matches(this_dataset, df_supported['dataset'], n=1, cutoff=0.01)

            # get variable link
            mask = (df_supported['dataset'] == dataset_match[0]) & (df_supported['vintage'] == this_year)
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

                df_variables['year'] = this_year
                col = df_variables.pop('year')
                df_variables.insert(0, 'year', col)

                df_variables['dataset'] = this_dataset
                col = df_variables.pop('dataset')
                df_variables.insert(0, 'dataset', col)
                df_variables.sort_values(by=["name"], inplace=True)
                df_variables.reset_index(drop=True, inplace=True)
                df_variables_list.append(df_variables)

            except JSONDecodeError:
                raise("Cannot connect to U.S. Census API")

    # convert list of dataframes to dataframe
    df_variables = pd.concat(df_variables_list, axis=0)

    return df_variables



# todo: docstrings!!!!
def get_geographies(dataset, year):
    """

    Parameters
    ----------
    dataset :
    year :

    Returns
    -------

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
                # TODO: Reformat this - what are these columns?  What does the user need?

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


if __name__ == "__main__":

    dataset = []
    year = []
    df_supported = get_supported_products()
    df_variables = get_variables(df_supported, dataset, year)
    get_geographies(df_supported, dataset, year)
