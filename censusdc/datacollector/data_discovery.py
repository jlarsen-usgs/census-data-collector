import requests
import pandas as pd
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError




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
    except JSONDecodeError:
        raise("Cannot connect to U.S. Census API")


