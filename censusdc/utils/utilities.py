import os
import requests
from bs4 import BeautifulSoup
import pandas as pd


def get_wkt_wkid_table(refresh=False):
    """
    Method to build or load the ArcGIS Well Known Text and Well Known ID
    table

    Returns
    -------

    """
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    table_file = os.path.join(utils_dir, '..', 'cache', 'wkid_wkt_table.dat')

    if not os.path.isfile(table_file) or refresh:
        r = requests.get(
            'https://developers.arcgis.com/rest/services-reference/'
            'projected-coordinate-systems.htm',
            verify=False)
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.findAll('table')
        table_body = table[0].find('tbody')
        data = []
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])

        data2 = ['\t'.join(d) + '\n' for d in data]
        data2.insert(0, 'wkid\tname\twkt\n')
        with open(table_file, 'w') as foo:
            foo.writelines(data2)

    df = pd.read_csv(table_file, delimiter='\t', index_col=False,)
    return df