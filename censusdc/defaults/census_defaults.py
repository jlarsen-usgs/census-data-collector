import pandas as pd
from pathlib import Path



class DefaultInterface(object):
    """
    Parent interface for storing and loading default census data pulls

    Class is not intended to be instantiated directly. See Acs5Defaults for
    a general usage pattern to develop new Defaults class objects.
    """
    def __init__(self, product, subproduct=None):
        self._cen_prod = product
        self._cen_subprod = subproduct
        self._base_path = Path(__file__).parent

    def _load_dataframe(self):
        """

        Returns
        -------

        """
        self._data = pd.read_csv(self._file).to_dict()

    @property
    def census_product(self):
        """

        Returns
        -------

        """
        return self._cen_prod

    @property
    def dataframe(self):
        """

        Returns
        -------

        """
        return pd.DataFrame(self._data)

    @property
    def parameter_codes(self):
        """

        Returns
        -------

        """
        return self._data["cen_code"]

    @property
    def parameter_names(self):
        """

        Returns
        -------

        """
        return self._data["name"]

    @property
    def pandas_rename(self):
        """

        Returns
        -------

        """
        return {v: n for v, n in zip(self.parameter_codes, self.parameter_names)}

    def add_defaults(self, parameter_code, name):
        """

        Parameters
        ----------
        parameter_code
        name

        Returns
        -------

        """
        self._data["cen_code"].append(parameter_code)
        self._data["name"].append(name)

    def remove_defaults(self, parameter_code=None, name=None):
        """

        Parameters
        ----------
        parameter_code
        name

        Returns
        -------

        """
        if parameter_code is None and name is None:
            raise AssertionError("parameter_code or name must be supplied")

        if parameter_code is None:
            ix = self._data["name"].index(name)
            self._data["name"].pop(ix)
            self._data["cen_code"].pop(ix)

        else:
            ix = self._data["cen_code"].index(parameter_code)
            self._data["name"].pop(ix)
            self._data["cen_code"].pop(ix)

    def clear_defaults(self):
        """
        Removes all defaults for a given instance

        Returns
        -------

        """
        self._data["name"] = []
        self._data["cen_code"] = []

    def write_defaults(self, f=None):
        """
        Writes new defaults to file

        Parameters
        ----------
        f : PathLike
            optional file path to write defaults to, if None the defaults
            will be stored in the default file for the Census Data Collector

        Returns
        -------
            None
        """