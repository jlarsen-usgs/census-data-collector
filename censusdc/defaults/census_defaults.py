import pandas as pd
from pathlib import Path


class DefaultInterface(object):
    """
    Parent interface for storing and loading default census data pulls

    Class is not intended to be instantiated directly. See Acs5Defaults for
    a general usage pattern to develop new Defaults class objects.

    Parameters
    ----------
    product : str
        census product name, e.g., "acs5"
    subproduct : str
        optional subproduct name, e.g., "profile"

    """
    def __init__(self, product, subproduct=None):
        self._cen_prod = product
        self._cen_subprod = subproduct
        self._base_path = Path(__file__).parent
        self._data = None

    def _load_dataframe(self):
        """
        Internal method to load the stored defaults into memory

        """
        df = pd.read_csv(self._file)
        self._data = {c: list(df[c].values) for c in list(df)}

    @property
    def census_product(self):
        """
        Returns the Census Product that the instance is associated with

        """
        return self._cen_prod

    @property
    def dataframe(self):
        """
        Returns a pandas dataframe of defaults variables

        """
        return pd.DataFrame(self._data)

    @property
    def parameter_codes(self):
        """
        Returns census variable codes

        """
        return self._data["cen_code"].tolist()

    @property
    def parameter_names(self):
        """
        Returns human readable parameter names

        """
        return self._data["name"].tolist()

    @property
    def pandas_rename(self):
        """
        Returns a dictionary of parameter codes, parameter names for renaming
        feature output from census data pulls

        """
        return {v: n for v, n in zip(self.parameter_codes, self.parameter_names)}

    def add_defaults(self, parameter_code, name):
        """
        Method to add a new default variable to the stored defaults

        Parameters
        ----------
        parameter_code : str
            census parameter code
        name : str
            human readable name associated with the parameter code

        """
        self._data["cen_code"].append(parameter_code)
        self._data["name"].append(name)

    def remove_defaults(self, parameter_code=None, name=None):
        """
        Method to remove a default from the saved Defaults. Can be removed by
        census variable code or by human readable name

        Parameters
        ----------
        parameter_code : str
            optional parameter code to remove
        name : str
            optional variable name to remove

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
        if f is None:
            f = self._file

        self.dataframe.to_csv(f, index=False)


class UserDefaults(DefaultInterface):
    """
    Container for loading and manipulating default variables for
    American Community Survey census product data pulls

    f : None or PathLike
        Optional file name or None. If None, code will load in the
        default file for acs5 variables (subproduct dependent)
    subproduct : None or str
        Optional sub-product name (e.g., "profile", "summary")

    """
    def __init__(self):
        super().__init__("user", "specified")
        self._data = {"name": [], "cen_code": []}

    @staticmethod
    def load(f):
        """
        Method to load a user specified default file

        Parameters
        ----------
        f : PathLike
            file name path

        Returns
        -------
            UserDefaults object
        """
        obj = UserDefaults()
        obj._file = f
        obj._load_dataframe()
        return obj
