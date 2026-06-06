import pandas as pd



class DefaultInterface(object):


    def __init__(self, product):
        self._cen_prod = product


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
        return

    @property
    def parameter_codes(self):
        """

        Returns
        -------

        """
        return

    @property
    def parameter_names(self):
        """

        Returns
        -------

        """
        return

    @property
    def pandas_rename(self):
        """

        Returns
        -------

        """
        return {v: n for v, n in zip(self.variables, self.names)}

    def add_defaults(self, parameter_code, name):
        """

        Parameters
        ----------
        parameter_code
        name

        Returns
        -------

        """

    def remove_defaults(self, parameter_code, name):
        """

        Parameters
        ----------
        parameter_code
        name

        Returns
        -------

        """



