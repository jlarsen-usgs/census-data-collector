import numpy as np
import pandas as pd
import geopandas as gpd


def _IGNORE():
    """
    Variables to ignore when performing area weighted resampling

    Returns
    -------
        tuple of variable names
    """
    from ..datacollector.tigerweb import TigerWebVariables

    ignore = (
        TigerWebVariables.mtfcc,
        TigerWebVariables.oid,
        TigerWebVariables.geoid,
        TigerWebVariables.state,
        TigerWebVariables.place,
        TigerWebVariables.place.lower,
        TigerWebVariables.county,
        TigerWebVariables.cousub,
        TigerWebVariables.tract,
        TigerWebVariables.blkgrp,
        TigerWebVariables.basename,
        TigerWebVariables.name,
        TigerWebVariables.lsadc,
        TigerWebVariables.funcstat,
        TigerWebVariables.arealand,
        TigerWebVariables.areawater,
        TigerWebVariables.stgeometry,
        TigerWebVariables.centlat,
        TigerWebVariables.centlon,
        TigerWebVariables.intptlat,
        TigerWebVariables.intptlon,
        TigerWebVariables.objectid,
    )
    return ignore


def calculate_intersection_weights(cen_gdf, aoi_gdf, groupby=None):
    """
    Method to calculate weights by the fractional area of itersection between
    census geometries and area of interest geometries.

    Parameters
    ----------
    cen_gdf : gpd.GeoDataFrame
        geodataframe from the TigerWeb().features or from CensusBase.features
    aoi_gdf : gpd.GeoDataFrame
        user provided geodataframe
    groupby : None, str, Iterable
        optional groupby routine to calculate and return the intersection weights
        for reuse, returns only the groupby keys and intersection weights

    Returns
    -------
        geopandas GeoDataFrame
    """
    # todo: add in dask-geopandas support which is a complex sjoin() command for this
    cen_gdf = cen_gdf.to_crs(aoi_gdf.crs)

    cen_gdf["cdf_area"] = cen_gdf.geometry.area
    idf = gpd.overlay(cen_gdf, aoi_gdf, how="intersection")
    idf["insec_area"] = idf.geometry.area
    idf["i_weight"] = idf["insec_area"] / idf["cdf_area"]

    idf = idf.drop(columns=["insec_area", "cdf_area"])
    if groupby is not None:
        if not isinstance(groupby, (list, tuple, np.ndarray)):
            groupby = [groupby,]
        else:
            groupby = list(groupby)

        idf = idf.groupby(by=groupby, as_index=False)[["i_weight"]].sum()
        return idf

    else:
        return idf


def area_weighted_resampling(cen_gdf, aoi_gdf, groupby, method="accumulate"):
    """
    Method to perform area weighted resampling on census-data based on external
    geometries. This method supports a number of area_weighted operations including
    "sum" ("accumulate"), "mean", "max", "median", "min", and "count"

    Parameters
    ----------
    cen_gdf : gpd.GeoDataFrame
        GeoDataFrame results from the Census data collector
    aoi_gdf : gpd.GeoDataFrame or None
        Input GeoDataFrame containing the area of interest(s) for resampling,
        None can be provided if an "i_weight" field has been calculated and joined
        to the census geodataframe
    groupby : str, list, tuple, or np.ndarray
        str or iterable of dataframe column names
    method : str

    refresh : bool
        method to refresh the intersection weights if they exist on the dataframe

    Returns
    -------
        pd.DataFrame
    """
    from .utilities import sequence_matcher

    # todo: extend support for dask-geopandas/dask groupby operations
    valid = ("sum", "accumulate", "mean", "max", "min", "count", "median")
    method = method.lower()

    method = sequence_matcher(method, valid, fail_ratio=0.5)

    if not isinstance(groupby, (np.ndarray, tuple, list)):
        if isinstance(groupby, (str, int, float)):
            groupby = [groupby,]
        else:
            raise NotImplementedError(
                f'{type(groupby)} is not currently supported for groupby'
            )
    else:
        groupby = list(groupby)

    if aoi_gdf is not None:
        drop_keys = []
        for key in groupby:
            if key in list(cen_gdf) and key in list(aoi_gdf):
                drop_keys.append(key)
        if drop_keys:
            cen_gdf = cen_gdf.drop(columns=[drop_keys])

        igdf = calculate_intersection_weights(cen_gdf, aoi_gdf)
    else:
        if "i_weight" not in list(cen_gdf):
            raise KeyError(
                "i_weight column must be calculated or an aoi_gdf must be provided"
            )
        else:
            igdf = cen_gdf.copy()

    ignore = _IGNORE() + tuple(groupby) + ("geometry",)
    columns = [i for i in list(igdf) if i not in ignore]

    if method in ("sum", "accumulate", "mean", "max", "min", "count"):
        for column in columns:
            igdf[column] *= igdf["i_weight"]

        if method in ("sum", "accumulate", "mean"):
            ogdf = igdf.groupby(by=groupby, as_index=False)[columns].sum()
            if method == "mean":
                for column in columns:
                    ogdf[column] /= ogdf["i_weight"]
        elif method == "max":
            ogdf = igdf.groupby(by=groupby, as_index=False)[columns].max()

        elif method == "count":
            ogdf = igdf.groupby(by=groupby, as_index=False)[["i_weight"]].sum()
            ogdf["count"] = ogdf["i_weight"]

        else:
            ogdf = igdf.groupby(by=groupby, as_index=False)[columns].min()

        ogdf = ogdf.drop(columns=["i_weight"])

    elif method == "median":
        ogdf = igdf.groupby(by=groupby, as_index=False)[columns].median()
    else:
        raise NotImplementedError(
            f"{method} not implemented for area_weighted_resampling"
        )

    if "Id" in list(ogdf) and "Id" not in groupby:
        ogdf = ogdf.drop(columns=["Id"])

    return ogdf


'''
def _IGNORE():
    from ..datacollector.tigerweb import TigerWebVariables
    from ..datacollector.acs import AcsVariables
    from ..datacollector.dec import Sf3Variables, Sf3Variables1990

    return (TigerWebVariables.mtfcc,
            TigerWebVariables.oid,
            TigerWebVariables.geoid,
            TigerWebVariables.state,
            TigerWebVariables.place,
            TigerWebVariables.place.lower,
            TigerWebVariables.county,
            TigerWebVariables.cousub,
            TigerWebVariables.tract,
            TigerWebVariables.blkgrp,
            TigerWebVariables.basename,
            TigerWebVariables.name,
            TigerWebVariables.lsadc,
            TigerWebVariables.funcstat,
            TigerWebVariables.arealand,
            TigerWebVariables.areawater,
            TigerWebVariables.stgeometry,
            TigerWebVariables.centlat,
            TigerWebVariables.centlon,
            TigerWebVariables.intptlat,
            TigerWebVariables.intptlon,
            TigerWebVariables.objectid,
            TigerWebVariables.population,
            AcsVariables.median_income,
            AcsVariables.median_p_owner_cost_to_income,
            Sf3Variables1990.median_income,
            Sf3Variables.median_income,
            AcsVariables.median_h_year,
            'pop_density',
            AcsVariables.gini)


def _AVERAGE():
    from ..datacollector.acs import AcsVariables
    from ..datacollector.dec import Sf3Variables, Sf3Variables1990
    return (AcsVariables.median_income,
            AcsVariables.median_p_owner_cost_to_income,
            Sf3Variables1990.median_income,
            Sf3Variables.median_income,
            AcsVariables.median_h_year,
            AcsVariables.gini,
            'pop_density')


def _POPULATION():
    from ..datacollector.acs import AcsVariables
    from ..datacollector.dec import Sf3Variables, Sf3Variables1990

    return (Sf3Variables.population,
            AcsVariables.population,
            Sf3Variables1990.population)
'''