#!/usr/bin/env python3
"""
    Common prefect tasks for FESOM analysis.
"""

import f90nml
import numpy as np
import pyfesom2 as pf2
import xarray as xr
from prefect import task


@task
def get_mesh(path: str):  # Once py2 is typed: -> pf2.load_mesh_data.fesom_mesh:
    """Returns the FESOM mesh"""
    # Get the FESOM Mesh from the namelist file:
    namelist_file = f"{path}/config/fesom/namelist.config"
    # Open the file with the f90nml parser:
    namelist = f90nml.read(namelist_file)
    # Get the mesh:
    mesh = namelist["paths"]["meshpath"]
    # Get the abg values from the namelist:
    a = namelist["geometry"]["alphaeuler"]
    b = namelist["geometry"]["betaeuler"]
    g = namelist["geometry"]["gammaeuler"]
    meshobj = pf2.load_mesh(mesh, abg=[a, b, g])
    return meshobj


@task
def interpolate_data_2d(
    data: xr.Dataset,
    varname: str,
    mesh,
    lons: np.ndarray,
    lats: np.ndarray,
    fesom2regular_kwargs: dict = {},
) -> xr.Dataset:
    """
    Interpolates data to a regular grid.

    Parameters
    ----------
    data : xr.Dataset
        The data to interpolate. Must be 2D!
    varname : str
        The name of the variable to interpolate.
    mesh :
        The FESOM mesh object. Already loaded via pyfesom2.load_mesh.
    lons : np.ndarray
        The longitudes to interpolate to.
    lats : np.ndarray
        The latitudes to interpolate to.
    fesom2regular_kwargs : dict
        Keyword arguments to pass to fesom2regular.


    Returns
    -------
    xr.Dataset
        The interpolated data, with attached coordinates.

    """
    data = data.variables[varname].data
    interpolated_data = pf2.fesom2regular(
        data, mesh, lons, lats, **fesom2regular_kwargs
    )
    fesom_ds = xr.Dataset(
        {varname: (["lat", "lon"], interpolated_data)},
        coords={"lat": lats, "lon": lons},
    )
    return fesom_ds


@task
def interpolate_data_3d(
    data: xr.Dataset,
    varname: str,
    mesh,
    lons: np.ndarray,
    lats: np.ndarray,
    fesom2regular_kwargs: dict = {},
) -> xr.Dataset:
    """
    Interpolates data to a regular grid.

    Parameters
    ----------
    data : xr.Dataset
        The data to interpolate. Must be 3D!
    varname : str
        The name of the variable to interpolate.
    mesh :
        The FESOM mesh object. Already loaded via pyfesom2.load_mesh.
    lons : np.ndarray
        The longitudes to interpolate to.
    lats : np.ndarray
        The latitudes to interpolate to.
    fesom2regular_kwargs : dict
        Keyword arguments to pass to fesom2regular.

    Returns
    -------
    xr.Dataset
        The interpolated data, with attached coordinates.
    """
    data = data.variables[varname].data
    interpolated_data = pf2.fesom2regular(
        data, mesh, lons, lats, **fesom2regular_kwargs
    )
    depths = mesh.zlevs
    fesom_ds = xr.Dataset(
        {varname: (["depth", "lat", "lon"], interpolated_data)},
        coords={"depth": depths, "lat": lats, "lon": lons},
    )
    return fesom_ds


@task
def compute_xmoc(
    mesh,
    data: xr.DataArray or np.ndarray,
    **xmoc_data_kwargs,
):
    """
    Computes the XMOC from a FESOM data array.

    Parameters
    ----------
    mesh :
        The FESOM mesh object. Already loaded via pyfesom2.load_mesh.
    data : xr.DataArray or np.ndarray
        The data to compute the XMOC from. Should 2d W of (nodes, levels).
    xmoc_data_kwargs : dict
        Keyword arguments to pass to pyfesom2.xmoc_data.
    """
    xmoc_data = pf2.xmoc_data(mesh, data, **xmoc_data_kwargs)
    return xmoc_data
