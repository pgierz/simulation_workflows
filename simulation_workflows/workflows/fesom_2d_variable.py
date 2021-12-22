#!/usr/bin/env python3
"""
    Prefect workflows to get 2d variable from FESOM onto a regular grid.
"""
import os
import pathlib
import re

import numpy as np
import prefect
from prefect import Flow, Parameter, task

from simulation_workflows.tasks import common_cdo_chains, fesom


@task
def np_arange(start, stop, step):
    """
    Task to create a numpy array from start, stop and step.
    """
    return np.arange(start, stop, step)


@task
def finalize_pattern(varname: str) -> str:
    """
    Task to finish up the FESOM filename pattern with the year and month.
    """
    return f"{varname}." + "fesom.[0-9]{6}.01.nc"


@task
def get_n_newest_files_for_pattern(pattern: str, path: str, n: int) -> list:
    """
    Task to get the n newest files for a given pattern.
    """
    logger = prefect.context.get("logger")
    files = [path + f"/{f}" for f in os.listdir(path)]
    files = [pathlib.Path(f) for f in files if re.search(pattern, f)]
    logger.info(f"Found {len(files)} files for pattern {pattern}")
    logger.debug(f"Files: {files}")
    logger.info("Sorting files by modification time")
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    logger.info(f"Returning the {n} newest files")
    return files[:n]


with Flow(
    "Regridded Timmean of Newest N Files for a FESOM 2D Variable (ESM Tools Layout)"
) as flow:
    # Get the experiment ID
    expid = Parameter(name="Experiment ID")
    # Get the main path of the output directory from the top of the experiment tree from the user:
    path = Parameter(name="Path to the top level of the experiment tree")
    # Get the 2D variable as a user parameter
    varname = Parameter(name="FESOM Variable Name")
    # Get the number of files to average as a user parameter
    nfiles = Parameter(name="nfiles", default=30)
    # Get the regrid size from the user:
    lat_size = Parameter("Latitude Size (e.g 1 for a 1x1 degree grid)", default=1.0)
    lon_size = Parameter("Longitude Size (e.g 1 for a 1x1 degree grid)", default=1.0)

    lons = np_arange(-180, 180, lon_size)
    lats = np_arange(-90, 90, lat_size)
    output_dir = f"{path}/outdata/fesom"
    pattern = finalize_pattern(varname)
    # Get all files in the output directory
    files = get_n_newest_files_for_pattern(pattern, output_dir, nfiles)
    # Merge the files together:
    fesom_ds = common_cdo_chains.mergetime_files(files, returnXDataset=True)

    # Get the FESOM Mesh:
    fesom_mesh = fesom.get_mesh(path)

    # Regrid the data:
    regridded_data = fesom.interpolate_data_2d(
        fesom_ds,
        varname,
        fesom_mesh,
        lons,
        lats,
    )
