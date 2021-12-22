#!/usr/bin/env python3
"""
This module contains common CDO command chains as Prefect tasks.
"""
import re
from typing import Any

import prefect
from prefect import task

from .prefect_cdo import Command

cdo = Command()


@task
def get_newest_files_for_pattern(files: list, n: int, pattern: str) -> list:
    """
    Get the n newest files for a given pattern.

    Args:
        files (list): List of files to filter.
        n (int): Number of files to return.
        pattern (str): Pattern to filter files.

    Returns:
        list: List of files.
    """
    logger = prefect.context.get("logger")
    logger.info(f"pattern={pattern}")
    repattern = re.compile(pattern)
    logger.info("These files are being sorted:")
    for f in files:
        logger.info(f)
    files = sorted([f for f in files if repattern.search(f)])
    logger.info("These files were found:")
    [logger.info(f) for f in files]
    return files[-n:]


@task
def mergetime_files(files: list[str], output: str or None = None, **kwargs) -> Any:
    """
    Merge files into a single file.

    Args:
        files (list): List of files to merge.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run("mergetime", input=joined_files, output=output, **kwargs)


@task
def mergetime_selvar_files(
    files: list[str], var: str, output: str or None = None, **kwargs
) -> Any:
    """
    Merge files into a single file, first selecting a variable.

    Args:
        files (list): List of files to merge.
        var (str): Variable to select.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run(
        "mergetime", input=f"-selvar,{var} {joined_files}", output=output, **kwargs
    )


@task
def selvar_files(
    files: list[str], var: str, output: str or None = None, **kwargs
) -> Any:
    """
    Select a variable from a list of files.

    Args:
        files (list): List of files to select from.
        var (str): Variable to select.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run(
        "selvar", input=f"-selvar,{var} {joined_files}", output=output, **kwargs
    )


@task
def timmean_files(files: list[str], output: str or None = None, **kwargs) -> Any:
    """
    Calculate the time mean of a list of files.

    Args:
        files (list): List of files to calculate the time mean.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run("timmean", input=joined_files, output=output, **kwargs)


@task
def ymonmean_files(files: list[str], output: str or None = None, **kwargs) -> Any:
    """
    Calculate the multiyear month mean of a list of files.

    Args:
        files (list): List of files to calculate the time mean.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run("ymonmean", input=joined_files, output=output, **kwargs)


@task
def yseasmean_files(files: list[str], output: str or None = None, **kwargs) -> Any:
    """
    Calculate the multiyear seasonal mean of a list of files.

    Args:
        files (list): List of files to calculate the time mean.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run("yseasmean", input=joined_files, output=output, **kwargs)


@task
def ymonmean_selvar_files(
    files: list[str], var: str, output: str or None = None, **kwargs
) -> Any:
    """
    Calculate the multiyear month mean of a list of files, first selecting a variable.

    Args:
        files (list): List of files to calculate the time mean.
        var (str): Variable to select.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run(
        "ymonmean", input=f"-selvar,{var} {joined_files}", output=output, **kwargs
    )


@task
def yseasmean_selvar_files(
    files: list[str], var: str, output: str or None = None, **kwargs
) -> Any:
    """
    Calculate the multiyear seasonal mean of a list of files, first selecting a variable.

    Args:
        files (list): List of files to calculate the time mean.
        var (str): Variable to select.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run(
        "yseasmean", input=f"-selvar,{var} {joined_files}", output=output, **kwargs
    )


@task
def depth_profile_at_point(
    files: list[str], lat: float, lon: float, output: str or None = None, **kwargs
) -> Any:
    """
    Get the profile at a given point.

    Args:
        files (list): List of files to calculate the depth profile.
        lat (float): Latitude of the point.
        lon (float): Longitude of the point.
        output (str): Output file.
        **kwargs: Additional arguments to pass to CDO.
    """
    joined_files = " ".join(files)
    return cdo.run(
        "fldmean",
        input=f"-sellonlatbox,{lon},{lon},{lat},{lat} {joined_files}",
        output=output,
        **kwargs,
    )
