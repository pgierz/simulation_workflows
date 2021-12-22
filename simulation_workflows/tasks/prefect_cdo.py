#!/usr/bin/env python3
"""
prefect_cdo.py
~~~~~~~~~~~~~~

A wrapper for the Python interface to the CDO command line tool which can be
used as Prefect Tasks.

Called ``prefect_cdo`` to avoid conflict with the ``cdo`` module.
"""

import cdo as pycdo
from prefect import Task


class Command(Task):
    """
    A Prefect Task for the Python interface of the CDO command line tool.
    """

    def __init__(self, CdoObj_kwargs={}, **kwargs):
        """
        Args:
            - CdoObj_kwargs (dict): A dictionary of keyword arguments to pass
              to the cdo.Cdo object __init__.
            - **kwargs: Additional keyword arguments to pass to the Task object.
        """
        self.CDO = pycdo.Cdo(**CdoObj_kwargs)
        super().__init__(**kwargs)

    def run(self, command, input, output=None, **kwargs):
        """
        Execute the CDO command.

        Args:
            - command (str): The CDO command to execute.
            - input (str): The input file to the CDO command.
            - output (str): The output file of the CDO command.
            - **kwargs: Additional keyword arguments to pass to the CDO command.
        """
        return getattr(self.CDO, command)(input=input, output=output, **kwargs)
