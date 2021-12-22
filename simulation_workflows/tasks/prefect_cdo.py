#!/usr/bin/env python3
"""
prefect_cdo.py
~~~~~~~~~~~~~~

A wrapper for the Python interface to the CDO command line tool which can be
used as Prefect Tasks.

Called ``prefect_cdo`` to avoid conflict with the ``cdo`` module.
"""

from prefect import Task

import cdo as pycdo


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

    def run(self, command, input_file, output_file=None, **kwargs):
        """
        Execute the CDO command.

        Args:
            - command (str): The CDO command to execute.
            - input_file (str): The input file to the CDO command.
            - output_file (str): The output file of the CDO command.
            - **kwargs: Additional keyword arguments to pass to the CDO command.
        """
        return getattr(self.CDO, command)(
            input=input_file, output=output_file, **kwargs
        )
