from setuptools import setup

import versioneer

requirements = [
    # package requirements go here
]

setup(
    name="simulation_workflows",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Workflows for setting up initial conditions, analyzing output, plotting, and much more. In Prefect.",
    license="GNUv3",
    author="Paul Gierz",
    author_email="pgierz@awi.de",
    url="https://github.com/pgierz/simulation_workflows",
    packages=["simulation_workflows"],
    install_requires=requirements,
    keywords="simulation_workflows",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
