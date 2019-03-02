"""
Installer for ScSF
"""
from setuptools import setup, find_packages


# Synchronize version from code.
version = "0.1"

# Do the setup
setup(
    name="scsf",
    packages=find_packages(),
    version=version,
    extras_require={},
    author="Gonzalo Rodrigo",
    author_email="GPRodrigoAlvarez@lbl.gov",
    maintainer="Gonzalo Rodrigo",
    url="https://bitbucket.org/gonzalorodrigo/scsf/",
    license="BSD 3-clause",
    description="""ScSF: Schedulling Simulation Framework: Tool covering the
    scheduling simulation framework: workload modeling and generations,
    control of an external scheduler simulator, experiment coordination, and
    results analysis. For more details read:
    R. Gonzalo P., E. Elmroth, P-O. Ostberg, and L. Ramakrishnan, 'ScSF: a
    Scheduling Simulation Framework', in Proceedings of the 21th Workshop on
    Job Scheduling Strategies for Parallel Processing, 2017.
    """,
    long_description="",
    keywords=["HPC", "workloads", "simulation"],
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)