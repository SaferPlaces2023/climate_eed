import setuptools

VERSION = '0.0.1'
PACKAGE_NAME = "climate_eed"

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author="Marco Renzi",
    author_email="marco.renzi@gecosistema.it",
    description="Download climate data and models from repositories",
    long_description="""Download climate data and models from repositories:
    - https://planetarycomputer.microsoft.com/api/stac/v1/
    """,
    url="https://github.com/SaferPlaces2023/climate_eed",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'xarray[complete]',
        'pystac_client',
        'planetary_computer',
        'pandas',
        'numpy',
        # 'netCDF4',
        'adlfs',
        'click',
    ],
    entry_points="""
      [console_scripts]
      climate_eed=climate_eed.main:main
      """,
)
