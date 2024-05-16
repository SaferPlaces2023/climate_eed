import os
from dask.diagnostics import ProgressBar
from climate_eed.module_config import CopernicusConfig, SMHIConfig, PlanetaryConfig, SMHIConfig, parse_bbox, parse_collections, parse_dates, parse_query, parse_repository
from climate_eed.module_copernicus_operations import cds_data_request
from climate_eed.module_smhi_operations import smhi_data_request
from climate_eed.module_planetary_operations import planetary_data_request, var_list_request


def list_repo_vars(repository, collections):

    repository = parse_repository(repository)
    collections = parse_collections(collections)

    var_list = var_list_request(repository, collections)
    
    return var_list


def fetch_var_copernicus(dataset, query, fileout, engine='netcdf4'):
    """
    Fetches data from the Copernicus Climate Data Store API and returns it as an xarray dataset.
    Args:
        - dataset (str): The dataset to fetch the data from. Example: "seasonal-monthly-single-levels".
        - query (dict): The query to filter the data by. Example: {'format': 'grib','originating_centre': 'ecmwf','system': '5','variable': varname,'product_type': 'monthly_mean','year': years,'month': month,'leadtime_month': leadtime_month}
        - fileout (str): The file to output the data to. Example: "*.grib".
    Returns:
        - xr.Dataset: The data fetched from the Copernicus Climate Data Store API.
    """
    output_ds = cds_data_request(dataset, query, fileout, engine)
    
    return output_ds


def fetch_var_smhi(living_lab, data_dir, issue_date, ftp_config=None):
    """
    Fetches data from ftp server and returns it as an xarray dataset.
    Args:
        - living_lab (str): The living lab to fetch the data from. Example: "georgia".
        - data_dir (str): The data directory to fetch the data from. Example: "seasonal_forecast".
        - issue_date (str): The issue date of the data to fetch. Example: "202404".
        - ftp_config (dict): The configuration of the FTP server. Example: {"url": "ftp.smhi.se", "folder": "/climate_data", "user": "user", "passwd": "passwd"}.
    Returns:
        - xr.Dataset: The data fetched from the FTP server.
    """
    output_ds = smhi_data_request(living_lab=living_lab, data_dir=data_dir, issue_date=issue_date, ftp_config=ftp_config)
    
    return output_ds


def fetch_var_planetary(varname=PlanetaryConfig.DEFAULT_VARNAME, 
         models=PlanetaryConfig.DEFAULT_MODELS,
         factor=PlanetaryConfig.DEFAULT_FACTOR, 
         bbox=PlanetaryConfig.DEFAULT_BBOX, 
         start_date=PlanetaryConfig.DEFALUT_START_DATE, 
         end_date=PlanetaryConfig.DEFALUT_END_DATE, 
         repository=PlanetaryConfig.DEFAULT_REPOSITORY, 
         collections=PlanetaryConfig.DEFAULT_COLLECTIONS, 
         query=PlanetaryConfig.DEFAULT_QUERY):
    
    """
    Fetches data from a STAC repository and returns it as a pandas dataframe or xarray dataset.
    Args:
        - varname (str): The variable name to fetch. Example: "tasmax".
        - models (str): The models to fetch the data from. Example: "GFDL-ESM4".
        - factor (float): The factor to multiply the variable by. Example: 1000.
        - bbox (list): The bounding box to fetch the data from. Example: [6.75, 36.75, 18.28, 47.00].
        - start_date (str): The start date of the data to fetch. Example: "01-01-2020".
        - end_date (str): The end date of the data to fetch. Example: "01-02-2020".
        - repository (str): The STAC repository to fetch the data from. Example: "planetary".
        - collections (str): The collections to fetch the data from. Example: "era5-pds".
        - query (str): The query to filter the data by. Example: {"era5:kind": {"eq": "fc"}}.
        - fileout (str): The file to output the data to. Example: "*.csv" or "*.nc".
    Returns:
        - pd.DataFrame or xr.Dataset: The data fetched from the STAC repository."""

    query = parse_query(query)
    if start_date and end_date:
        start_date, end_date = parse_dates(start_date, end_date)
    collections = parse_collections(collections)
    bbox = parse_bbox(bbox)
    repository = parse_repository(repository)

    output_ds = planetary_data_request(varname, models, factor, bbox, start_date, end_date, repository, collections, query)
            
    return output_ds