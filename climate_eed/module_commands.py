import os
from dask.diagnostics import ProgressBar
from climate_eed.module_config import CopernicusConfig, ICISKConfig, PlanetaryConfig, parse_bbox, parse_collections, parse_dates, parse_query, parse_repository
from climate_eed.module_icisk_operations import icisk_data_request
from climate_eed.module_planetary_operations import data_request, var_list_request


def list_repo_vars(repository, collections):

    repository = parse_repository(repository)
    collections = parse_collections(collections)

    var_list = var_list_request(repository, collections)
    
    return var_list


# def fetch_var_copernicus(varname=CopernicusConfig.DEFAULT_VARNAME,
#                          factor=CopernicusConfig.DEFAULT_FACTOR, 
#                          bbox=CopernicusConfig.DEFAULT_BBOX, 
#                          years=CopernicusConfig.DEFAULT_YEARS, 
#                          month=CopernicusConfig.DEFAULT_MONTH, 
#                          leadtime_month=CopernicusConfig.DEFAULT_LEADTIME_MONTH, 
#                          fileout=CopernicusConfig.DEFAULT_FILEOUT):
#     """
#     Fetches data from the Copernicus Climate Data Store API and returns it as an xarray dataset.
#     Args:
#         - varname (str): The variable name to fetch. Example: "total_precipitation".
#         - factor (float): The factor to multiply the variable by. Example: 1000.
#         - bbox (list): The bounding box to fetch the data from. Example: [6.75, 36.75, 18.28, 47.00].
#         - years (str): The year of the data to fetch. Example:  ['1993', '1994', '1995'].
#         - month (str): The month of the data to fetch. Example: "05".
#         - leadtime_month (str): The leadtime month of the data to fetch. Example: ['1', '2', '3', '4', '5', '6'].
#         - fileout (str): The file to output the data to. Example: "*.grib".
#     Returns:
#         - xr.Dataset: The data fetched from the Copernicus Climate Data Store API.
#     """
#     output_ds = data_request(varname, factor, bbox, years, month, leadtime_month, fileout)
    
#     return output_ds

def fetch_var(varname=PlanetaryConfig.DEFAULT_VARNAME, 
         models=PlanetaryConfig.DEFAULT_MODELS,
         factor=PlanetaryConfig.DEFAULT_FACTOR, 
         bbox=PlanetaryConfig.DEFAULT_BBOX, 
         start_date=PlanetaryConfig.DEFALUT_START_DATE, 
         end_date=PlanetaryConfig.DEFALUT_END_DATE, 
         repository=PlanetaryConfig.DEFAULT_REPOSITORY, 
         collections=PlanetaryConfig.DEFAULT_COLLECTIONS, 
         query=PlanetaryConfig.DEFAULT_QUERY, 
         return_format=PlanetaryConfig.DEFAULT_RETURN_FORMAT, 
         fileout=PlanetaryConfig.DEFAULT_FILEOUT,
         additional_params=None):
    
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
        - return_format (str): The format to return the data in. Example: "pd" or "xr".
        - fileout (str): The file to output the data to. Example: "*.csv" or "*.nc".
    Returns:
        - pd.DataFrame or xr.Dataset: The data fetched from the STAC repository."""

    query = parse_query(query)
    if start_date and end_date:
        start_date, end_date = parse_dates(start_date, end_date)
    collections = parse_collections(collections)
    bbox = parse_bbox(bbox)
    repository = parse_repository(repository)
    if repository == ICISKConfig.STACAPI_SEASONAL_FORECASTS_URL:
        output_ds = icisk_data_request(varname, models, factor, bbox, start_date, end_date, repository, collections, query, additional_params)
    else:
        output_ds = data_request(varname, models, factor, bbox, start_date, end_date, repository, collections, query)
    # print("OUTPUT DS: ", output_ds)
    df = None
    if return_format == "pd":
        with ProgressBar():
            df = output_ds.to_dataframe()
    else:
        df = output_ds
    
    if fileout:
    
        with ProgressBar():
            if fileout.endswith(".csv"):
                if not df:
                    df = output_ds.to_dataframe()
                df.to_csv(fileout)
                return df
            elif fileout.endswith(".nc"):
                output_ds.to_netcdf(fileout)

                return output_ds
            
    return df