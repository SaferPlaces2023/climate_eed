import cdsapi
import os
from dotenv import load_dotenv, find_dotenv
import xarray as xr

# Disable warnings for data download via API
import urllib3 
urllib3.disable_warnings()

def cds_data_request(dataset, query, fileout, engine='netcdf4'):
    """
    Fetches data from the Copernicus Climate Data Store API and returns it as an xarray dataset.
    Args:
        - varname (str): The variable name to fetch. Example: "total_precipitation".
        - factor (float): The factor to multiply the variable by. Example: 1000.
        - bbox (list): The bounding box to fetch the data from. Example: [6.75, 36.75, 18.28, 47.00].
        - years (str): The year of the data to fetch. Example:  ['1993', '1994', '1995'].
        - month (str): The month of the data to fetch. Example: "05".
        - leadtime_month (str): The leadtime month of the data to fetch. Example: ['1', '2', '3', '4', '5', '6'].
        - file_grib (str): The file to output the data to. Example: "*.grib".
    Returns:
        - xr.Dataset: The data fetched from the Copernicus Climate Data Store API.
    """
    output_ds = None

    load_dotenv(find_dotenv())

    URL = 'https://cds.climate.copernicus.eu/api/v2'
    KEY = os.environ.get('CDSAPI_KEY')
    # DATADIR = './test_data/seasonal'
    # {
    #     'format': 'grib',
    #     'originating_centre': 'ecmwf',
    #     'system': '5',
    #     'variable': varname,
    #     'product_type': 'monthly_mean',
    #     'year': years,
    #     'month': month,
    #     'leadtime_month': leadtime_month,
    # }

    c = cdsapi.Client(url=URL, key=KEY)

    # Hindcast data request
    c.retrieve(
        dataset,    # 'seasonal-monthly-single-levels',
        query,
        fileout)
    
    output_ds = xr.open_dataset(f'{fileout}', engine=engine)
    
    return output_ds
