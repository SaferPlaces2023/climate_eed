import xarray as xr
import fsspec
import s3fs
from ftplib import FTP
import os

from climate_eed.module_config import SMHIConfig


def download_files_from_ftp(ftp, folder):
    # ftp.cwd(folder)
    files = ftp.nlst()
    nc_files = []
    for file in files:
        if file.endswith('.nc'):
            local_filename = os.path.join(f"./seasonal_forecast/{folder}", file)
            nc_files.append(local_filename)
            if not os.path.exists(local_filename):

                if not os.path.exists(f"./seasonal_forecast/{folder}"):
                    os.makedirs(f"./seasonal_forecast/{folder}")
                with open(local_filename, 'wb') as f:
                    ftp.retrbinary('RETR ' + file, f.write)
                # print(f"Downloaded: {local_filename}")
            else:
                # print(f"File already exists: {local_filename}")
                pass
    ftp.cwd("..")
    return nc_files

# Function to read geometry and bbox from NetCDF file
def read_netcdf(file_path):
    nc_file = fsspec.open(file_path,anon=True)
    nc = xr.open_dataset(nc_file.open())  # Dataset(file_path, 'r')
    # Extract geometry and bbox from the NetCDF file

    return nc


def smhi_data_request(living_lab="georgia", data_dir="seasonal_forecast", issue_date="202404"):
    """
    Fetches data from a STAC repository and returns it as an xarray dataset.
    Args:
        - varname (str): The variable name to fetch. Example: "tasmax".
        - factor (float): The factor to multiply the variable by. Example: 1000.
        - bbox (list): The bounding box to fetch the data from. Example: [6.75, 36.75, 18.28, 47.00].
        - start_date (str): The start date of the data to fetch. Example: "01-01-2020".
        - end_date (str): The end date of the data to fetch. Example: "01-02-2020".
        - repository (str): The STAC repository to fetch the data from. Example: "planetary".
        - collections (str): The collections to fetch the data from. Example: "era5-pds".
        - query (str): The query to filter the data by. Example: {"era5:kind": {"eq": "fc"}}.
    Returns:
        - xr.Dataset: The data fetched from the STAC repository.
    """
    data_array = []
    ftp_config= {
        "url":SMHIConfig.FTP_HOST,
        "folder":SMHIConfig.FTP_DIR,
        "user":SMHIConfig.FTP_USER,
        "passwd":SMHIConfig.FTP_PASS
    }

    # Connect to FTP server
    ftp = FTP(ftp_config['url'])
    ftp.login(user=ftp_config['user'], passwd=ftp_config['passwd'])

    remote_folder = f"{ftp_config['folder']}/{living_lab}/{data_dir}/{issue_date}"
    local_folder = f"./{data_dir}/{issue_date}"

    with FTP(ftp_config['url']) as ftp:
        ftp.login(ftp_config['user'], ftp_config['passwd'])
        ftp.cwd(remote_folder)
        
        files = download_files_from_ftp(ftp, remote_folder)

    for file_nc in files:
        file_nc_path = f"{local_folder}/{file_nc}"
        data = read_netcdf(file_nc)

        model = file_nc_path.split('COUT_')[1].split('.')[0]
        data.coords['model'] = model

        if "COUT" in data:
            data["COUT"] = data["COUT"].expand_dims('model')

        data_array.append(data)

    output_ds = xr.merge(data_array)

    return output_ds
