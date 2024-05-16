import xarray as xr
import fsspec
# import s3fs
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


def smhi_data_request(living_lab="georgia", data_dir="seasonal_forecast", issue_date="202404",ftp_config=None):
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
    data_array = []
    if ftp_config is None:    
        config= {
            "url":SMHIConfig.FTP_HOST,
            "folder":SMHIConfig.FTP_DIR,
            "user":SMHIConfig.FTP_USER,
            "passwd":SMHIConfig.FTP_PASS
        }
    else:
        config = ftp_config

    # Connect to FTP server
    ftp = FTP(config['url'])
    ftp.login(user=config['user'], passwd=config['passwd'])

    remote_folder = f"{config['folder']}/{living_lab}/{data_dir}/{issue_date}"
    local_folder = f"./{data_dir}/{issue_date}"

    with FTP(config['url']) as ftp:
        ftp.login(config['user'], config['passwd'])
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
