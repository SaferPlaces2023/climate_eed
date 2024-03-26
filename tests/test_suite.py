import pandas as pd
import pytest
import xarray as xr
from climate_eed import fetch_var, list_repo_vars


def test_fetch_var():
    """Test the fetch_var function."""
    # Test the fetch_var function
    
    var_ERA5 = "precipitation_amount_1hour_Accumulation"
    start_date_str = "01-01-1995"
    end_date_str = "31-12-1995"
    factor_sel = 1000
    location_str = ["26.0","-31.0","29.0","-29.0"]
    data_ERA5 = fetch_var(varname=var_ERA5,start_date=start_date_str,end_date=end_date_str,factor=factor_sel,bbox=location_str,query={"era5:kind": {"eq": "fc"}})
    
    assert type(data_ERA5) == xr.DataArray
    assert data_ERA5.shape == (8760, 9, 13)


def test_fetch_var_nc_file():
    """Test the fetch_var function with nc file result."""
    # Test the fetch_var function
    
    nc_file = "test.nc"

    var_ERA5 = "precipitation_amount_1hour_Accumulation"
    start_date_str = "01-01-1995"
    end_date_str = "31-12-1995"
    factor_sel = 1000
    location_str = ["26.0","-31.0","29.0","-29.0"]
    data_ERA5 = fetch_var(varname=var_ERA5,start_date=start_date_str,end_date=end_date_str,factor=factor_sel,bbox=location_str,query={"era5:kind": {"eq": "fc"}}, fileout=nc_file)

    ds = xr.open_dataset(nc_file)

    ds_shape = (ds.sizes['time'], ds.sizes['lat'], ds.sizes['lon'])

    assert ds_shape == (8760, 9, 13)


def test_fetch_var_pd_df():
    """Test the fetch_var function with pd dataframe as return."""
    # Test the fetch_var function
    

    var_ERA5 = "precipitation_amount_1hour_Accumulation"
    start_date_str = "01-01-1995"
    end_date_str = "31-12-1995"
    factor_sel = 1000
    location_str = ["26.0","-31.0","29.0","-29.0"]
    data_ERA5 = fetch_var(varname=var_ERA5,start_date=start_date_str,end_date=end_date_str,factor=factor_sel,bbox=location_str,query={"era5:kind": {"eq": "fc"}}, return_format="pd")


    assert data_ERA5.shape == (1024920, 1)  # (8760, 9, 13)
    assert type(data_ERA5) == pd.DataFrame


def test_list_repo_vars():
    """Test the list_repo_vars function."""
    # Test the list_repo_vars function
    vars = list_repo_vars(repository="planetary",collections="era5-pds")
    assert vars == [
        'surface_air_pressure', 
        'sea_surface_temperature', 
        'eastward_wind_at_10_metres', 
        'air_temperature_at_2_metres', 
        'eastward_wind_at_100_metres', 
        'northward_wind_at_10_metres', 
        'northward_wind_at_100_metres', 
        'air_pressure_at_mean_sea_level', 
        'dew_point_temperature_at_2_metres'
    ]
