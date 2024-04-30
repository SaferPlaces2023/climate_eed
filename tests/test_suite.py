import pandas as pd
import pytest
import xarray as xr
from climate_eed import fetch_var, list_repo_vars


def test_era5_fetch_var():
    """Test the fetch_var function."""
    
    var_ERA5 = "precipitation_amount_1hour_Accumulation"
    start_date_str = "01-01-1995"
    end_date_str = "31-12-1995"
    factor_sel = 1000
    location_str = ["26.0","-31.0","29.0","-29.0"]
    data_ERA5 = fetch_var(varname=var_ERA5,start_date=start_date_str,end_date=end_date_str,factor=factor_sel,bbox=location_str,query={"era5:kind": {"eq": "fc"}})
    
    assert type(data_ERA5) == xr.DataArray
    assert data_ERA5.shape == (8760, 9, 13)


def test_era5_fetch_var_nc_file():
    """Test the fetch_var function with nc file result."""
    
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


def test_era5_fetch_var_pd_df():
    """Test the fetch_var function with pd dataframe as return."""

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

def test_ensemble_fetch_var():
    """Test the fetch_var function."""
    
    var_ensemble = "tasmax"
    start_date_str = "01-01-2095"
    end_date_str = "31-12-2100"
    # factor_sel = 1000
    location_str = ["30.0","9.0","40.0","19.0"]
    collections = ["cil-gdpcir-cc0","cil-gdpcir-cc-by"]
    query = {"cmip6:experiment_id": {"eq": "ssp370"}}
    data_ensemble = fetch_var(varname=var_ensemble,
                          start_date=start_date_str,
                          end_date=end_date_str,
                          collections=collections,
                          bbox=location_str,
                          query=query)
    
    print(data_ensemble)
    print("----------------")
    assert type(data_ensemble) == xr.DataArray
    assert data_ensemble.shape == (20, 2189, 40, 40)


def test_ensemble_fetch_var_sel_model():
    """Test the fetch_var function with selected models."""
    
    var_ensemble = "tasmax"
    start_date_str = "01-01-2095"
    end_date_str = "31-12-2100"
    # factor_sel = 1000
    location_str = ["30.0","9.0","40.0","19.0"]
    collections = ["cil-gdpcir-cc0","cil-gdpcir-cc-by"]
    query = {"cmip6:experiment_id": {"eq": "ssp370"}}
    models = ["GFDL-ESM4"]
    data_ensemble = fetch_var(varname=var_ensemble,
                          start_date=start_date_str,
                          end_date=end_date_str,
                          collections=collections,
                          bbox=location_str,
                          query=query,
                          models=models)
    
    print(data_ensemble)
    print("----------------")
    assert type(data_ensemble) == xr.DataArray
    assert data_ensemble.shape == (1, 2189, 40, 40)


def test_seasonal_forecasts_fetch_var():
    """Test the fetch_var function for seasonal_forecasts collection."""
    
    # var_ensemble = "tasmax"
    # start_date_str = "01-01-2095"
    # end_date_str = "31-12-2100"
    # # factor_sel = 1000
    # location_str = ["30.0","9.0","40.0","19.0"]
    # collections = ["cil-gdpcir-cc0","cil-gdpcir-cc-by"]
    # query = {"cmip6:experiment_id": {"eq": "ssp370"}}
    # models = ["GFDL-ESM4"]
    collections = ["seasonal_forecasts"]
    data_seasonal_forecasts = fetch_var(collections=collections,repository="icisk",additional_params={"basin_id": "301515"})
    
    print(data_seasonal_forecasts)
    print("----------------")
    assert type(data_seasonal_forecasts) == xr.DataArray
    assert data_seasonal_forecasts.shape == (2, 213)