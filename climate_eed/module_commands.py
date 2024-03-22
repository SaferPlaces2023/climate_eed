from datetime import datetime
import xarray as xr
import pystac_client
import planetary_computer
import pandas as pd
import numpy as np
import os
from dask.diagnostics import ProgressBar
from climate_eed.module_config import Config, parse_bbox, parse_collections, parse_dates, parse_repository
from climate_eed.module_threads import get_planetary_item_thr, join_thread, start_thread


def list_repo_vars(repository, collections):
    
    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections
    )
    items = search_results.items()
    for item in items:
        signed_item = planetary_computer.sign(item)
        return signed_item.assets.keys()
    return None

def fetch_var(varname=Config.DEFAULT_VARNAME, 
         factor=Config.DEFAULT_FACTOR, 
         bbox=Config.DEFAULT_BBOX, 
         start_date=Config.DEFALUT_START_DATE, 
         end_date=Config.DEFALUT_END_DATE, 
         repository=Config.DEFAULT_REPOSITORY, 
         collections=Config.DEFAULT_COLLECTIONS, 
         query=Config.DEFAULT_QUERY, 
         return_format=Config.DEFAULT_RETURN_FORMAT, 
         out_format=Config.DEFAULT_OUT_FORMAT):

    
    start_date, end_date = parse_dates(start_date, end_date)
    collections = parse_collections(collections)
    bbox = parse_bbox(bbox)
    repository = parse_repository(repository)
    output_ds = None

    # print("vaename",varname)
    # print("factor",factor)
    # print("bbox",bbox)
    # print("start_date",start_date)
    # print("end_date",end_date)
    # print("repository",repository)
    # print("collections",collections)
    # print("query",query)
    # print("return_format",return_format)
    # print("out_format",out_format)

    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections, datetime=[start_date, end_date], query=query
    )
    items = search_results.items()
    threads = []
    for item in items:
        thrd = get_planetary_item_thr(item=item, varname=varname, bbox=bbox, factor=factor)
        start_thread(thrd)
        threads.append(thrd)

    for thrd in threads:
        join_thread(thrd)
        ds_sliced = thrd.get_return_value()
        try:
            if output_ds is None:
                output_ds = ds_sliced
            else:
                output_ds = xr.concat([output_ds, ds_sliced], dim="time")
        except Exception as e:
            print("Exception")
            print(e)

    df = None
    if return_format == "pd":
        with ProgressBar():
            df = output_ds.to_dataframe()
    else:
        df = output_ds
    
    if out_format:
    
        with ProgressBar():
            if out_format == "csv":
                if not df:
                    df = output_ds.to_dataframe()
                df.to_csv("output_data.csv")
                return df
            elif out_format == "nc":
                output_ds.to_netcdf("output_data.nc")

                return output_ds
            
    return df