from datetime import datetime
import json
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

    repository = parse_repository(repository)
    collections = parse_collections(collections)

    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections
    )
    items = search_results.items()
    for item in items:
        signed_item = planetary_computer.sign(item)
        return signed_item.assets.keys()
    return None

def data_request(varname, factor, bbox, start_date, end_date, repository, collections, query):

    output_ds = None

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
    
    return output_ds


def fetch_var(varname=Config.DEFAULT_VARNAME, 
         factor=Config.DEFAULT_FACTOR, 
         bbox=Config.DEFAULT_BBOX, 
         start_date=Config.DEFALUT_START_DATE, 
         end_date=Config.DEFALUT_END_DATE, 
         repository=Config.DEFAULT_REPOSITORY, 
         collections=Config.DEFAULT_COLLECTIONS, 
         query=Config.DEFAULT_QUERY, 
         return_format=Config.DEFAULT_RETURN_FORMAT, 
         fileout=Config.DEFAULT_FILEOUT):

    if isinstance(query, str):
        try:
            query = json.loads(query)
        except json.JSONDecodeError as error:
            print(error)
    
    start_date, end_date = parse_dates(start_date, end_date)
    collections = parse_collections(collections)
    bbox = parse_bbox(bbox)
    repository = parse_repository(repository)

    # print("Varname: ", varname)
    # print("Factor: ", factor)
    # print("Bbox: ", bbox)
    # print("Start Date: ", start_date)
    # print("End Date: ", end_date)
    # print("Repository: ", repository)
    # print("Collections: ", collections)
    # print("Query: ", query, type(query))

    output_ds = data_request(varname, factor, bbox, start_date, end_date, repository, collections, query)
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