import json
import click
import xarray as xr
import pystac_client
import planetary_computer
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from dask.diagnostics import ProgressBar

from climate_eed.welcome import get_version


def fetch_var(varname, factor, bbox, start_date, end_date, repository, collections, query):
    
    output_ds = None
    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections, datetime=[start_date, end_date], query=query
    )
    items = search_results.items()
    for item in items:
        signed_item = planetary_computer.sign(item)
        asset = signed_item.assets.get(varname)
        if asset:
            dataset = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
            ds = dataset[varname]
            ds_sliced = ds.sel(lat=slice(bbox[3],bbox[1]), lon=slice(bbox[0],bbox[2])) * factor
            if output_ds is None:
                output_ds = ds_sliced
            else:
                output_ds = xr.concat([output_ds, ds_sliced], dim="time")

    return output_ds

@click.command()
@click.option("--varname", type=click.STRING, required=False, default="", help="The variable name to fetch")
@click.option("--factor", type=click.INT, required=False, default=1, help="The factor to multiply the variable by")
@click.option("--bbox", type=click.STRING, required=False, default="", help="The bounding box to fetch")
@click.option("--start_date", type=click.STRING, required=False, default="", help="The start date to fetch")
@click.option("--end_date", type=click.STRING, required=False, default="", help="The end date to fetch")
@click.option("--repository", type=click.STRING, required=False, default="", help="The repository to fetch from")
@click.option("--collections", type=click.STRING, required=False, default="", help="The collections of the repository to fetch from")
@click.option("--query", type=click.STRING, required=False, default="", help="The query to fetch from the repository")
@click.option("--out_format", type=click.STRING, required=False, default="pd", help="The output format to save the results to. Can be pandas DataFrame (pd) or xarray Dataset (xr). Defaults to pandas DataFrame.")
@click.option("--version", is_flag=True, required=False, default=False,
              help="Print version.")
def main(varname, factor, bbox, start_date, end_date, repository, collections, query, out_format, version):

    if version:
        click.echo("climate_eed v%s" % get_version())
        return 0

    # Define your start and end dates for the entire period you want to fetch
    date_format = "%d-%m-%Y"
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    
    query = {"era5:kind": {"eq": query}}
    bbox = [float(x) for x in bbox.split(",")]
    collections = [str(x) for x in collections.split(",")]

    df = fetch_var(
        varname=varname, 
        factor=factor, 
        bbox=bbox, 
        start_date=start_date.isoformat(), 
        end_date=end_date.isoformat(), 
        repository=repository, 
        collections=collections,
        query=query
    )

    with ProgressBar():
        if out_format == "pd":
            df.to_dataframe().to_csv("output.csv")
        elif out_format == "xr":
            df.to_netcdf("output.nc")
