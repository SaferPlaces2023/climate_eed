import pandas as pd
from tqdm import tqdm
import xarray as xr
import pystac_client
import planetary_computer
from climate_eed.module_threads import get_planetary_item_thr, get_planetary_model_thr, join_thread, start_thread


def data_request(varname, models, factor, bbox, start_date, end_date, repository, collections, query):
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

    output_ds = None

    
    if "cil-gdpcir-cc0" in collections or "cil-gdpcir-cc-by" in collections:
        catalog = pystac_client.Client.open(
            repository,
            modifier=planetary_computer.sign_inplace,
        )

        search_results = catalog.search(
            collections=collections,
            query=query,
        )

        ensemble = search_results.item_collection()
        datasets_by_model = []
        threads = []
        for item in tqdm(ensemble):
            thrd = get_planetary_model_thr(item=item, varname=varname, bbox=bbox, factor=factor)
            start_thread(thrd)
            threads.append(thrd)

        for thrd in threads:
            join_thread(thrd)
            ds_sliced = thrd.get_return_value()
            try:
                datasets_by_model.append(ds_sliced)
                    
            except Exception as e:
                print("Exception")
                print(e)

        output_ds = xr.concat(
            datasets_by_model,
            dim=pd.Index([ds.attrs["source_id"] for ds in datasets_by_model], name="model"),
            combine_attrs="drop_conflicts",
        )
        if models:    
            output_ds = output_ds.tasmax.sel(
                lon=slice(bbox[0], bbox[2]),
                lat=slice(bbox[1], bbox[3]),
                time=slice(start_date, end_date),
                model=models,
            )
        else:
            output_ds = output_ds.tasmax.sel(
                lon=slice(bbox[0], bbox[2]),
                lat=slice(bbox[1], bbox[3]),
                time=slice(start_date, end_date),
            )
        
    else:
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


def var_list_request(repository, collections):
    """
    Fetches the list of variables available in a STAC repository.
    Args:
        - repository (str): The STAC repository to fetch the data from. Example: "planetary".
        - collections (str): The collections to fetch the data from. Example: "era5-pds".
    Returns:
        - list: The list of variables available in the STAC repository.
    """

    var_list = None
    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections
    )
    items = search_results.items()
    for item in items:
        signed_item = planetary_computer.sign(item)
        var_list = list(signed_item.assets.keys())
    return var_list