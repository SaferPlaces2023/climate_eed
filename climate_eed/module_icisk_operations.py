import pandas as pd
from tqdm import tqdm
import xarray as xr
import pystac_client
from climate_eed.module_threads import get_seasonal_forecast_item_thr, join_thread, start_thread


def icisk_data_request(varname, models, factor, bbox, start_date, end_date, repository, collections, query, additional_params=None):
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

    catalog = pystac_client.Client.open(repository)
    catalog.add_conforms_to("ITEM_SEARCH")
    catalog.add_conforms_to("QUERY")
    search_results = catalog.search(
        collections=collections, datetime=[start_date, end_date], query=query
    )
    items = search_results.items()
    # basin_id = None
    # if additional_params and "basin_id" in additional_params:
    #     basin_id = additional_params["basin_id"]
    threads = []
    for item in items:
        thrd = get_seasonal_forecast_item_thr(item=item, varname=varname, factor=factor)
        start_thread(thrd)
        threads.append(thrd)
    data_arrays = []
    coverage = None
    for thrd in threads:
        join_thread(thrd)
        ds_sliced = thrd.get_return_value()
        data_arrays.append(ds_sliced)
        # try:
        #     if output_ds is None:
        #         output_ds = ds_sliced
        #     else:
        #         output_ds = xr.concat([output_ds, ds_sliced], dim="model")
        # except Exception as e:
        #     print("Exception")
        #     print(e)
    if data_arrays:
        # print("DATA ARRAYS")
        # print(data_arrays)
        # print("**************************************")
        agg_dataset = xr.merge(data_arrays, join='outer')
        # coverage = xarray_to_prs_coverage_json(agg_dataset)
    return agg_dataset
