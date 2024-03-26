import xarray as xr
import pystac_client
import planetary_computer
from climate_eed.module_threads import get_planetary_item_thr, join_thread, start_thread


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


def var_list_request(repository, collections):

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