import planetary_computer
import pystac_client
import xarray as xr

def list_var(repository, collections):
    
    output_ds = None
    catalog = pystac_client.Client.open(repository)
    search_results = catalog.search(
        collections=collections
    )
    items = search_results.items()
    for item in items:
        signed_item = planetary_computer.sign(item)
        asset = signed_item.assets.get("precipitation_amount_1hour_Accumulation")
        print(signed_item.assets.keys())

        if asset:
            dataset = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
            print(dataset)
            # ds = dataset[varname]
            # ds_sliced = ds.sel(lat=slice(bbox[3],bbox[1]), lon=slice(bbox[0],bbox[2])) * factor
            # if output_ds is None:
            #     output_ds = ds_sliced
            # else:
            #     output_ds = xr.concat([output_ds, ds_sliced], dim="time")

        return signed_item.assets.keys()


vars = list_var("https://planetarycomputer.microsoft.com/api/stac/v1/", "era5-pds")

print(vars)