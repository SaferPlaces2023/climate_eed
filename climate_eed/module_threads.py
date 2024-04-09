import inspect
import threading
import planetary_computer
import xarray as xr 

class ThreadReturn(threading.Thread):
    def __init__(self, target, args=None, kwargs=None):
        """
        awslambda - call a lambda function
        """
        super().__init__()
        self.target = target
        self.event = {}
        if args is not None:
            argnames = inspect.getfullargspec(self.target).args
            self.event = dict(zip(argnames, args))
        if kwargs is not None:
            argnames = inspect.getfullargspec(self.target).args
            for key in kwargs:
                if key in argnames:
                    self.event[key] = kwargs[key]
        self.result = None

    def run(self):
        """
        run - thread run method
        """
        response = self.target(**self.event)
        self.result = response
        return self.result

    def get_return_value(self):
        return self.result


def get_planetary_item(item, varname, bbox, factor):
    output_ds = None
    signed_item = planetary_computer.sign(item)
    asset = signed_item.assets.get(varname)
    if asset:
        dataset = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
        ds = dataset[varname]
        if bbox:
            ds = ds.sel(lat=slice(bbox[3],bbox[1]), lon=slice(bbox[0],bbox[2])) * factor
        output_ds = ds
    return output_ds


def get_planetary_model(item, varname, bbox, factor):
    output_ds = None
    asset = item.assets[varname]

    if asset:    
        ds = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
        output_ds = ds
    return output_ds


def get_planetary_item_thr(item, varname, bbox, factor):
    thread = ThreadReturn(target=get_planetary_item, kwargs={"item":item,"varname":varname,"bbox":bbox,"factor":factor})
    return thread


def get_planetary_model_thr(item, varname, bbox, factor):
    thread = ThreadReturn(target=get_planetary_model, kwargs={"item":item,"varname":varname,"bbox":bbox,"factor":factor})
    return thread


def start_thread(thread,comnplete_thread_event=None):
    if comnplete_thread_event:
        comnplete_thread_event.wait()
    thread.start()
    completed_event = threading.Event()
    return completed_event


def join_thread(thread):
    thread.join()