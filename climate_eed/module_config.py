
from datetime import datetime
import json

class PlanetaryConfig:
    DEFAULT_VARNAME = ""
    DEFAULT_MODELS = ""
    DEFAULT_FACTOR = 1
    DEFAULT_BBOX = ""
    DEFALUT_START_DATE = ""
    DEFALUT_END_DATE = ""
    DEFAULT_REPOSITORY = "planetary"
    DEFAULT_COLLECTIONS = "era5-pds"
    DEFAULT_QUERY = ""
    DEFAULT_FILEOUT = ""
    DEFAULT_RETURN_FORMAT = "xr"
    DEFAULT_VERSION = False
    DEFAULT_LIST_VARS = False
    DEFAULT_LIST_REPOS = False

class CopernicusConfig:
    DEFAULT_VARNAME = ""
    DEFAULT_FACTOR = 1
    DEFAULT_BBOX = ""
    DEFAULT_FILEOUT = ""
    DEFAULT_YEARS = ""
    DEFAULT_MONTH = ""
    DEFAULT_LEADTIME_MONTH = ""
    DEFAULT_RETURN_FORMAT = "xr"
    DEFAULT_VERSION = False


def parse_query(query):
    if isinstance(query, str):
        try:
            query = json.loads(query)
        except json.JSONDecodeError as error:
            print(error)
            return 0
    elif isinstance(query, dict):
        pass
    else:
        print("Invalid query type")
        return 0
    return query
    
def parse_dates(start_date, end_date):
    # Define your start and end dates for the entire period you want to fetch
    date_format = "%d-%m-%Y"
    start_date = datetime.strptime(start_date, date_format).isoformat()
    end_date = datetime.strptime(end_date, date_format).isoformat()
    return start_date, end_date

def parse_repository(repository):
    if(repository == "planetary"):
        repo = "https://planetarycomputer.microsoft.com/api/stac/v1/"
    else:
        print("Unknown repository")
        repo = None
    return repo
    
def parse_bbox(bbox):
    if bbox == "":
        bbox = None
    elif isinstance(bbox, list):
        bbox = [float(x) for x in bbox]
    else:
        bbox = [float(x) for x in bbox.split(",")]
    return bbox

def parse_collections(collections):
    if isinstance(collections, list):
        collections = [str(x) for x in collections]
    elif isinstance(collections, str):
        collections = [str(x) for x in collections.split(",")]
    return collections

# def parse_arguments(repository, key, query_list, collections):

#     query
#     if repository == "planetary":
#         repository_param = "https://planetarycomputer.microsoft.com/api/stac/v1/"
#         if collections == ["era5-pds"]:
#             query_param = {"era5:kind": {"eq": query}}
#         elif collections == ["cil-gdpcir-cc-by"]:
#             # query_1 = "cmip6:source_id=NESM3"
#             # query_2 = "cmip6:experiment_id=ssp585"

#             for query in query_list:
#                 key, value = query.split("=")
#                 query_param[key] = {"eq": value}

#             query_param = {"cmip6:source_id": {"eq": "NESM3"}, "cmip6:experiment_id": {"eq": "ssp585"}},
        
#         print(query)
#         print("------")
        

#         return repository_param, query_param
#     else:
#         print("Unknown repository")
#         return None
    
