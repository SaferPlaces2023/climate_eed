import json
import re
import sys
import click
from datetime import datetime, timedelta
from climate_eed.module_commands import fetch_var, list_repo_vars
# from climate_eed.module_config import parse_arguments
from climate_eed.welcome import get_version
from climate_eed.module_config import PlanetaryConfig


def preprocess_json(json_str):
    # Replace single quotes with double quotes
    json_str = json_str.replace("'", '"')
    
    # Enclose keys without quotes in double quotes
    # json_str = re.sub(r'(\w+)(:)', r'"\1"\2', json_str)

    # Replace occurrences of ": {" with ": {"
    json_str = re.sub(r'":\s+{', '": {', json_str)


    # Return the preprocessed JSON string
    return json_str

@click.command()
@click.option("--varname", type=click.STRING, required=False, default=PlanetaryConfig.DEFAULT_VARNAME, help="The variable name to fetch. Example: precipitation_amount_1hour_Accumulation")
@click.option("--models", type=click.STRING, required=False, default=PlanetaryConfig.DEFAULT_MODELS, help="The models to fetch from. Example: GFDL-ESM4")
@click.option("--factor", type=click.INT, required=False, default=PlanetaryConfig.DEFAULT_FACTOR, help="The factor to multiply the variable by. Example: 1000")
@click.option("--bbox", type=click.STRING, required=False, default=PlanetaryConfig.DEFAULT_BBOX, help="The bounding box to fetch with format min_lon,min_lat,max_lon,max_lat. Example: 6.75,36.75,18.28,47.00")
@click.option("--start_date", type=click.STRING, required=False, default=PlanetaryConfig.DEFALUT_START_DATE, help="The start date to fetch with format dd-mm-yyyy. Example: 01-01-2020")
@click.option("--end_date", type=click.STRING, required=False, default=PlanetaryConfig.DEFALUT_END_DATE, help="The end date to fetch with format dd-mm-yyyy. Example: 01-01-2021")
@click.option("--repository", type=click.STRING, required=False, default=PlanetaryConfig.DEFAULT_REPOSITORY, help="The repository to fetch from: Example https://planetarycomputer.microsoft.com/api/stac/v1/")
@click.option("--collections", type=click.STRING, required=False, default=PlanetaryConfig.DEFAULT_COLLECTIONS, help="The collections of the repository to fetch from. Example: era5-pds")
@click.option("--query", type=click.STRING, required=False, default="", help="The query to fetch from the repository")
@click.option("--fileout", type=click.STRING, required=False, default="", help="The file to save the output to, must have extension .csv or .nc. Example: output.nc or out.csv")
@click.option("--return_format", type=click.STRING, required=False, default="xr", help="The output format to return the results in. Can be pandas dataframe (pd) or xarray dataset (xr). Default is xarray dataset (xr).")
@click.option("--version", is_flag=True, required=False, default=False, help="Print version and exit.")
@click.option("--list_vars", is_flag=True, required=False, default=False, help="List available variables in the repository. Requires --repository and --collections.")
@click.option("--list_repos", is_flag=True, required=False, default=False, help="List available repositories. Requires --repository and --collections.")
@click.option("--additional_params", is_flag=True, required=False, default="", help="Additional parameters to pass to the query. Must be a JSON string. Example: {'basin_id': '1234'}")
def main(varname=PlanetaryConfig.DEFAULT_VARNAME, 
         models=PlanetaryConfig.DEFAULT_MODELS,
         factor=PlanetaryConfig.DEFAULT_FACTOR, 
         bbox=PlanetaryConfig.DEFAULT_BBOX, 
         start_date=PlanetaryConfig.DEFALUT_START_DATE, 
         end_date=PlanetaryConfig.DEFALUT_END_DATE, 
         repository=PlanetaryConfig.DEFAULT_REPOSITORY, 
         collections=PlanetaryConfig.DEFAULT_COLLECTIONS, 
         query=PlanetaryConfig.DEFAULT_QUERY, 
         return_format=PlanetaryConfig.DEFAULT_RETURN_FORMAT, 
         fileout=PlanetaryConfig.DEFAULT_FILEOUT, 
         version=PlanetaryConfig.DEFAULT_VERSION, 
         list_vars=PlanetaryConfig.DEFAULT_LIST_VARS, 
         list_repos=PlanetaryConfig.DEFAULT_LIST_REPOS,
         additional_params=None):

    if version:
        click.echo("climate_eed v%s" % get_version())
        return 0
    
    if list_vars:
        repo_vars = list_repo_vars(repository, collections)
        click.echo("Available variables: ")
        click.echo(repo_vars)
        return 0

    df = fetch_var(
        varname=varname, 
        models=models,
        factor=factor, 
        bbox=bbox, 
        start_date=start_date, 
        end_date=end_date, 
        repository=repository, 
        collections=collections,
        query=query,
        fileout=fileout,
        additional_params=additional_params,
    )
    
    return df
