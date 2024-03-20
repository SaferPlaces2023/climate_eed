import json
import click
from datetime import datetime, timedelta
from dask.diagnostics import ProgressBar
from climate_eed.module_commands import fetch_var, list_repo_vars
from climate_eed.welcome import get_version


@click.command()
@click.option("--varname", type=click.STRING, required=False, default="", help="The variable name to fetch")
@click.option("--factor", type=click.INT, required=False, default=1, help="The factor to multiply the variable by")
@click.option("--bbox", type=click.STRING, required=False, default="", help="The bounding box to fetch")
@click.option("--start_date", type=click.STRING, required=False, default="", help="The start date to fetch")
@click.option("--end_date", type=click.STRING, required=False, default="", help="The end date to fetch")
@click.option("--repository", type=click.STRING, required=False, default="", help="The repository to fetch from")
@click.option("--collections", type=click.STRING, required=False, default="", help="The collections of the repository to fetch from")
@click.option("--query", type=click.STRING, required=False, default="", help="The query to fetch from the repository")
@click.option("--out_format", type=click.STRING, required=False, default="", help="The output format to save the results to. Can be pandas DataFrame (pd) or xarray Dataset (xr). Defaults to pandas DataFrame.")
@click.option("--version", is_flag=True, required=False, default=False, help="Print version")
@click.option("--list_vars", is_flag=True, required=False, default=False, help="List available variables in the repository")
def main(varname, factor, bbox, start_date, end_date, repository, collections, query, out_format, version, list_vars):
    print("out_format",out_format)
    if version:
        click.echo("climate_eed v%s" % get_version())
        return 0
    
    if list_vars:
        repo_vars = list_repo_vars(repository, collections)
        click.echo("Available variables: ")
        click.echo(repo_vars)
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
    print("RESULT")
    print(df)

    
    if out_format:
        with ProgressBar():
            if out_format == "pd":
                df.to_dataframe().to_csv("output.csv")
            elif out_format == "xr":
                df.to_netcdf("output.nc")
    return df
