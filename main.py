import re
import textwrap
import functools
from typing import Literal, Callable

import click
from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.logger import logger
from src.exporter_annotations import export as run_export_annotations
from src.exporter_manifests import export as run_export_manifests
from src.test_pagination import test_pagination as run_test_pagination
from src.clean_manifest_errors import clean_manifest_errors as run_clean_manifest_errors
from src.migrate import migrate as run_migrate
from src.output_analysis import output_analysis as run_output_analysis
from src.anno2mani import anno2mani as run_anno2mani

export_retry_help_values = "one of: 'all'|'timeout'|'http'|'http:XXX, where '*' means retry all errors and 'XXX' is an HTTP error code"

def export_retry_validator(ctx, param, value):
    if value is not None and not re.match(r"^(all|timeout|http(:\d{3})?)$", value):
        print(f"ERROR: Wrong value for parameter '{param}': '{value}'. Must be {export_retry_help_values}")
        exit(1)
    return value


# commonly shared argument to define a datatype. https://stackoverflow.com/a/70852267
def datatype_argument(func: Callable) -> Callable:
    @click.argument(
        "datatype",
        type=click.Choice(["manifests", "annotations"]),
        required=True
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group()
def cli():
    logger.info("*" * 50)


@cli.command()
@datatype_argument
@click.option(
    "-r", "--retry",
    help=f"retry exports for manifests that failed at a previous fetch for a specific error type",
    callback=export_retry_validator,
)
@click.option(
    "-e", "--export-manifests",
    help="export manifests as well as annotations. has no effect if 'argument' is 'manifests'",
    is_flag=True,
    type=click.BOOL,
    default=False
)
def export(
    datatype: Literal["manifests","annotations"],
    retry: str|None,
    export_manifests: bool = False
):
    """
    export data from a live SAS instance

    \b
    - if 'argument' is 'annotations', export all annotations from an SAS endpoint
    - if 'argument' is 'manifests', export all manifests indexed in an SAS endpoint
        (i.e., manifests mentionned in $SAS_ENDPOINT/manifests/)

    \b
    if "-r" "--retry" is specified, only attempt to download data
    for manifests that failed at a previous step. possible values or retry are:
    - "all" (refetch for all errors),
    - "timeout" (refetch for timeout errors),
    - "http" (refetch for all HTTP errors),
    - "http:XXX" (where XXX is an HTTP status code: refetch only HTTP
        errors with a specific status code, i.e., 500.)

    if the endpoint of your IIIF Manifest provider has changed and those changes
    have not been reflected in your SAS, use the EXPORT_STRATEGY and IIIF_HOST_REPL
    env variables (and see their doc in .env.template).
    """
    if datatype == "annotations":
        run_export_annotations(retry, export_manifests)
    else:
        run_export_manifests(retry)


@cli.command()
def test_pagination():
    """
    test the concat of paginated results in exports

    after exporting AnnotationLists, test that the concatenation of
    paginated AnnotationLists into a single AnnotationList worked
    """
    run_test_pagination()


@cli.command()
def clean_manifest_error():
    """
    build a list of AnnoLists with fetchable manifests

    validate exported AnnotationLists by ensuring their target manifest(s)
    can be fetched. save paths to valid AnnotationLists to a file.

    NOTE that this step is useless if the export was made with --strategy="canvas".
    """
    run_clean_manifest_errors()


@cli.command()
@datatype_argument
def migrate(datatype: Literal["annotations", "manifests"]):
    """
    aikon-specific process to migrate data structures
    """
    run_migrate(datatype)


@cli.command()
@datatype_argument
def output_analysis(datatype: Literal["manifests","annotations"]):
    """
    get a summary of an export: results, errors

    \b
    argument <datatype> defines which output to analyse:
    - manifest: output of manifest extraction
    - annotation: output of annotation extraction
    """
    run_output_analysis(datatype)

@cli.command()
@click.argument("step", type=click.Choice(["pre-migrate", "post-migrate"]), required=True)
def anno2mani(step: Literal["pre-migrate","post-migrate"]):
    """
    check that annotations are mappable to manifests

    after exporting manifests and annotations, check the target of each
    annotation to ensure it can be mapped to an exported manifest

    \b
    - use step="pre-migrate" to check annotations before running "migrate"
    - use step="post-migrate" to check annotations after running "migrate"
    """
    run_anno2mani(step)

if __name__ == "__main__":
    cli()
