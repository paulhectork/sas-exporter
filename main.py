import textwrap

import click
from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.logger import logger
from src.exporter import export as run_export
from src.test_pagination import test_pagination as run_test_pagination
from src.clean_manifest_errors import clean_manifest_errors as run_clean_manifest_errors
from src.anno_to_digit import anno_to_digit as run_anno_to_digit

@click.group()
def cli():
    logger.info("*" * 50)

@cli.command()
@click.option(
    "-s", "--strategy",
    default="search-api",
    type=click.Choice(["search-api", "canvas"]),
    help="which SAS url to fetch annotations with"
)
@click.option(
    "-a", "--alt-url-root",
    type=click.STRING,
    help="alternative root URL of manifests in case it has changed (i.e., 'iiif.example1.com' -> 'iiif.example2.com')"
)
def export(strategy: str, alt_url_root: str):
    """
    export all annotations from an SAS endpoint

    "--strategy" and "--old-url-root" are both useful when the root URL of a
    manifest provider has changed since manifests have been indexed in SAS.
    indeed, when a manifest provider changes its URL, changes aren't mirrored in SAS
    and it can leave orphanned annotations.

    the --strategy option is used to determine which SAS route to use to export annotations:
    with "search-api", use "/search-api/" (1 query per manifest).
    with "canvas", use "/annotation/search/" (1 query per canvas of the manifest).
    "canvas" takes MUCH longer, but can retrieve more annotations if the root of the
    target URL has changed

    if defining an "--alt-url-root" in conjunction with "canvas" strategy, for each canvas, query using both URLs.
    this takes even more time but ensures a maximal number of orphaned annotations are retrieved.
    """
    print(strategy, alt_url_root)
    run_export()

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
    can be fetched. save paths to valid AnnotationLists to a file
    """
    run_clean_manifest_errors()

@cli.command()
def anno_to_digit():
    """
    aikon-specific process to migrate manifest structure
    """
    run_anno_to_digit()

if __name__ == "__main__":
    cli()
#