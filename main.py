import re
from typing import Literal

import click
from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.logger import logger
from src.utils import URL_ROOT_REGEX
from src.exporter import export as run_export
from src.test_pagination import test_pagination as run_test_pagination
from src.clean_manifest_errors import clean_manifest_errors as run_clean_manifest_errors
from src.anno_to_digit import anno_to_digit as run_anno_to_digit

@click.group()
def cli():
    logger.info("*" * 50)

@cli.command()
def export():
    """
    export all annotations from an SAS endpoint

    if the endpoint of your IIIF Manifest provider has changed and those changes
    have not been reflected in your SAS, use the EXPORT_STRATEGY and IIIF_SCHEME_REPL
    env variables (and see their doc in .env.template).
    """
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
    can be fetched. save paths to valid AnnotationLists to a file.

    NOTE that this step is useless if the export was made with --strategy="canvas".
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
