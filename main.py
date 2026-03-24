import argparse
import textwrap

from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.logger import logger
from src.exporter import export
from src.test_pagination import test_pagination
from src.clean_manifest_errors import clean_manifest_errors
from src.clean_anno_to_digit import clean_anno_to_digit

def cli():
    logger.info("*" * 50)
    parser = argparse.ArgumentParser(
        prog="sas-exporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
        CLI to export data from a SimpleAnnotationServer endpoint

        commands:
            export                   export all annotations from a SimpleAnnotationServer endpoint
            test_pagination          after exporting AnnotationLists, test that the concatenation of
                                     paginated AnnotationLists into a single AnnotationList worked
            clean_manifest_errors    validate exported AnnotationLists by ensuring their
                                     target manifest(s) can be fetched. save paths to valid
                                     AnnotationLists to a file
            clean_anno_to_digit      aikon-specific process to migrate manifest structure
        """),
        usage="uv run main.py [export|clean_manifest_errors|test_pagination|clean_anno_to_digit]"
    )
    parser.add_argument(
        "command",
        choices=["export", "test_pagination", "clean_manifest_errors", "clean_anno_to_digit"]
    )

    args = parser.parse_args()
    if args.command == "export":
        export()
    elif args.command == "clean_manifest_errors":
        clean_manifest_errors()
    elif args.command == "clean_anno_to_digit":
        clean_anno_to_digit()
    elif args.command == "test_pagination":
        test_pagination()

if __name__ == "__main__":
    cli()
