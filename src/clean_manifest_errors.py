import aiohttp
import asyncio
from orjson import JSONDecodeError
from tqdm.asyncio import tqdm_asyncio

from .utils import (
    ANNOTATIONS_DIR,
    MAX_CONNECTIONS,
    OUT_DIR,
    fetch_to_json,
    json_read_from_dir,
    make_session
)
from .logger import logger

STEP_NAME = "clean_manifest_errors"

async def validate_manifest(session: aiohttp.ClientSession, manifest_url: str) -> str|None:
    """
    returns manifest_url if it could be fetched, None otherwise.
    if there's a JSONDecodeError, the manifest could not be fetched
    """
    try:
        await fetch_to_json(session, manifest_url)
        return manifest_url
    except JSONDecodeError:
        return None

async def pipeline():
    # extract a list of all unique manifest URIs from all annotation targets in all annotations in ANNOTATIONS_DIR
    # NOTE: this assumes that annotation["on"] is a simple string, not IIIF object.
    logger.info("Building an index of IIIF Manifest URLs")
    all_manifest_urls = []
    all_annotation_list_mapper = {}
    for fp, annotation_list in json_read_from_dir(ANNOTATIONS_DIR):
        if len(annotation_list.keys()):
            manifest_urls = set([
                "/".join( anno["on"].split("/")[:-2] ) + "/manifest.json"
                for anno in annotation_list.get("resources", [])
            ])
            all_annotation_list_mapper[fp] = manifest_urls
            all_manifest_urls.extend(manifest_urls)
    all_manifest_urls = set(all_manifest_urls)

    logger.info("2. Asserting manifests are accessible through HTTP(s)")
    valid_manifest_urls = []
    async with make_session(MAX_CONNECTIONS) as session:
        tasks = [
            validate_manifest(session, manifest_url)
            for manifest_url in all_manifest_urls
        ]
        results = await tqdm_asyncio.gather(
            *tasks,
            total=len(all_manifest_urls),
            desc="Validating manifests"
        )
        valid_manifest_urls = [ m for m in results if m is not None ]
    # extract paths to annotation lists that point to at least 1 manifest can be fetched
    valid_annotation_list = [
        fp
        for fp, manifest_urls in all_annotation_list_mapper.items()
        if any(m in valid_manifest_urls for m in manifest_urls)
    ]

    out_path = OUT_DIR / "annotationlists_valid.txt"
    logger.info(f"Saving valid annotation lists to {out_path}")
    txt_out = "\n".join(str(fp) for fp in valid_annotation_list);
    with open(out_path, mode="w") as fh:
        fh.write(txt_out)


def clean_manifest_errors():
    """
    validate AnnotationLists by ensuring their target manifest(s) can be fetched.
    for each AnnotationList, we assert that it point to at least 1 IIIF manifest
    that is accessible through HTTP(s).
    paths to valid AnnotationLists are saved to $OUT_DIR/annotationlists_valid.txt
    """
    logger.info(f"RUNNING: {STEP_NAME}")
    asyncio.run(pipeline())
    logger.info(f"COMPLETED: {STEP_NAME}  (* ´ ▽ ` *)")

