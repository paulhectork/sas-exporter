from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

import aiohttp
import asyncio
from orjson import JSONDecodeError
from tqdm.asyncio import tqdm_asyncio

from src.utils import (
    ANNOTATIONS_DIR,
    MAX_CONNECTIONS,
    fetch_to_dict,
    json_read_from_dir,
    make_session
)
from src.logger import logger

async def validate_manifest(session: aiohttp.ClientSession, manifest_url: str) -> str|None:
    """
    returns manifest_url if it could be fetched, None otherwise.
    if there's a JSONDecodeError, the manifest could not be fetched
    """
    try:
        await fetch_to_dict(session, manifest_url)
        return manifest_url
    except JSONDecodeError:
        return None

async def clean_manifest_errors():
    # extract a list of all unique manifest URIs from all annotation targets in all annotations in ANNOTATIONS_DIR
    # NOTE: this assumes that annotation["on"] is a simple string, not IIIF object.
    all_manifest_urls = []
    annotation_list_mapper = {}
    for fp, annotation_list in json_read_from_dir(ANNOTATIONS_DIR):
        if len(annotation_list.keys()):
            manifest_urls = set([
                "/".join( anno["on"].split("/")[:-2] ) + "/manifest.json"
                for anno in annotation_list.get("resources", [])
            ])
            annotation_list_mapper[fp] = manifest_urls
            all_manifest_urls.extend(manifest_urls)
    all_manifest_urls = set(all_manifest_urls)

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
        print(valid_manifest_urls)
    # DONE 1: mapper that maps an annotationList to all manifest URLs it contains
    # TODO 2: generate a list of full paths to annotationLists that contain AT LEAST a valid URL
    # TODO 3: write list to file as "\n" separated
    # TODO 4: clean_manifest_errors.py and test.py should both be called by main.py:
    # main should have 3 possible subcommands, export, test_pagination and clean_manifest_errors

if __name__ == "__main__":
    asyncio.run(clean_manifest_errors())

