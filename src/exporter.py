import asyncio
from pathlib import Path
from typing import List, Dict, Tuple
from multiprocessing import Pool

import requests
import aiohttp
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .utils import (
    SAS_ENDPOINT,
    SAVE_FILE,
    OUT_DIR,
    ANNOTATIONS_DIR,
    json_read_if_exists,
    json_write,
    json_parse
)
from .logger import logger


# NOTE: max number of async processes to run simultaneously. otherwise, we can exceed what our OS can handle.
# connector = aiohttp.TCPConnector(limit=32)

def manifest_uri_to_short_id(manifest_uri: str) -> str:
    return manifest_uri.split("/")[-2]

def iiif_collection_to_manifest_uri_list(iiif_collection: Dict) -> List[str]:
    return [
        m["@id"]
        for m in iiif_collection["manifests"]
        if m["@type"] == "sc:Manifest"
    ]

async def fetch_to_dict(session:aiohttp.ClientSession, url: str, raise_=True) -> Dict:
    try:
        async with session.get(url) as response:
            if raise_:
                response.raise_for_status()
            r_text = await response.text()
            return json_parse(r_text)
    except Exception as e:
        if raise_:
            raise Exception
        return {}


class SasExporter():
    save_file: Path
    def __init__(self):
        # check that the endpoint actually exists
        try:
            requests.get(f"{SAS_ENDPOINT}/manifests")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            logger.error(f"Endpoint {SAS_ENDPOINT} could not be reached ! Exiting...")
            exit(1)

        self.endpoint = SAS_ENDPOINT
        self.annotations_dir = ANNOTATIONS_DIR
        self.out_dir = OUT_DIR
        self.save_file = SAVE_FILE
        self.save_data, exists = json_read_if_exists(self.save_file)
        self.manifests: List[str] = []

        logger.info(f"Initiated SasExporter successfully.")
        if exists:
            logger.info(f"Skipping {len(list(self.save_data.keys()))} pre-fetched manifests")
        else:
            logger.info(f"No pre-fetched manifests to load. Everything will be exported.")
        return

    @property
    def endpoint_manifests(self) -> str:
        return f"{self.endpoint}/manifests"

    def endpoint_annotations(self, manifest_short_id: str) -> str:
        # search-api endpoint returns all annotations for a manifest, paginated.
        return f"{self.endpoint}/search-api/{manifest_short_id}/search"

    def annotation_list_path(self, manifest_short_id: str) -> str|Path:
        return self.annotations_dir / f"{manifest_short_id}.json"

    def write_annotation_list(self, data, fp: str|Path) -> "SasExporter":
        json_write(data, fp)
        return self

    def write_save_data(self) -> "SasExporter":
        json_write(self.save_data or {}, self.save_file)
        return self

    # TODO error handling if request fails
    async def fetch_manifests(self) -> "SasExporter":
        manifests = []
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=32)
        ) as session:
            collection = await fetch_to_dict(session, self.endpoint_manifests)
            manifests = iiif_collection_to_manifest_uri_list(collection)
        logger.info(f"Found {len(manifests)} for which to extract annotations.")
        self.manifests = manifests
        return self

    # TODO pagination
    async def fetch_annotations(self, session: aiohttp.ClientSession, manifest_uri: str) -> None:
        """
        pipeline to download a single annotation_list
        """
        manifest_short_id = manifest_uri_to_short_id(manifest_uri)
        search_api_endpoint = self.endpoint_annotations(manifest_short_id)
        out_path = self.annotation_list_path(manifest_short_id)
        data = await fetch_to_dict(session, search_api_endpoint)
        self.write_annotation_list(data, out_path)
        self.save_data[manifest_uri] = str(out_path)
        # print(self.save_data)

    # TODO error handling in request fails
    # NOTE see if there's a problem with all fetch_annotations writing to self.save_data
    async def fetch_annotations_all(self) -> "SasExporter":
        # self.save_data is a dict or { <manifest URI>: <path to downloaded annotationList> }
        manifests_to_download = [
            m for m in self.manifests if m not in self.save_data.keys()
        ]
        tasks = []
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=32)
        ) as session:
            for m_uri in manifests_to_download:
                tasks.append(self.fetch_annotations(session, m_uri))
            await tqdm_asyncio.gather(
                    *tasks,
                    total=len(manifests_to_download),
                    desc=f"DL annotation lists"
                )
        return self

    async def pipeline_async(self) -> "SasExporter":
        await self.fetch_manifests()
        await self.fetch_annotations_all()
        return self

    def pipeline(self) -> "SasExporter":
        try:
            asyncio.run(self.pipeline_async())
        finally:
            self.write_save_data()
        return self
