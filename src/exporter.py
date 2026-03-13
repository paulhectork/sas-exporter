import asyncio
from pathlib import Path
from typing import List, Dict, Tuple
from multiprocessing import Pool

import aiohttp
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .utils import (
    SAS_ENDPOINT,
    SAVE_OK_FILE,
    OUT_DIR,
    ANNOTATIONS_DIR,
    SAVE_ERR_FILE,
    MAX_CONNECTIONS,
    json_read_if_exists,
    json_write,
    json_parse
)
from .logger import logger

def manifest_uri_to_short_id(manifest_uri: str) -> str:
    return manifest_uri.split("/")[-2]

def iiif_collection_to_manifest_uri_list(iiif_collection: Dict) -> List[str]:
    return [
        m["@id"]
        for m in iiif_collection["manifests"]
        if m["@type"] == "sc:Manifest"
    ]

async def fetch_to_dict(session:aiohttp.ClientSession, url: str) -> Dict:
    import random
    if random.randint(0, 10) == 5:
        raise ValueError
    async with session.get(url) as response:
        r_text = await response.text()
        return json_parse(r_text)


class SasExporter():
    save_ok_file: Path
    def __init__(self):
        self.endpoint = SAS_ENDPOINT
        self.annotations_dir = ANNOTATIONS_DIR
        self.out_dir = OUT_DIR
        self.save_ok_file = SAVE_OK_FILE
        self.max_connections = MAX_CONNECTIONS
        # mapping of { <manifest_uri>: <path to downloaded annotation list>? }
        self.save_data, exists = json_read_if_exists(self.save_ok_file)
        # NOTE: we overwrite contents of SAVE_ERR_FILE from 1 run to another: we retry a download on every failed annotation list extraction.
        self.save_err_file = SAVE_ERR_FILE
        # list of manifests to download
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
        # NOTE: split self.save_data in 2 items: one with successful saves, one with errors.
        # write both to file. in `self.fetch_annotations_from_manifest_uri`, if there's a DL error, path is set to None
        save_ok_data = {}
        save_err_data = []
        for manifest_uri, path in self.save_data.items():
            print(manifest_uri, path)
            if path is not None:
                save_ok_data[manifest_uri] = path
            else:
                save_err_data.append(manifest_uri)
        json_write(save_ok_data, self.save_ok_file)
        json_write(save_err_data, self.save_err_file)
        return self

    # TODO error handling if request fails
    async def fetch_manifests(self) -> "SasExporter":
        manifests = []
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_connections)
        ) as session:
            collection = await fetch_to_dict(session, self.endpoint_manifests)
            manifests = iiif_collection_to_manifest_uri_list(collection)
        logger.info(f"Found {len(manifests)} for which to extract annotations.")
        self.manifests = manifests
        return self

    # TODO pagination
    async def fetch_annotations_from_manifest_uri(
        self,
        session: aiohttp.ClientSession,
        manifest_uri: str
    ) -> Tuple[str, str|None]:
        """
        pipeline to download a single annotation_list
        """
        manifest_short_id = manifest_uri_to_short_id(manifest_uri)
        search_api_endpoint = self.endpoint_annotations(manifest_short_id)
        out_path = self.annotation_list_path(manifest_short_id)
        try:
            data = await fetch_to_dict(session, search_api_endpoint)
            self.write_annotation_list(data, out_path)
            return manifest_uri, str(out_path)
        except Exception as e:
            logger.error(f"Failed to fetch annotations for manifest {manifest_uri}: {e}")
            return manifest_uri, None

    async def fetch_annotations(self) -> "SasExporter":
        # self.save_data is a dict or { <manifest URI>: <path to downloaded annotationList> }
        manifests_to_download = [
            m for m in self.manifests if m not in self.save_data.keys()
        ]
        tasks = []
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_connections)
        ) as session:
            tasks = [
                self.fetch_annotations_from_manifest_uri(session, m_uri)
                for m_uri in manifests_to_download
            ]
            results = await tqdm_asyncio.gather(
                *tasks,
                total=len(manifests_to_download),
                desc=f"DL annotation lists"
            )
            self.save_data = { m_uri: path for m_uri, path in results }
        return self

    async def pipeline_async(self) -> "SasExporter":
        await self.fetch_manifests()
        await self.fetch_annotations()
        return self

    def pipeline(self) -> "SasExporter":
        try:
            asyncio.run(self.pipeline_async())
        finally:
            self.write_save_data()
        return self
