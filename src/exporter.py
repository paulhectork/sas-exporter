import asyncio
from pathlib import Path
from typing import List, Dict, Tuple, Literal

from tqdm.asyncio import tqdm_asyncio

from .utils import (
    SAS_ENDPOINT,
    SAVE_OK_FILE,
    OUT_DIR,
    ANNOTATIONS_DIR,
    SAVE_ERR_FILE,
    MAX_CONNECTIONS,
    EXPORTER_STRATEGY_VALUES,
    json_read_if_exists,
    json_write,
    fetch_to_dict,
    make_session
)
from .logger import logger

STEP_NAME = "export"

def manifest_uri_to_short_id(manifest_uri: str) -> str:
    return manifest_uri.split("/")[-2]

def iiif_collection_to_manifest_uri_list(iiif_collection: Dict) -> List[str]:
    return [
        m["@id"]
        for m in iiif_collection["manifests"]
        if m["@type"] == "sc:Manifest"
    ]


def fix_next_page_url(url: str|None) -> str|None:
    """
    NOTE dirty fix for annotation_list.next URLs:
    the "next" URLs are defined relative to a base URL set in SAS that can be different from the actual endpoint:
    in the case of aikon.enpc.fr, the actual endpoint is defined in the NGINX Docker container wrapping SAS, not by SAS itself.
    => rewrite to match the actual base url so that we can fetch the next pages.
    """
    if url is None:
        return url
    elif not url.startswith(f"{SAS_ENDPOINT}/search-api/"):
        _, url_tail = url.split("/search-api/")
        return f"{SAS_ENDPOINT}/search-api/{url_tail}"
    return url


class SasExporter():
    save_ok_file: Path
    def __init__(
        self,
        strategy: Literal["search-api", "canvas"]="search-api",
        alt_url_root: str|None = None
    ):
        if strategy not in ["search-api", "canvas"]:
            raise ValueError(f"SasExporter: expected one of ['search-api', 'canvas'] for argument 'strategy', got '{strategy}'")
        if alt_url_root is not None and not isinstance(alt_url_root, "str"):
            raise ValueError(f"SasExporter: argument 'alt_url_root' can be a string or None, got '{alt_url_root}'")

        self.strategy = strategy
        self.alt_url_root = alt_url_root

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
        # aiohttp session
        self.make_session = lambda: make_session(self.max_connections)

        logger.info(f"Initiated SasExporter successfully (strategy={strategy}, alt_url_root={alt_url_root}).")
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

    def write_save_data(self, save_ok_data:Dict, save_err_data:List) -> "SasExporter":
        # NOTE: split self.save_data in 2 items: one with successful saves, one with errors.
        # write both to file. in `self.fetch_annotations_from_manifest_uri`, if there's a DL error, path is set to None
        json_write(save_ok_data, self.save_ok_file)
        json_write(save_err_data, self.save_err_file)
        return self

    def split_save_data(self) -> Tuple[Dict, List]:
        save_ok_data = {}
        save_err_data = []
        for manifest_uri, path in self.save_data.items():
            if path is not None:
                save_ok_data[manifest_uri] = path
            else:
                save_err_data.append(manifest_uri)
        return save_ok_data, save_err_data

    async def fetch_to_dict(self, url: str) -> Dict:
        async with self.make_session() as session:
            return await fetch_to_dict(session, url)

    async def fetch_annotation_list_paginated(self, url: str) -> Dict:
        """
        fetch all paginated annotations for a manifest and return them as a single IIIF AnnotationList.
        - fetch the base AnnotationList (1st page of results)
        - fetch all extra pages (URLs defined in "next" key of an annotation list)
        - concatenate all annotations within a single list
        - add this complete list to the base AnnotationList and return it
        """
        next_page = url
        annotation_list_full = None
        annotations = []
        while next_page:
            annotation_list = await self.fetch_to_dict(next_page)
            # base structure of the output annotation list. set at 1st iteration of while.
            if annotation_list_full is None:
                annotation_list_full = annotation_list
            annotations.extend(annotation_list.get("resources", []))
            next_page = fix_next_page_url(
                annotation_list.get("next", None)
            )
        annotation_list_full["resources"] = annotations  # pyright: ignore
        # remove pagination since all results are concatenated in annotation_list_full.
        if annotation_list_full.get("next", None):  # pyright: ignore
            del annotation_list_full["next"]  # pyright: ignore
        return annotation_list_full  # pyright: ignore

    async def fetch_manifests(self) -> "SasExporter":
        manifests = []
        collection = await self.fetch_to_dict(self.endpoint_manifests)
        manifests = iiif_collection_to_manifest_uri_list(collection)
        json_write(manifests, self.out_dir / "manifests_collection.json")
        self.manifests = manifests
        return self

    async def fetch_annotations_from_manifest_uri(self, manifest_uri: str) -> Tuple[str, str|None]:
        """
        pipeline to download a single annotation_list

        :returns:
            - if the download succeeds: (<manifest_uri, path_to_downloaded_annotation_list>)
            - if the download fails: (<manifest_uri>, None)
        """
        manifest_short_id = manifest_uri_to_short_id(manifest_uri)
        search_api_endpoint = self.endpoint_annotations(manifest_short_id)
        out_path = self.annotation_list_path(manifest_short_id)
        try:
            data = await self.fetch_annotation_list_paginated(search_api_endpoint)
            self.write_annotation_list(data, out_path)
            return manifest_uri, str(out_path)
        except Exception as e:
            logger.error(f"Failed to fetch annotations for manifest {manifest_uri}: {e}")
            return manifest_uri, None

    async def fetch_annotations(self) -> "SasExporter":
        manifests_to_download = [
            m for m in self.manifests if m not in self.save_data.keys()
        ]
        tasks = [
            self.fetch_annotations_from_manifest_uri(m_uri)
            for m_uri in manifests_to_download
        ]
        results = await tqdm_asyncio.gather(
            *tasks,
            total=len(manifests_to_download),
            desc=f"Downloading annotation lists"
        )
        self.save_data = { m_uri: path for m_uri, path in results }
        return self

    async def pipeline_async(self) -> "SasExporter":
        logger.info(f"Exporting data from '{SAS_ENDPOINT}'")
        logger.info("Fetching all indexed manifests.")
        await self.fetch_manifests()
        logger.info(f"Found {len(self.manifests)} manifests for which to extract annotations.")
        logger.info(f"Fetching annotations for {len(self.manifests)} manifests.")
        await self.fetch_annotations()
        logger.info(f"Finished fetching annotations.")
        return self

    def pipeline(self) -> "SasExporter":
        try:
            asyncio.run(self.pipeline_async())
        finally:
            save_ok_data, save_err_data = self.split_save_data()
            logger.info(f"Exporting data (success: {len(save_ok_data.keys())}, error: {len(save_err_data)}).")
            self.write_save_data(save_ok_data, save_err_data)
        return self

def export(stategy: Literal["search-api", "canvas"], alt_url_root: str|None):
    logger.info(f"RUNNING   : {STEP_NAME}")
    SasExporter(stategy, alt_url_root).pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
