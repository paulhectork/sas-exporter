import os
import copy
import asyncio
from pathlib import Path
from typing import List, Dict, Tuple, Literal

import aiohttp
from tqdm.asyncio import tqdm_asyncio
from yarl import URL

from .utils import (
    SAS_ENDPOINT,
    SAVE_OK_FILE,
    OUT_DIR,
    ANNOTATIONS_DIR,
    SAVE_ERR_FILE,
    MAX_CONNECTIONS,
    ANNOTATION_LIST_TEMPLATE,
    URL_ROOT_REGEX,
    json_read_if_exists,
    json_write,
    fetch_to_json,
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
    def __init__(self):
        # get and validate env variables
        strategy = os.getenv("EXPORT_STRATEGY")
        if strategy not in ["search-api", "canvas"]:
            raise ValueError(f"SasExporter: env variable EXPORT_STRATEGY expected one of ['search-api', 'canvas'], got '{strategy}'")

        iiif_host_repl = os.getenv("IIIF_HOST_REPL")
        if iiif_host_repl is not None:
            iiif_host_repl = iiif_host_repl.split(",")
            if not len(iiif_host_repl) == 2:
                raise ValueError(f"SasExporter: env variable 'IIIF_HOST_REPL' must be 'old-host,new-host' (i.e., 'old.example.org,new.example.org'), got '{iiif_host_repl}'")
            iiif_host_repl = (iiif_host_repl[0], iiif_host_repl[1])  # ( old_host, new_host )

        self.strategy = strategy
        self.iiif_host_repl: None|Tuple[str,str] = iiif_host_repl

        self.endpoint = SAS_ENDPOINT
        self.annotations_dir = ANNOTATIONS_DIR
        self.out_dir = OUT_DIR
        self.save_ok_file = SAVE_OK_FILE
        self.max_connections = MAX_CONNECTIONS
        # mapping of { <manifest_uri>: <path to downloaded annotation list>? }
        self.save_data_previous, exists = json_read_if_exists(self.save_ok_file)
        # save_data for the curent iteration of the pipeline
        self.save_data = {}
        # NOTE: we overwrite contents of SAVE_ERR_FILE from 1 run to another: we retry a download on every failed annotation list extraction.
        self.save_err_file = SAVE_ERR_FILE
        # list of manifests to download
        self.manifests: List[str] = []

        # HTTP client session
        # defined in __aenter__ / closed in `__aexit__`
        self._session: aiohttp.ClientSession | None = None

        logger.info(f"Initiated SasExporter successfully (strategy={strategy}, iiif_host_repl={iiif_host_repl}).")
        if exists:
            logger.info(f"Skipping {len(list(self.save_data.keys()))} pre-fetched manifests")
        else:
            logger.info(f"No pre-fetched manifests to load. Everything will be exported.")
        return

    # NOTE: defining __aenter__ / __aexit__ turns SasExporter into an async content manager.
    # the advantage is that we can define 1 async context for the whole pipeline, thus
    # sharing the same aiohttp.ClientSession for the whole pipeline, avoiding leaks and
    # actually controlling the maximum number of parrallel queries run at once.
    async def __aenter__(self) -> "SasExporter":
        self._session = make_session(self.max_connections)
        return self

    async def __aexit__(self, *args) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("SasExporter must be used as an async context manager")
        return self._session

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

    def prepare_save_data(self) -> Tuple[Dict, List]:
        save_ok_data = {}
        save_err_data = []
        # split save_data in 2: manifests that are ok, and those with errors.
        for manifest_uri, path in self.save_data.items():
            if path is not None:
                save_ok_data[manifest_uri] = path
            else:
                save_err_data.append(manifest_uri)

        # concatenate save_sata with self.saver_data_previous (data extracted at the previous iteration)
        for manifest_uri, path in self.save_data_previous.items():
            if manifest_uri not in save_ok_data.keys():
                save_ok_data[manifest_uri] = path

        return save_ok_data, save_err_data

    async def fetch_to_json(self, url: str, params: Dict = {}) -> Dict|List:
        return await fetch_to_json(self.session, url, params)

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
            annotation_list = await self.fetch_to_json(next_page)
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

    async def fetch_annotations_with_search_api(self, manifest_short_id: str):
        search_api_endpoint = self.endpoint_annotations(manifest_short_id)
        return await self.fetch_annotation_list_paginated(search_api_endpoint)

    async def fetch_annotations_for_canvas(self, canvas_id: str) -> List[Dict]:
        # TODO witXX_manXX_annoXX has been changed to witXX_manXX so now there are TONS of fetch errors
        #   when fetching with canvas URIs BUT is works fine when fetching with /search-api/
        #   and i suspect that querying search_api with witXX_manXX will actually cause data loss.
        fetch = lambda x: self.fetch_to_json(f"{self.endpoint}/annotation/search", { "uri": x })
        return await fetch(canvas_id)  # pyright: ignore


    async def fetch_annotations_with_search_canvas(self, manifest_uri: str):
        # NOTE: about self.iiif_host_repl:
        # in the case where:
        # - the IIIF manifest provider has changed its host (old.example.com has become new.example.com)
        # - BUT those changes have not been reflected in SAS (manifests are still indexed using old.example.com)
        # do:
        # 1. fetch manifest using new IIIF host
        # 2. build an index of canvases with the old IIIF host: the route /annotation/search will still use the old IIIF root,
        #      since IIIF annotation targets have not been updated.

        # replace old IIIF host (indexed in SAS but NOT accessible on our IIIF server) by new IIIF host.
        if self.iiif_host_repl is not None:
            manifest_uri = manifest_uri.replace(self.iiif_host_repl[0], self.iiif_host_repl[1])

        manifest = await self.fetch_to_json(manifest_uri)
        # 1. build a list of all canvas IDs to query
        canvas_uri_set = set(
            canvas["@id"]
            for canvas in manifest["sequences"][0]["canvases"]
        )
        # 2. convert host of canvas URIs back to the old host: it is the old host that is indexed in SAS.
        if self.iiif_host_repl is not None:
            canvas_uri_set = set(
                canvas_uri.replace(self.iiif_host_repl[1], self.iiif_host_repl[0])
                for canvas_uri in canvas_uri_set
            )
        # 3. query all canvas IDs, handling alt_url_root if necessary
        tasks = [
            self.fetch_annotations_for_canvas(canvas_id)
            for canvas_id in canvas_uri_set
        ]
        # 3. concatenate results in an annotation list.
        # list of list of annotations
        results: List[List[Dict]] = await asyncio.gather(*tasks)
        # list of asnnotations
        annotation_array: List[Dict] = [
            _r for r in results for _r in r
        ]
        annotation_list = copy.deepcopy(ANNOTATION_LIST_TEMPLATE)
        annotation_list["@id"] = manifest_uri_to_short_id(manifest_uri)
        annotation_list["resources"] = annotation_array
        return annotation_list

    async def fetch_manifests(self) -> "SasExporter":
        manifests = []
        collection = await self.fetch_to_json(self.endpoint_manifests)
        manifests = iiif_collection_to_manifest_uri_list(collection)
        json_write(manifests, self.out_dir / "manifests_collection.json")
        self.manifests = manifests
        return self

    async def fetch_annotations_from_manifest_uri(self, manifest_uri: str) -> "SasExporter":
        """
        pipeline to download a single annotation_list

        finishes by appending to `self.save_data` a dict on the extracted annotations:
            - if the download succeeds: (<manifest_uri, path_to_downloaded_annotation_list>)
            - if the download fails: (<manifest_uri>, None)
        """
        manifest_short_id = manifest_uri_to_short_id(manifest_uri)
        out_path = self.annotation_list_path(manifest_short_id)

        try:
            if self.strategy == "search-api":
                data = await self.fetch_annotations_with_search_api(manifest_short_id)
            else:
                data = await self.fetch_annotations_with_search_canvas(manifest_uri) or {}
            self.write_annotation_list(data, out_path)
            self.save_data[manifest_uri] = str(out_path)

        except Exception as e:
            logger.error(f"Failed to fetch annotations for manifest {manifest_uri}: {e}")
            self.save_data[manifest_uri] = None
        return self

    async def fetch_annotations(self) -> "SasExporter":
        manifests_to_download = [
            m for m in self.manifests if m not in self.save_data_previous.keys()
        ]
        tasks = [
            self.fetch_annotations_from_manifest_uri(m_uri)
            for m_uri in manifests_to_download
        ]
        await tqdm_asyncio.gather(
            *tasks,
            total=len(manifests_to_download),
            desc=f"Downloading annotation lists"
        )
        return self

    async def pipeline_async(self) -> "SasExporter":
        # this wraps the pipeline in an async context manager, with a sincle client session.
        async with self:
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
            save_ok_data, save_err_data = self.prepare_save_data()
            logger.info(f"Exporting data (success: {len(save_ok_data.keys())}, error: {len(save_err_data)}).")
            self.write_save_data(save_ok_data, save_err_data)
        return self

def export():
    logger.info(f"RUNNING   : {STEP_NAME}")
    SasExporter().pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
