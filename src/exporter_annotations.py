import re
import os
import copy
import asyncio
import traceback
from pathlib import Path
from datetime import timedelta
from timeit import default_timer as timer
from typing import List, Dict, Tuple, Literal

import aiohttp
from tqdm.asyncio import tqdm_asyncio

from src.exporter_base import SasExporterBase
from .logger import logger
from .utils import (
    SAS_ENDPOINT,
    SAVE_OK_FILE_ANNOTATIONS,
    SAVE_ERR_FILE_ANNOTATIONS,
    ANNOTATION_LIST_TEMPLATE,
    EXPORT_STRATEGY,
    json_write,
)

STEP_NAME = "export_annotations"

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


class SasExporterAnnotations(SasExporterBase):
    def __init__(self, retry: str|None, export_manifests: bool = False):
        super().__init__(retry)
        self.strategy = EXPORT_STRATEGY

        if export_manifests and not self.strategy == "canvas":
            logger.error(f"export_manifests can only be used if `EXPORT_STRATEGY=='canvas'`. exiting !")
            exit(1)
        self.export_manifests = bool(export_manifests)

        self.save_ok_file = SAVE_OK_FILE_ANNOTATIONS
        self.save_err_file = SAVE_ERR_FILE_ANNOTATIONS
        self.load_save()
        logger.info(f"Initiated SasExporterAnnotations successfully (strategy={self.strategy}, iiif_host_repl={self.iiif_host_repl}, max_connections={self.max_connections}).")

    def endpoint_annotations(self, manifest_short_id: str) -> str:
        # search-api endpoint returns all annotations for a manifest, paginated.
        return f"{self.endpoint}/search-api/{manifest_short_id}/search"

    async def fetch_annotation_list_paginated(self, url: str) -> Dict:
        """
        fetch all paginated annotations for a manifest using the /search-api/
        and return them as a single IIIF AnnotationList.

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
        return await self.fetch_to_json(f"{self.endpoint}/annotation/search", { "uri": canvas_id })  # pyright: ignore

    async def fetch_annotations_with_search_canvas(self, manifest_uri: str):
        """
        fetch all annotations for a manifest using the /annotation/search route
        and return all concatenated annotations in an AnnotationList

        NOTE: about self.iiif_host_repl:
        in the case where:
        - the IIIF manifest provider has changed its host (old.example.com has become new.example.com)
        - BUT those changes have not been reflected in SAS (manifests are still indexed using old.example.com)
        do:
        1. fetch manifest using new IIIF host (done in SasExporterBase.fetch_manifest)
        2. build an index of canvases with the old IIIF host: the route /annotation/search will still use the old IIIF root,
             since IIIF annotation targets have not been updated.
        """
        # 1. build a list of all canvas IDs to query
        manifest = await self.fetch_manifest(manifest_uri, to_file=self.export_manifests)

        # NOTE: in some cases, this will raise a KeyError: in AIKON, a JSON is returned, but with the structure { "response": "...", "reason": "..." }
        # this is caused by a deleted witness.
        canvas_uri_list = list(set(
            canvas["@id"]
            for canvas in manifest["sequences"][0]["canvases"]
        ))

        # 2. convert host of canvas URIs back to the old host: it is the old host that is indexed in SAS.
        if self.iiif_host_repl is not None:
            canvas_uri_list = [
                canvas_uri.replace(self.iiif_host_repl[1], self.iiif_host_repl[0])
                for canvas_uri in canvas_uri_list
            ]
        # 3. query all canvas IDs, handling alt_url_root if necessary
        tasks = [
            self.fetch_annotations_for_canvas(canvas_id)
            for canvas_id in canvas_uri_list
        ]
        # 4. concatenate results in an annotation list.
        # list of list of annotations
        results: List[List[Dict]] = await tqdm_asyncio.gather(*tasks, desc=self.manifest_uri_to_short_id(manifest_uri))
        # list of asnnotations
        annotation_array: List[Dict] = [
            _r for r in results for _r in r
        ]
        annotation_list = copy.deepcopy(ANNOTATION_LIST_TEMPLATE)
        annotation_list["@id"] = self.manifest_uri_to_short_id(manifest_uri)
        annotation_list["resources"] = annotation_array
        return annotation_list

    async def fetch_annotations_from_manifest_uri(self, manifest_uri: str) -> "SasExporterAnnotations":
        """
        pipeline to download a single annotation_list

        finishes by appending to `self.save_data` a dict on the extracted annotations.
        structure changes between success and errors.
        """
        manifest_short_id = self.manifest_uri_to_short_id(manifest_uri)
        out_path = self.annotation_list_path(manifest_short_id)

        try:
            if self.strategy == "search-api":
                data = await self.fetch_annotations_with_search_api(manifest_short_id)
            else:
                data = await self.fetch_annotations_with_search_canvas(manifest_uri) or {}
            json_write(data, out_path)
            self.save_data[manifest_uri] = {
                "path": str(out_path),
                "success": True
            }

        except Exception as e:
            logger.error(
                f"Failed to fetch annotations for manifest {manifest_uri}: {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )
            self.save_data[manifest_uri] = self.make_err_obj(e)
        return self

    async def fetch_annotations(self) -> "SasExporterAnnotations":
        manifests_to_download = self.apply_retry_filter()
        logger.info(f"Fetching annotations for {len(manifests_to_download)} manifests.")

        # NOTE: parrallelization and asyncio.gather:
        # - if self.strategy==search-api, parallelize all
        #       `fetch_annotations_from_manifest_uri` using asyncio.gather
        # - if self.strategy==canvas, DO NOT use asyncio.gather on
        #       `fetch_annotations_from_manifest_uri`:
        #       there is a nested asyncio.gather (1 request/canvas), which
        #       will cause runtime errors: the asyncio queue is filled with
        #       `n_manifests x n_canvas` pending jobs, which WILL cause timeouts.
        if (self.strategy != "canvas"):
            tasks = [
                self.fetch_annotations_from_manifest_uri(m_uri)
                for m_uri in manifests_to_download
            ]
            await tqdm_asyncio.gather(
                *tasks,
                total=len(manifests_to_download),
                desc=f"Downloading annotation lists"
            )
        else:
            # non-parrallelized outer loop. we create a small progress tracker
            time = None
            total = len(manifests_to_download)
            calc_timedelta = lambda t: timedelta(seconds=round(t,0))  # t: int = time in seconds
            elapsed = 0
            for i, m_uri in enumerate(manifests_to_download):
                i += 1
                if time is not None:
                    time_item = round(time, 2)  # round to 1/100th of a second
                    remaining = calc_timedelta(time*(total-i))
                    elapsed += time  # time since start in seconds
                else:
                    time_item = "??"
                    remaining = "??"
                elapsed_td = calc_timedelta(elapsed)
                done = round(100*i/total, 2)
                time_info = f"[{time_item}s/it, {done}%, elapsed={elapsed_td}, remaining={remaining}]"
                s = timer()
                print(f"fetching annotations for manifest {i}/{total} {time_info}")
                await self.fetch_annotations_from_manifest_uri(m_uri)
                e = timer()
                time = e-s

        return self

    async def pipeline_async(self) -> "SasExporterAnnotations":
        # this wraps the pipeline in an async context manager, with a sincle client session.
        async with self:
            await self.fetch_manifest_collection()
            logger.info(f"Found {len(self.manifests)} manifests for which to extract annotations.")
            await self.fetch_annotations()
        return self

def export(retry: str|None, export_manifests: bool = False):
    logger.info(f"RUNNING   : {STEP_NAME}")
    SasExporterAnnotations(retry, export_manifests).pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
