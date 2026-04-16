import re
import asyncio
from pathlib import Path
from typing import Tuple, Dict, List

import aiohttp

from src.utils import json_read_if_exists

from .utils import (
    SAS_ENDPOINT,
    OUT_DIR,
    MAX_CONNECTIONS,
    IIIF_HOST_REPL,
    TIMEOUT,
    ANNOTATIONS_DIR,
    MANIFESTS_DIR,
    json_read_if_exists,
    json_write,
    fetch_to_json,
    make_session,
    make_semaphore,
    manifest_uri_to_short_id
)
from .logger import logger

class SasExporterBase():
    def __init__(self, retry: str|None):
        # get and validate retry
        # if retry is specified, fetch previous errors and select only the
        # ones with the valid "error_type" (and "http_satus", for HTTP errors)
        # only manifests with these errors will be processed.
        if retry is not None:
            retry_mapper = {
                "all": "all",
                "http": "ClientResponseError",
                "timeout": "SocketTimeoutError"
            }
            if re.match(r"^http:\d{3}$", retry):
                retry, http_status = retry.split(":")
                retry_filter = {
                    "error_type": retry_mapper[retry],
                    "http_status": int(http_status)
                }
            else:
                retry_filter = { "error_type": retry_mapper[retry] }
        else:
            retry_filter = None

        self.endpoint = SAS_ENDPOINT

        self.annotations_dir = ANNOTATIONS_DIR
        self.manifests_dir = MANIFESTS_DIR

        self.retry_filter = retry_filter
        self.iiif_host_repl: None|Tuple[str,str] = IIIF_HOST_REPL
        self.timeout = TIMEOUT
        self.out_dir = OUT_DIR
        self.max_connections = MAX_CONNECTIONS

        # HTTP client session
        # defined in __aenter__ / closed in `__aexit__`
        self._session: aiohttp.ClientSession | None = None
        self.semaphore = make_semaphore(self.max_connections)

        # save_data for the curent iteration of the pipeline. in self.save_data, we don't separate between errors and success. this is done in the final export only.
        self.save_data = {}
        # list of manifests to extract data from.
        self.manifests: List[str] = []

        # save_ok_file and save_err_file are defined by inheriting classes.
        # there is 1 save_(ok|err)_file per annotation.
        # NOTE: we overwrite contents of SAVE_ERR_FILE from 1 run to another:
        # if retry_filter is None, we retry a download on every failed annotation list extraction.
        # otherwise, we retry a download only on specific errors.
        self.save_ok_file = ""
        self.save_err_file = ""

        self.manifest_uri_to_short_id = manifest_uri_to_short_id
        return

    def load_save(self):
        ok_exists = None
        err_exists = None
        if len(self.save_err_file):
            self.save_err_previous, err_exists = json_read_if_exists(self.save_err_file)
        if len(self.save_ok_file):
            self.save_ok_previous, ok_exists = json_read_if_exists(self.save_ok_file)

        if ok_exists:
            logger.info(f"Skipping {len(list(self.save_ok_previous.keys()))} pre-fetched manifests")
        else:
            logger.info(f"No pre-fetched manifests to load. Everything will be exported.")
        return

    # NOTE: defining __aenter__ / __aexit__ turns SasExporterBase into an async content manager.
    # the advantage is that we can define 1 async context for the whole pipeline, thus
    # sharing the same aiohttp.ClientSession for the whole pipeline, avoiding leaks and
    # actually controlling the maximum number of parrallel queries run at once.
    async def __aenter__(self) -> "SasExporterBase":
        self._session = make_session(self.max_connections)
        return self

    async def __aexit__(self, *args) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError(f"{self.__class__} must be used as an async context manager")
        return self._session

    @property
    def endpoint_manifests(self) -> str:
        return f"{self.endpoint}/manifests"

    def write_save_data(self, save_ok_data:Dict, save_err_data:Dict) -> "SasExporterBase":
        # NOTE: split self.save_data in 2 items: one with successful saves, one with errors.
        # write both to file. in `self.fetch_annotations_from_manifest_uri`, if there's a DL error, path is set to None
        json_write(save_ok_data, self.save_ok_file)
        json_write(save_err_data, self.save_err_file)
        return self

    def prepare_save_data(self) -> Tuple[Dict, Dict]:
        save_ok_data = {}
        save_err_data = {}
        # split save_data in 2: manifests that are ok, and those with errors.
        for k, v in self.save_data.items():
            if v["success"] is True:
                save_ok_data[k] = v
            else:
                save_err_data[k] = v

        # concatenate save_sata with self.saver_data_previous (data extracted at the previous iteration)
        for k, v in self.save_ok_previous.items():
            if k not in save_ok_data.keys():
                save_ok_data[k] = v

        return save_ok_data, save_err_data

    def annotation_list_path(self, manifest_short_id: str) -> str|Path:
        return self.annotations_dir / f"{manifest_short_id}.json"

    def manifest_path(self, manifest_short_id: str) -> str|Path:
        return self.manifests_dir / f"{manifest_short_id}.json"

    async def fetch_to_json(self, url: str, params: Dict = {}) -> Dict|List:
        return await fetch_to_json(self.semaphore, self.session, url, params)

    async def fetch_manifest(self, manifest_uri: str, to_file: bool = False) -> Dict:
        # replace old IIIF host (indexed in SAS but NOT accessible on our IIIF server) by new IIIF host.
        if self.iiif_host_repl is not None:
            manifest_uri = manifest_uri.replace(self.iiif_host_repl[0], self.iiif_host_repl[1])

        manifest = await self.fetch_to_json(manifest_uri)  # pyright: ignore

        # if to_file, save the manifest.
        if to_file:
            json_write(
                manifest,
                self.manifest_path(self.manifest_uri_to_short_id(manifest_uri))
            )
        return manifest  # pyright: ignore

    async def fetch_manifest_collection(self) -> "SasExporterBase":
        manifests = []
        collection = await self.fetch_to_json(self.endpoint_manifests)
        manifests = [
            m["@id"]
            for m in collection["manifests"]  # pyright: ignore
            if m["@type"] == "sc:Manifest"
        ]
        json_write(manifests, self.out_dir / "manifests_collection.json")
        self.manifests = manifests
        return self

    def apply_retry_filter(self) -> List[str]:
        """
        after fetching the Collection of IIIF manifests to download from the SAS instance
        and after saving all the manifest URIs to `self.manifests`, filter manifests from
        `self.manifests` based on data from `self.retry_filter`.

        :returns: the filtered list of manifest URIs.
        """
        # skip successfully downloaded manifests
        if not self.retry_filter:
            manifests_to_download = [
                m for m in self.manifests
                if m not in self.save_ok_previous.keys()
            ]
        # expand retry_filter to re-export only certain failed manifests
        else:
            # if 'all', redownload all failures
            if self.retry_filter["error_type"] == "all":
                manifests_to_download = [
                    m for m in self.manifests
                    if m in self.save_err_previous.keys()
                ]
            # filter for a specific http status
            elif "http_status" in self.retry_filter.keys():
                manifests_to_download = []
                for err_m, err_obj in self.save_err_previous.items():
                    if (
                        err_m in self.manifests
                        and "http_status" in err_obj.keys()
                        and int(err_obj["http_status"]) == int(self.retry_filter["http_status"])
                    ):
                        manifests_to_download.append(err_m)
            # otherwise, self.retry_filter["error_type"] contains a value
            # of the "error_type" key in save_err_previous.values()
            else:
                manifests_to_download = [
                    m
                    for m in self.manifests
                    if m in self.save_err_previous.keys()
                    and self.save_err_previous[m]["error_type"] == self.retry_filter["error_type"]
                ]
        return manifests_to_download

    def make_err_obj(self, e: Exception) -> Dict:
        """
        build an informative object logging info on the exception
        """
        err_obj = {
            "success": False,
            "error_type": type(e).__name__
        }
        # build an error description
        if hasattr(e, "message"):
            err_obj["error_message"] = e.message  # pyright: ignore
        if hasattr(e, "status"):
            err_obj["http_status"] = e.status  # pyright: ignore
        return err_obj

    def pipeline_async(self):
        raise NotImplementedError("SasExporterBase.pipeline_async must be implemented by classes inheriting from SasExporterBase !")

    def pipeline(self) -> "SasExporterBase":
        logger.info(f"Exporting data from '{SAS_ENDPOINT}'")
        logger.info("Fetching all indexed manifests.")
        try:
            asyncio.run(self.pipeline_async())
        finally:
            save_ok_data, save_err_data = self.prepare_save_data()
            logger.info(f"Finished fetching data.")
            logger.info(f"Exporting data (success: {len(save_ok_data.keys())}, error: {len(save_err_data.keys())}).")
            self.write_save_data(save_ok_data, save_err_data)
        return self















