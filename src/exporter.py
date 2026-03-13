from pathlib import Path
from typing import List, Dict, Tuple

import requests
from tqdm import tqdm

from .utils import SAS_ENDPOINT, SAVE_FILE, OUT_DIR, ANNOTATIONS_DIR, json_read_if_exists, json_write
from .logger import logger


class SasExporter():
    save_file: Path
    def __init__(self):
        # check that the endpoint actually exists
        try:
            requests.get(SAS_ENDPOINT)
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            logger.error(f"Endpoint {SAS_ENDPOINT} could not be reached ! Exiting...")
            exit(1)

        self.endpoint = SAS_ENDPOINT
        self.logger = logger
        self.annotations_dir = ANNOTATIONS_DIR
        self.out_dir = OUT_DIR
        self.save_file = SAVE_FILE
        self.save_data, exists = json_read_if_exists(self.save_file)
        self.manifests: List[str] = []

        self.logger.info(f"Initiated SasExporter successfully.")
        if exists:
            self.logger.info(f"Skipping {len(list(self.save_data.keys()))} pre-fetched manifests")
        else:
            self.logger.info(f"No pre-fetched manifests to load. Everything will be exported.")
        return

    def write_annotation(self, data) -> "SasExporter":
        ...
        return self

    def write_save_data(self) -> "SasExporter":
        json_write(self.save_data, self.save_file)
        return self

    def fetch_manifests(self) -> "SasExporter":
        ...
        manifests = []
        # todo fetch
        self.logger.info(f"Found {len(manifests)} for which to extract annotations.")
        self.manifests = manifests
        return self

    def fetch_annotations(self) -> "SasExporter":
        # self.save_data is a dict or { <manifest URI>: <path to downloaded annotationList> }
        manifests_to_download = [
            m for m in self.manifests if m not in self.save_data.keys()
        ]
        for m_uri in manifests_to_download:
            ...
            # NOTE here, we DL all annotations for a single manifest.
            # NOTE pipeline (without multiprocessing):
            #   - DL the manifest.
            #   - if it works, increment self.save_data
            #   - else, log an error message
        return self

    def pipeline(self) -> "SasExporter":
        try:
            (self
                .fetch_manifests()
                .fetch_annotations())

        finally:
            self.write_save_data()
        return self
