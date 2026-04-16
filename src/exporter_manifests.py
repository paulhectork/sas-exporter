import traceback

from tqdm.asyncio import tqdm_asyncio

from .exporter_base import SasExporterBase
from .logger import logger
from .utils import (
    SAVE_OK_FILE_MANIFESTS,
    SAVE_ERR_FILE_MANIFESTS
)


STEP_NAME = "export_manifests"

class SasExporterManifests(SasExporterBase):
    def __init__(self, retry: str|None):
        super().__init__(retry)

        self.save_ok_file = SAVE_OK_FILE_MANIFESTS
        self.save_err_file = SAVE_ERR_FILE_MANIFESTS
        self.load_save()
        logger.info(f"Initiated SasExporterManifests successfully (iiif_host_repl={self.iiif_host_repl}, max_connections={self.max_connections}).")

    async def export_manifest(self, manifest_uri: str) -> "SasExporterManifests":
        try:
            manifest = await self.fetch_manifest(manifest_uri, True)
        except Exception as e:
            logger.error(
                f"Failed to export manifest {manifest_uri}: {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )
            self.save_data[manifest_uri] = self.make_err_obj(e)
        return self

    async def pipeline_async(self) -> "SasExporterManifests":
        async with self:
            await self.fetch_manifest_collection()
            logger.info(f"Found {len(self.manifests)} manifests to export.")

            manifests_to_download = self.apply_retry_filter()
            logger.info(f"Fetching {len(manifests_to_download)} manifests.")

            tasks = [
                self.export_manifest(m_uri)
                for m_uri in manifests_to_download
            ]
            await tqdm_asyncio.gather(
                *tasks,
                total=len(manifests_to_download),
                desc="Downloading manifests"
            )
        return self

def export(retry: str|None):
    logger.info(f"RUNNING   : {STEP_NAME}")
    SasExporterBase(retry).pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
