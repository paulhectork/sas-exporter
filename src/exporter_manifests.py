import traceback

from tqdm import tqdm
import asyncio

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
            await self.fetch_manifest(manifest_uri, True)
            self.save_data[manifest_uri] = {
                "path": str(self.manifest_path(self.manifest_uri_to_short_id(manifest_uri))),
                "success": True
            }

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

            queue = asyncio.Queue()
            for m_uri in manifests_to_download:
                await queue.put(m_uri)

            progress = tqdm(total=len(manifests_to_download), desc="downloading manifests")

            async def worker():
                while True:
                    try:
                        m_uri = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    await self.export_manifest(m_uri)
                    progress.update(1)
                    queue.task_done()

            # spawn exactly MAX_CONNECTIONS workers — no more coroutines in flight
            workers = [asyncio.create_task(worker()) for _ in range(self.max_connections)]
            await asyncio.gather(*workers)
            progress.close()

        return self

def export(retry: str|None):
    logger.info(f"RUNNING   : {STEP_NAME}")
    SasExporterManifests(retry).pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
