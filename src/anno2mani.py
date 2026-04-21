# ensure that, for all annotations, there is a downloaded target manifest.
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any, Literal

from tqdm import tqdm

from .utils import (
    ANNOTATIONS_DIR,
    MANIFESTS_DIR,
    ANNOTATIONS_MIGRATE_DIR,
    MANIFESTS_MIGRATE_DIR,
    IIIF_HOST_REPL,
    OUT_DIR,
    make_path,
    json_read_from_dir,
    json_write,
    json_dumps
)
from .logger import logger

STEP_NAME = "anno2mani"

regex_short_id = re.compile(r"wit\d+_[a-z]+\d+(_anno\d+)?")
get_short_id = lambda s: regex_short_id.search(s)[0]  # pyright: ignore

# deduplicate a list
dedup = lambda l: list(set(l))

# flatten a list. non-recursive => limited depth.
# signature: List[List[Any]] -> List[Any]
flatten = lambda xss: [ x for xs in xss for x in xs ]

def target_to_short_ids(target: List|Dict|str) -> List[str]:
    target = target if isinstance(target, list) else [target]
    target_short_ids = []
    for t in target:
        if isinstance(t, str):
            target_short_ids.append(get_short_id(t))
        elif isinstance(t, dict):
            target_short_ids.append(get_short_id(t["full"]))
    return target_short_ids

def pipeline(step: Literal["pre-migrate","post-migrate"]):
    indir_annotations = ANNOTATIONS_DIR if step=="pre-migrate" else ANNOTATIONS_MIGRATE_DIR
    indir_manifests = MANIFESTS_DIR if step=="pre-migrate" else MANIFESTS_MIGRATE_DIR

    # 1. build an index of manifest short IDs from all annotation targets
    # { <filepath>: <list of short IDs> }
    annotation_mapper = {
        fp: flatten([
            target_to_short_ids(annotation["on"])
            for annotation in annotation_list["resources"]
        ])
        for fp, annotation_list
        in json_read_from_dir(indir_annotations)
    }
    annotation_manifest_short_ids = dedup([
        short_id
        for fp, annotation_short_ids in annotation_mapper.items()
        for short_id in annotation_short_ids
    ])

    # 2. build an index of manifest short IDs from all manifests.
    manifest_short_ids = dedup([
        get_short_id(manifest["@id"])
        for _, manifest in json_read_from_dir(indir_manifests)
        if "@id" in manifest.keys()
    ])

    # 3. compare
    # TODO less ugly output / save to file
    missing_manifests_short_ids = [
        short_id
        for short_id in annotation_manifest_short_ids
        if short_id not in manifest_short_ids
    ]
    if len(missing_manifests_short_ids) > 0:
        print(f"{len(missing_manifests_short_ids)} manifests are missing from {indir_manifests} :")
        print(json_dumps(missing_manifests_short_ids).decode())
    else:
        print("All annotations have a corresponding manifest !")

def anno2mani(step: Literal["pre-migrate","post-migrate"]):
    logger.info(f"RUNNING: {STEP_NAME}")
    pipeline(step)
    logger.info(f"COMPLETED: {STEP_NAME}  (* ´ ▽ ` *)")
