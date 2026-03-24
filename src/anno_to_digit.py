"""
AIKON-SPECIFIC migration script:
- THE ORIGINAL AIKON STRUCTURE was: Witness => Digitization => Regions,
      where digitization is 1 digitization of a physical object, and Regions is 1 regions extraction in this digitization
      (in SAS/IIIF terms, 1 regions extraction <=> 1 manifest => annotations done on this manifest)
      => originally, annotations stored in SAS are defined relative to a regions extraction
- WITH THE NEW AIKON STRUCTURE,
      - annotations are done directly on the digitization and not on the regions.
      - the precise regions extraction ID is referenced as a tag in the annotation

=> this script updates annotations with the OLD structure to annotations with the NEW structure.
    basically all it does is update the target URLs and write the output to file.
"""
import re
from pathlib import Path
from typing import List, Dict, Tuple

from .utils import (
    ANNOTATIONS_DIR,
    MAX_CONNECTIONS,
    OUT_DIR,
    make_path,
    json_read_from_dir,
    json_write
)
from .logger import logger


STEP_NAME = "anno_to_digit"


regex_short_id = re.compile(r"^(wit\d+_[a-z]+\d+)_anno\d+$")
def update_short_id(short_id: str) -> str:
    """
    input  : wit124_man152_anno228
    output : wit124_man152
    """
    match = regex_short_id.search(short_id)
    if match:
        return match[1]
    else:
        raise ValueError(f"could not extract valid new short ID from '{short_id}'")


def update_manifest_base_url(target_uri: str) -> Tuple[str,str,str]:
    """
    the new URL is made by dropping the "v2" and updating the short ID

    input  : https://aikon.enpc.fr/aikon/iiif/v2/wit124_man152_anno228
    output : (
        https://aikon.enpc.fr/aikon/iiif/wit124_man152,
        wit124_man152_anno228,
        wit124_man152
    )

    :returns: (
        new_target_uri,
        new_manifest_short_id,
        old_manifest_short_id
    )
    """
    base, tail = target_uri.split("/v2/")
    old_short_id = tail.split("/")[0]
    new_short_id = update_short_id(old_short_id)
    return (
        f"{base}/{new_short_id}",
        new_short_id,
        old_short_id
    )


regex_split = re.compile(r"\/(?=canvas)")
def update_annotation(annotation: Dict):
    target = annotation.get("on")

    if not isinstance(target, str):
        raise TypeError(f"Expected type 'str' for annotation.on, got {type(target)} (on {target})")

    # 1. update the annotation.on
    target_base, target_tail = regex_split.split(target)
    manifest_base_url, new_short_id, old_short_id = update_manifest_base_url(target_base)
    target = f"{manifest_base_url}/{target_tail}"
    annotation["on"] = target

    # 2. log the region extraction id to a tag in the annotation's body
    # the old short ID was the region extraction ID
    body = annotation.get("resource", {})
    tag = { "@type": "oa:Tag", "chars": old_short_id }
    if isinstance(body, list) and len(body) > 0:
        body.append(tag)
    elif isinstance(body, dict) and len(body.keys()) > 0:
        body = [body, tag]
    else:
        body = tag
    annotation["resource"] = body
    return annotation


def pipeline():
    out_dir = OUT_DIR / f"{ANNOTATIONS_DIR.name}_{STEP_NAME}"
    make_path(out_dir, is_dir=True)

    # update each AnnotationList and write to file
    for fp, annotation_list in json_read_from_dir(ANNOTATIONS_DIR):
        fn = Path(fp).name
        fp_out = out_dir / fn
        annotation_array = []

        for annotation in annotation_list.get("resources", []):
            annotation_array.append(update_annotation(annotation))
        annotation_list["resources"] = annotation_array

        json_write(annotation_list, fp_out)
    return


def anno_to_digit():
    logger.info(f"RUNNING: {STEP_NAME}")
    pipeline()
    logger.info(f"COMPLETED: {STEP_NAME}  (* ´ ▽ ` *)")
