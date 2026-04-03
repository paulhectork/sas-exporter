"""
AIKON-SPECIFIC JSON structure migration script:
- THE ORIGINAL AIKON STRUCTURE was: Witness => Digitization => Regions,
      where digitization is 1 digitization of a physical object, and Regions is 1 regions extraction in this digitization
      (in SAS/IIIF terms, 1 regions extraction <=> 1 manifest => annotations done on this manifest)
      => originally, annotations stored in SAS are defined relative to a regions extraction
- WITH THE NEW AIKON STRUCTURE,
      - annotations are done directly on the digitization and not on the regions.
      - the precise regions extraction ID is referenced as a tag in the annotation

=> this script updates annotations with the OLD structure to annotations with the NEW structure.
it also does some other minor changes.
"""
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any, Literal

from tqdm import tqdm

from .utils import (
    ANNOTATIONS_DIR,
    IIIF_HOST_REPL,
    OUT_DIR,
    make_path,
    json_read_from_dir,
    json_write
)
from .logger import logger


STEP_NAME = "migrate_structure"


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


def update_iiif_base_uri(manifest_uri: str) -> Tuple[str,str,str]:
    """
    update a manifest's URI:
    - replace a manifest's short ID
        from {wit_id}_{digit_id}_{region_id}
        to   {wit_id}_{digit_id}
    - drop the "v2"
    => return the tuple: (
        new_manifest_uri,
        new_manifest_short_id,
        old_manifest_short_id
    )

    input  : https://aikon.enpc.fr/aikon/iiif/v2/wit124_man152_anno228
    output : (
        https://aikon.enpc.fr/aikon/iiif/wit124_man152,
        wit124_man152_anno228,
        wit124_man152
    )
    """
    base, tail = manifest_uri.split("/v2/")
    old_short_id = tail.split("/")[0]
    new_short_id = update_short_id(old_short_id)
    return (
        f"{base}/{new_short_id}",
        new_short_id,
        old_short_id
    )

def make_iiif_host_repl(s:str) -> str:
    """
    replace OLD iiif host base URI by NEW iiif host base URI
    """
    if not IIIF_HOST_REPL:
        return s
    if IIIF_HOST_REPL[0] in s:
        return s.replace(IIIF_HOST_REPL[0], IIIF_HOST_REPL[1])
    return s

regex_canvas_split = re.compile(r"\/(?=canvas)")
regex_manifest_split = re.compile(r"\/(?=manifest.json)")
def update_iiif_uri(iiif_uri, uri_type: Literal["canvas", "manifest"]) -> Tuple[str,str]:
    """
    replacements to a IIIF URI are:
    - update the IIIF host if needed
    - remove "/v2/" (useless since all annotations are directly on the digitization, not on the region)
    - update the IIIF short ID (remove the region_id part)
    """
    if uri_type == "canvas":
        splitter = regex_canvas_split
    else:
        splitter = regex_manifest_split
    uri_base, uri_tail = splitter.split(iiif_uri)
    uri_base, new_short_id, old_short_id = update_iiif_base_uri(uri_base)
    iiif_uri = f"{uri_base}/{uri_tail}"
    return make_iiif_host_repl(iiif_uri), old_short_id

def update_dict_target(target: dict) -> Tuple[dict, str]:
    """
    update a SpecificResource (by updating its canvas URI and manifest ID)
    """
    if not target["@type"] == "oa:SpecificResource":
        raise ValueError(f"expected dict target to have '@type' 'oa:SpecificResource', got {target['@type']}")

    # target["within"] should be a dict, but sometimes it's just a manifest URI
    if isinstance(target["within"], str):
        within = {
            "@id": target["within"],
            "@type": "sc:Manifest"
        }
        target["within"] = within

    manifest_uri, old_short_id = update_iiif_uri(target["within"]["@id"], "manifest")
    canvas_uri, _ = update_iiif_uri(target["full"], "canvas")
    target["within"]["@id"] = manifest_uri
    target["full"] = canvas_uri
    return target, old_short_id

def update_target_recursive(target: Any, inner: bool = False):
    if isinstance(target, str):
        target, old_short_id = update_iiif_uri(target, "canvas")
    elif isinstance(target, dict):
        target, old_short_id = update_dict_target(target)
    elif isinstance(target, list):
        if not len(target):
            raise ValueError("a IIIF annotation.on list cannot be empty !")
        if inner:
            raise TypeError("a IIIF annotation.on cannot be a nested list !")

        # list of (target, old_short_id)
        result: List[Tuple[Dict|str, str]] = []
        for t in target:
            result.append(update_target_recursive(t, True))  # pyright: ignore
        target = [ r[0] for r in result]
        # we expect that all values of annotation.on target the same manifest,
        # and so only extract the 1st target short ID.
        old_short_id = result[0][0]
    else:
        raise TypeError(f"only supported types are 'str', 'dict', 'list'. got {type(target)}")

    return target, old_short_id

def update_annotation(annotation: Dict):
    target = annotation.get("on")

    # 1. update the annotation.on
    target, old_short_id = update_target_recursive(target, False)
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
        body = [tag]

    # 3. drop the "$root_url/sas/full_text" key from body (auto-generated in SAS, useless in aiiinotate)
    body_out = []
    for item in body:
        k_list = [ k for k in item.keys() if k.endswith("/sas/full_text") ]
        for k in k_list:
            del item[k]
        body_out.append(item)

    annotation["resource"] = body_out
    return annotation


def pipeline():
    out_dir = OUT_DIR / f"{ANNOTATIONS_DIR.name}_{STEP_NAME}"
    make_path(out_dir, is_dir=True)

    # update each AnnotationList and write to file
    for fp, annotation_list in tqdm(
        json_read_from_dir(ANNOTATIONS_DIR),
        desc="updating annotations",
        total=len(list(ANNOTATIONS_DIR.iterdir()))
    ):
        fn = Path(fp).name
        fp_out = out_dir / fn
        annotation_array = []

        for annotation in annotation_list.get("resources", []):
            annotation_array.append(update_annotation(annotation))
        annotation_list["resources"] = annotation_array

        json_write(annotation_list, fp_out)
    return


def migrate_structure():
    logger.info(f"RUNNING: {STEP_NAME}")
    pipeline()
    logger.info(f"COMPLETED: {STEP_NAME}  (* ´ ▽ ` *)")
