"""
AIKON-SPECIFIC JSON structure migration script:
- THE ORIGINAL AIKON STRUCTURE was: Witness => Digitization => Regions,
      where digitization is 1 digitization of a physical object, and Regions is 1 regions extraction in this digitization
      (in SAS/IIIF terms, 1 regions extraction <=> 1 manifest => annotations done on this manifest)
      => originally, annotations stored in SAS are defined relative to a regions extraction
- WITH THE NEW AIKON STRUCTURE,
      - annotations are done directly on the digitization and not on the regions.
      - the precise regions extraction ID is referenced as a tag in the annotation

=> this script updates annotations and manifests with the OLD structure to annotations
with the NEW structure. it also does some other minor changes.
"""
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
    json_read_from_dir,
    json_write,
)
from .logger import logger


STEP_NAME = "migrate"

# -------------------------------------------------------------
# UTILS

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


regex_split_iiif = re.compile(r"(?<=iiif)\/")
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
    if "/v2/" in manifest_uri:
        base, tail = manifest_uri.split("/v2/")
    else:
        base, tail = regex_split_iiif.split(manifest_uri)
    old_short_id = tail.split("/")[0]
    try:
        new_short_id = update_short_id(old_short_id)
        return (
            f"{base}/{new_short_id}",
            new_short_id,
            old_short_id
        )
    # if a new ID couldn´t be extracted, it's because the URI doesn´t follow the {wit_id}_{digit_id}_{region_id} pattern => don't bother updating.
    except ValueError:
        return (
            f"{base}/{old_short_id}",
            old_short_id,
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

regex_split = re.compile(r"\/((?=manifest)|(?=sequence)|(?=canvas)|(?=annotation))")
def update_iiif_uri(iiif_uri) -> Tuple[str,str]:
    """
    allowed URIs: manifest URIs, sequence URIs, canvas URIs, image URIs

    replacements to a IIIF URI are:
    - update the IIIF host if needed
    - remove "/v2/" (useless since all annotations are directly on the digitization, not on the region)
    - update the IIIF short ID (remove the region_id part)
    """
    try:
        uri_base, _, uri_tail = regex_split.split(iiif_uri)
    # non-aikon URI => can't extract or update it => pass
    except ValueError:
        logger.error(f"update_iiif_uri: can't process {iiif_uri}")
        return iiif_uri, ""
    uri_base, new_short_id, old_short_id = update_iiif_base_uri(uri_base)
    iiif_uri = f"{uri_base}/{uri_tail}"
    return make_iiif_host_repl(iiif_uri), old_short_id

update_obj_id = lambda resource: update_iiif_uri(resource["@id"])[0]

# -------------------------------------------------------------
# ANNOTATIONS

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

    manifest_uri, old_short_id = update_iiif_uri(target["within"]["@id"])
    canvas_uri, _ = update_iiif_uri(target["full"])
    target["within"]["@id"] = manifest_uri
    target["full"] = canvas_uri
    return target, old_short_id

def update_target_recursive(target: Any, inner: bool = False):
    if isinstance(target, str):
        target, old_short_id = update_iiif_uri(target)
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
        old_short_id = result[0][1]
    else:
        raise TypeError(f"only supported types are 'str', 'dict', 'list'. got {type(target)}")

    return target, old_short_id

def update_annotation(annotation: Dict):
    annotation["@id"] = ""  # aiiinotate recreates an @id

    # 1. update the annotation.on
    target = annotation.get("on")
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

    # 3. drop empty bodies
    #   and drop the "$root_url/sas/full_text" key from body (auto-generated in SAS, useless in aiiinotate)
    body_out = []
    for item in body:
        if not len(item["chars"]) or item["chars"] == "<p></p>":
            continue
        k_list = [ k for k in item.keys() if k.endswith("/sas/full_text") ]
        for k in k_list:
            del item[k]
        body_out.append(item)

    annotation["resource"] = body_out
    return annotation


def update_annotation_list(annotation_list: Dict) -> Dict:
    annotation_array = []
    for annotation in annotation_list.get("resources", []):
        annotation_array.append(update_annotation(annotation))
    annotation_list["resources"] = annotation_array
    return annotation_list

# -------------------------------------------------------------
# MANIFESTS

def update_image(image: Dict) -> Dict:
    image["@id"] = update_obj_id(image)
    image["on"] = update_iiif_uri(image["on"])[0]
    return image


def update_canvas(canvas: Dict) -> Dict:
    canvas["@id"] = update_obj_id(canvas)
    canvas["images"] = [
        update_image(i) for i in canvas["images"]
    ]
    return canvas


def update_sequence(sequence: Dict) -> Dict:
    # manifest.sequence["@id"] is not important for aiiinotate
    # => don't generate an @id if there isn't one.
    if "@id" in sequence.keys():
        sequence["@id"] = update_obj_id(sequence)
    sequence["canvases"] = [
        update_canvas(c) for c in sequence["canvases"]
    ]
    return sequence


def update_manifest(manifest: Dict) -> Dict|None:
    # in some cases, instead of a manifest, we have extracted { "response": "...", "reason": "..." }
    # this is becase some data was deleted in aikon.
    # in this case, don't migrate the manifest.
    if "@id" not in manifest.keys():
        return None

    manifest["@id"] = update_obj_id(manifest)
    manifest["sequences"] = [
        update_sequence(s) for s in manifest["sequences"]
    ]
    return manifest

# -------------------------------------------------------------
# PIPELINE

def pipeline(datatype: Literal["annotations","manifests"]):
    indir = ANNOTATIONS_DIR if datatype == "annotations" else MANIFESTS_DIR
    outdir = ANNOTATIONS_MIGRATE_DIR if datatype == "annotations" else MANIFESTS_MIGRATE_DIR

    # update each AnnotationList and write to file
    for fp, data in tqdm(
        json_read_from_dir(indir),
        desc=f"updating {datatype}",
        total=len(list(indir.iterdir()))
    ):
        fn = Path(fp).name

        if datatype == "annotations":
            data = update_annotation_list(data)
        else:
            data = update_manifest(data)
            # to avoid duplicate manifests, update file basename
            # to new short ID => deduplicate when there are multiple
            # region IDs for the same manifest
            if data is not None:
                fn = f"{data['@id'].split('/')[-2]}.json"

        if data is not None:
            fp_out = outdir / fn
            json_write(data, fp_out)
    return


def migrate(datatype: Literal["annotations","manifests"]):
    logger.info(f"RUNNING: {STEP_NAME}")
    pipeline(datatype)
    logger.info(f"COMPLETED: {STEP_NAME}  (* ´ ▽ ` *)")
