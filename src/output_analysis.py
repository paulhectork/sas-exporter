import pathlib
from typing import List, Dict, Literal

from .utils import (
    SAS_ENDPOINT,
    SAVE_OK_FILE,
    SAVE_ERR_FILE,
    OUT_DIR,
    EXPORT_STRATEGY,
    json_dumps,
    json_write,
    json_read,
    manifest_uri_to_short_id
)
from .logger import logger

STEP_NAME = "error_analysis"

def expand_manifest_short_id(manifest_uri: str) -> dict[str, str|None]:
    """
    a manifest's shortId has the structure:
        {witness_id}_{digitization_id}_{regions_id}?
    return an object-description of it
    """
    short_id = manifest_uri_to_short_id(manifest_uri)
    short_id = short_id.split("_")
    assert len(short_id) == 2 or len(short_id) == 3
    return {
        "wit_id": short_id[0],
        "digit_id": short_id[1],
        "region_id": short_id[2] if len(short_id) == 3 else None
    }

def get_alt_matches_for_manifest_uri(
    match_for: Literal["500", "KeyError"],
    manifest_uri:str,
    ok_json:dict,
) -> list[dict[str,str]]:
    """
    for key errors, see if there are successful annotation
        extractions for the sane digitization as the failed one:
        (same witness_id and digitization_id, different region_id)
    for HTTP 500 errors, see if there are successful annotation
        extractions for the witness
        (same witness_id, different digitization_id and region_id)
    """
    short_id_dict = expand_manifest_short_id(manifest_uri)
    if str(match_for) == "500":
        cond = lambda ok_item: ok_item["short_id_dict"]["wit_id"] == short_id_dict["wit_id"]
    else:
        cond = lambda ok_item: (
            ok_item["short_id_dict"]["wit_id"] == short_id_dict["wit_id"]
            and ok_item["short_id_dict"]["digit_id"] == short_id_dict["digit_id"]
        )
    return [
        {
            "manifest_uri": _manifest_uri,
            "path": ok_dict["path"]
        }
        for _manifest_uri, ok_dict in ok_json.items() if cond(ok_dict)
    ]

def get_alt_matches(match_for: Literal["500", "KeyError"], errors: list[dict], ok_json: dict):
    key = "witness" if match_for == "500" else "digitization"
    alt_matches = []
    no_alt_matches = []
    for e in errors:
        _alt_matches = get_alt_matches_for_manifest_uri(match_for, e["manifest_uri"], ok_json)
        if len(_alt_matches):
            alt_matches.append(e["manifest_uri"])
        else:
            no_alt_matches.append(e["manifest_uri"])
    return {
        f"{key}_matches_count": len(alt_matches),
        f"no_{key}_matches": no_alt_matches
    }

def pipeline():
    """
    1. get each error and count # or errors
    2, for ClientResponseError, get each HTTP error and # for each
    3. KeyErrors should be caused by deleted regions
          => see if there's a matching digitization or
          another region for the same digitization
    4. 500 errors should be caused by a deleted digitization
          => see if there's a matching witness for the manifestShortId
    """
    ok_json = json_read(SAVE_OK_FILE)
    err_json = json_read(SAVE_ERR_FILE)

    # add short_id_dict to ok_json
    ok_json = {
        manifest_uri: {
            "short_id_dict": expand_manifest_short_id(manifest_uri),
            **info
        }
        for manifest_uri, info in ok_json.items()
    }

    out = {
        "endpoint": SAS_ENDPOINT,
        "strategy": EXPORT_STRATEGY,
        "ok_count": len(ok_json.keys()),
        "error_count": len(err_json.keys()),
        "err_desc": {
            "error_types": list(set(
                e["error_type"]
                for e in err_json.values()
            )),
            "errors": []
        }
    }
    for err_type in out["err_desc"]["error_types"]:
        # add manifest_uri to error descriptions.
        errors = [
            { "manifest_uri": manifest_uri, **e }
            for manifest_uri, e in err_json.items()
            if e["error_type"] == err_type
        ]
        err_desc = {
            "error_type": err_type,
            "error_count": len(errors),
        }
        # get distinct HTTP errors and number of errors for each
        if err_type == "ClientResponseError":
            all_http_status = list(set( str(e["http_status"]) for e in errors ))
            err_desc["http_error_statuses"] = all_http_status
            err_desc["detail"] = []
            for http_status in all_http_status:
                http_status = str(http_status)
                http_status_errors = [
                    e for e in errors
                    if str(e["http_status"]) == http_status
                ]
                http_err_desc = {
                    "http_status": http_status,
                    "count": len(http_status_errors)
                }
                # for error 500s, see if there are matching witnesses.
                if http_status == "500":
                    http_err_desc = {
                        **http_err_desc,
                        **get_alt_matches(http_status, http_status_errors, ok_json)
                    }
                err_desc["detail"].append(http_err_desc)

        # for KeyErrors, see if there are matching digitizations
        elif err_type == "KeyError":
            err_desc = {
                **err_desc,
                **get_alt_matches(err_type, errors, ok_json)
            }
        out["err_desc"]["errors"].append(err_desc)

    print(json_dumps(out).decode("utf-8"))
    json_write(out, OUT_DIR / "_output_analysis.json")


def output_analysis():
    logger.info(f"RUNNING   : {STEP_NAME}")
    pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
