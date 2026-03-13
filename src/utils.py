import os
import json
from pathlib import Path
from typing import Dict, Tuple

import orjson

def get_env_var(env_var: str) -> str:
    v = os.getenv(env_var, default="")
    if v == "":
        raise ValueError(f"Env var {env_var} is undefined and path cannot be set !")
    return v


def path_from_env(env_var: str, root_dir) -> Path:
    v = get_env_var(env_var)
    path = Path(v)
    if not path.is_absolute():
        path = Path(root_dir / path)
    return path


def make_path(path: str | Path, is_dir: bool = True) -> Path:
    path = Path(path)
    if is_dir:
        path.mkdir(exist_ok=True)
    else:
        with open(path, mode="w"):
            pass
    return path


def set_and_make_dir_from_env(env_var: str, root_dir) -> Path:
    path = path_from_env(env_var, root_dir)
    make_path(path, True)
    return path


def json_read(path: Path|str) -> Dict:
    """read a Dict from a JSON file"""
    with open(path, mode="rb") as fh:
        d_str = fh.read()
        return orjson.loads(d_str)


def json_parse(data: str) -> Dict:
    """parse a string to a Dict"""
    return orjson.loads(data)


def json_write(data: Dict, path: Path|str) -> None:
    """write a Dict to a JSON file"""
    with open(path, mode="wb") as fh:
        d_str = orjson.dumps(data, option=orjson.OPT_INDENT_2)
        fh.write(d_str)


def json_read_if_exists(path: Path|str) -> Tuple[Dict, bool]:
    """return a pair of {<dict>}, <file exists>"""
    if not Path(path).exists():
        return {}, False
    data = json_read(path)
    return data, True


SRC_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SRC_DIR.parent.resolve()
LOG_DIR = set_and_make_dir_from_env("LOG_DIR", ROOT_DIR)
OUT_DIR = set_and_make_dir_from_env("OUT_DIR", ROOT_DIR)
ANNOTATIONS_DIR = make_path(OUT_DIR / "annotations", True)
SAVE_FILE = Path(OUT_DIR / "_save.json")
SAS_ENDPOINT = get_env_var("SAS_ENDPOINT")
N_PROCESSES = int(get_env_var("N_PROCESSES"))
