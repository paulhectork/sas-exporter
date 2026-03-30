import re
import os
import json
import aiohttp
from pathlib import Path
from typing import Dict, Generator, Tuple, List

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
        if path.is_file():
            raise FileExistsError
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

def json_read_from_dir(path: Path|str) -> Generator[Tuple[str|Path, Dict], None, None]:
    """
    generator that yields (filepath, <JSON contents as dict>) for all files in a directory
    if there was a problem parsing a file, yield `fp, {}` for type consistency
    """
    for fp in Path(path).iterdir():
        if fp.is_file():
            try:
                yield fp, json_read(fp)
            except orjson.JSONDecodeError:
                yield fp, {}
        else:
            yield fp, {}
    return

def json_parse(data: str) -> Dict:
    """parse a string to a Dict"""
    return orjson.loads(data)


def json_dumps(data: Dict|List) -> bytes:
    return orjson.dumps(data, option=orjson.OPT_INDENT_2)


def json_write(data: Dict|List, path: Path|str) -> None:
    """write a Dict to a JSON file"""
    with open(path, mode="wb") as fh:
        d_str = json_dumps(data)
        fh.write(d_str)


def json_read_if_exists(path: Path|str) -> Tuple[Dict, bool]:
    """return a pair of <dict>, <file exists>"""
    if not Path(path).exists():
        return {}, False
    data = json_read(path)
    return data, True

def make_session(max_connections: int = 10) -> aiohttp.ClientSession:
    # NOTE: we define a timeout on read time only, not on waiting for a free connection or anything else.
    # a significant time in our pipeline is spent waiting for a free conneciton.
    # this timeout is shared by all requets made by the session.
    #   - total : overall timeout for the entire request (connection + read)
    #   - connect : time to acquire a connection from the pool + time to establish the TCP socket (i.e. it covers both pool wait and socket connection)
    #   - sock_connect : time to establish the TCP socket only (excludes pool wait time)
    #   - sock_read : time to wait for data from the server after the request is sent    return aiohttp.ClientSession(
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=max_connections),
        timeout=aiohttp.ClientTimeout(
            total=None,        # no hard cap on the full lifecycle
            connect=None,      # no cap on pool wait + sock_connect combined
            sock_connect=10,   # timeout for TCP handshake/DNS only, excludes pool wait
            sock_read=30       # timeout waiting for server response after request is sent
        )
    )

async def fetch_to_json(session: aiohttp.ClientSession, url: str, params: Dict = {}) -> Dict|List:
    """
    must be run in an `async with aiohttp.ClientSession(...) as session` block:
    """
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        r_text = await response.text()
    return json_parse(r_text)

URL_ROOT_REGEX = re.compile(r"^https?:\/\/[^\/]+,https?:\/\/[^\/]+")

SRC_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SRC_DIR.parent.resolve()
LOG_DIR = set_and_make_dir_from_env("LOG_DIR", ROOT_DIR)
OUT_DIR = set_and_make_dir_from_env("OUT_DIR", ROOT_DIR)
ANNOTATIONS_DIR = make_path(OUT_DIR / "annotations", True)
SAVE_OK_FILE = Path(OUT_DIR / "_save_ok.json")
SAVE_ERR_FILE = Path(OUT_DIR / "_save_err.json")
SAS_ENDPOINT = get_env_var("SAS_ENDPOINT")
MAX_CONNECTIONS = int(get_env_var("MAX_CONNECTIONS"))

ANNOTATION_LIST_TEMPLATE = {
    "@context": "http://iiif.io/api/presentation/2/context.json",
    "@type": "sc:AnnotationList",
    "@id": "",
    "resources": []
}
