import re
import os
import json
from pathlib import Path
from typing import Dict, Generator, Tuple, List

import asyncio
import aiohttp
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
    return aiohttp.ClientSession(
        # NOTE TCPConnector limit must be higher than Semaphore limit
        # so that connections are never the bottleneck;
        # the semaphore always fires first and the queue stays empty.
        # see `make_semaphore`
        connector=aiohttp.TCPConnector(limit=max_connections+5),
        timeout=aiohttp.ClientTimeout(
            total=None,        # no hard cap on the full lifecycle
            connect=None,      # no cap on pool wait + sock_connect combined
            sock_connect=10,   # timeout for TCP handshake/DNS only, excludes pool wait
            sock_read=30       # timeout waiting for server response after request is sent
        )
    )

def make_semaphore(max_connections: int = 10) -> asyncio.Semaphore:
    return asyncio.Semaphore(max_connections)

async def fetch_to_json(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, params: Dict = {}) -> Dict|List:
    """
    must be run in an `async with aiohttp.ClientSession(...) as session` block:
    """
    # NOTE: the semaphore actually controls the # of simultaneous connections.
    # this is necessary with nested asyncio.gathers or very large queues:
    # without it, the queue grows unbounded. this causes indefinite wait time,
    # which causes stale connections => HTTP errors (servers side reset => query fails)
    # on the contrary, using a semaphore moves the queue from aiohttp to asyncio.
    # requests added to the session queue are run immediately, so there are no timeouts
    # and server errors.
    async with semaphore:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            r_text = await response.text()
    return json_parse(r_text)

strategy = os.getenv("EXPORT_STRATEGY")
if strategy not in ["search-api", "canvas"]:
    raise ValueError(f"env variable EXPORT_STRATEGY expected one of ['search-api', 'canvas'], got '{strategy}'")
EXPORT_STRATEGY = strategy

iiif_host_repl = os.getenv("IIIF_HOST_REPL")
if iiif_host_repl is not None:
    iiif_host_repl = iiif_host_repl.split(",")
    if not len(iiif_host_repl) == 2:
        raise ValueError(f"SasExporter: env variable 'IIIF_HOST_REPL' must be 'old-host,new-host' (i.e., 'old.example.org,new.example.org'), got '{iiif_host_repl}'")
    iiif_host_repl = (iiif_host_repl[0], iiif_host_repl[1])  # ( old_host, new_host )
IIIF_HOST_REPL = iiif_host_repl

timeout = os.getenv("TIMEOUT", 30)
if timeout:
    try:
        timeout = float(timeout)
    except Exception as e:
        raise ValueError(f"Cannot retype env variable TIMEOUT to float: '{timeout}'")
TIMEOUT = timeout

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
