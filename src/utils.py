import os
from pathlib import Path

def path_from_env(env_var: str) -> Path:
    v = os.getenv(env_var, default="")
    if v == "":
        raise ValueError(f"Env var {env_var} is undefined and path cannot be set !")
    path = Path(os.getenv(env_var, default=""))
    if not path.is_absolute():
        path = Path(ROOT_DIR / path)
    return path


def set_and_make_path(env_var: str, is_dir=True) -> Path:
    path = path_from_env(env_var)
    if is_dir:
        path.mkdir(exist_ok=True)
    return path


SRC_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SRC_DIR.parent.resolve()
LOG_DIR = set_and_make_path("LOG_DIR")
OUT_DIR = set_and_make_path("OUT_DIR")
SAVE_FILE = Path(OUT_DIR / "_save.json")
