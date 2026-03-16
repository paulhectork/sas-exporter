# after running an extraction on at least 10 ann_list,
# assert that the actual number of annotations in an
# AnnotationList corresponds to the expected number.

import os
from pathlib import Path

import orjson
from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.utils import ANNOTATIONS_DIR

# mapper: list of (<filename>, <size>)
mapper = []
for f in os.listdir(ANNOTATIONS_DIR):
    mapper.append((f, os.stat(ANNOTATIONS_DIR / f).st_size))
mapper = sorted(mapper, key=lambda x: x[1])

# path to 10 largest annotation lists
n_ann_list = min(len(mapper), 10)  # test on 10 ann_list or the total # of ann_list if there are less than 10 in ANNOTATIONS_DIR.
ann_list = [
    ANNOTATIONS_DIR / filename
    for (filename, size) in mapper[-n_ann_list:]
]

# assert that within each annotation list, the # of annotations fits within.total (the expected total number of annotations)
n_ok = 0
n_err = 0
for m in ann_list:
    with open(m, mode="rb") as fh:
        txt = fh.read()
    d = orjson.loads(txt)
    t = d["within"]['total']
    n_anno = len(d["resources"])
    ok = t==n_anno
    if ok:
        n_ok += 1
    else:
        n_err += 1
    print(f"* on annotation list: {m.name}, OK: {ok} (expected={t}, actual={n_anno})")
print(f"TOTAL OK: {n_ok}, TOTAL ERRORS: {n_err}")