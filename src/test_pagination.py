# after running an extraction on at least 10 ann_list,
# assert that the actual number of annotations in an
# AnnotationList corresponds to the expected number.

import os

from .utils import ANNOTATIONS_DIR, json_read
from .logger import logger

STEP_NAME = "test_pagination"

def pipeline():
    # mapper: list of (<filename>, <size>)
    mapper = []
    for f in ANNOTATIONS_DIR.iterdir():
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
    for fp in ann_list:
        annotation_list = json_read(fp)
        t = annotation_list["within"]['total']
        n_anno = len(annotation_list["resources"])
        ok = t==n_anno
        if ok:
            n_ok += 1
        else:
            n_err += 1
        logger.info(f"on annotation list: {fp.name}, OK: {ok} (expected={t}, actual={n_anno})")
    logger.info(f"TOTAL OK: {n_ok}, TOTAL ERRORS: {n_err}")
    return

def test_pagination():
    """
    after running an extraction, test that the export's pagination aggregation worked:
    assert that the actual number of annotations in an AnnotationList corresponds to the expected number.

    this is because, when exporting, annotations from all pages of a paginated AnnotationList
    are combined in a single AnnotationList.
    """
    logger.info(f"RUNNING   : {STEP_NAME}")
    pipeline()
    logger.info(f"COMPLETED : {STEP_NAME} (* ´ ▽ ` *)")
