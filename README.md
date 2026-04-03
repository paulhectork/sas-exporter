# sas-exporter: export annotations from SAS

Provide the URL to an SAS endpoint, and export all annotations stored on it !

---

## exporting data

requires [uv](https://docs.astral.sh/uv/getting-started/installation/) and git.

1. **install**

```bash
git clone git@github.com:paulhectork/sas-exporter.git
cd sas-exporter
uv init
```

2. **setup**

```bash
cp .env.template .env
# now, complete your .env file to your needs
```

3. **export data**

```bash
uv run main.py export
```

4. **(optional) retry export on specific errors**: after a first failed export, redownload errors only. if you set a value other than `all`, retry only for specific errors.

```bash
uv run main.py --retry all|timeout|http|http:XXX  # XXX = http error code
```

---

## extra commands

these can only be run after a first export has been done !!!

0. **analyze export success**: how many successful/failed exports, which exports, which errors were encountered...

```bash
uv run main.py output-analysis
```

1. **test if pagination worked** (works only if `$EXPORT_STRATEGY=search-api`). this is by no way a real test suite, but after running a (partial) export you can test that the number of annotations is the same as was expected

```bash
uv run main.py test-pagination
```

2. **generate a list of "valid" AnnotationLists**. (useless if `$EXPORT_STRATEGY=canvas`) valid AnnotationLists are those whose manifests can be fetched correctly (useful to import into [aiiinotate](github.com/Aikon-platform/aiiinotate))

```bash
uv run main.py clean-manifest-errors
```

3. [AIKON-SPECIFIC] **update annotation JSON structure** from regions-extraction-as-target to digitization-as-target (see docstring of `./src/migrate_structure`).

```bash
uv run main.py migrate-structure
```

---

## export workflow

1. **get manifests**: query `$SAS_ENDPOINT/manifests` to get the collection of all manifests indexed in the SAS instance
2.  **get annotations**: 
    - if `$EXPORT_STRATEGY=search-api`: for each manifest, query the `/search-api/` to retrieve all annotations related to the manifest
    - if `$EXPORT_STRATEGY=canvas`: for each manifest, build an index of canvases. for each canvas, query the `/annotation/search` route to get annotations for the manifest
    - at the end, concatenate all results for a manifest into a single AnnotationList.
3. **progress saving and error logging**: 
    - `$OUT_DIR/_save_ok.json` saves all successfull downloads, `$OUT_DIR/_save_err.json` logs all manifests that could not be processed.
    - if the program stops before completing, the files `$OUT_DIR/_save_ok.json` are not redownloaded. annotations in `$OUT_DIR/_save_err.json` are redownloaded.

---

## output structure

```
$OUT_DIR/                            # output directory
 |_ manifests_collection.json        # IIIF collection of all manifests indexed in SAS
 |_ _save_ok.json                    # log of all successful annotation downloads
 |_ _save_err.json                   # log of all failed annotation downloads at the previous step
 |_ annotationlists_valid.txt        # output of `clean-manifest-errors`
 |_ output_analysis.json             # output of `output-analysis` 
 |_ annotations/                     # folder storing all annotations
 |   |_ $manifest_short_id.json      # all annotations related to a single manifest
 |_ annotations_anno_to_digit  # output of `anno-to-digit`, same structure as `annotations/`
     |_ $manifest_short_id.json      # updated annotationlist
```

---

## optimizations

- fast JSON parsing and stringifying with [`orjson`](https://github.com/ijl/orjson)
- entierly async, since SAS-exporting is heavily I/O-bound, using [`asyncio`](https://docs.python.org/3/library/asyncio.html) and [`aiohttp`](https://docs.aiohttp.org/en/stable/index.html).

---

## license

GNU GPL 3.0
