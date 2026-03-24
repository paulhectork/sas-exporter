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

---

## extra commands

these can only be run after a first export has been done !!!

1. **test if pagination worked**. this is by no way a real test suite, but after running a (partial) export you can test that the number of annotations is the same as was expected

```bash
uv run main.py test_pagination
```

2. **generate a list of "valid" AnnotationLists**. valid AnnotationLists are those whose manifests can be fetched correctly (useful to import into [aiiinotate](github.com/Aikon-platform/aiiinotate))

```bash
uv run main.py clean_manifest_errors

3. [AIKON-SPECIFIC] **update annotations** from regions-extraction-as-target to digitization-as-target (see docstring of `./src/clean_anno_to_digit`).

```bash
uv run main.py clean_anno_to_digit`
```

---

## export workflow

1. **get manifests**: query `$SAS_ENDPOINT/manifests` to get the collection of all manifests indexed in the SAS instance
2. **get annotations**: for each manifest, query the `search-api` to retrieve all annotations related to the manifest
3. **progress saving**: if the program stops before completing, the files `$OUT_DIR/_save_ok.json` and `$OUT_DIR/_save_err.json` track respectively the manifests processed successfully and the ones that failed. 
    - if `_save_ok.json` contains items, they will not be redownloaded on the next runs of `sas-exporter`
    - annotations of the manifests listed in `_save_err.json` will, however, be redownloaded. 

---

## output structure

```
$OUT_DIR/                            # output directory
 |_ manifests_collection.json        # IIIF collection of all manifests indexed in SAS
 |_ _save_ok.json                    # log of all successful annotation downloads
 |_ _save_err.json                   # log of all failed annotation downloads at the previous step
 |_ annotationlists_valid.txt        # output of `clean_manifest_errors`
 |_ annotations/                     # folder storing all annotations
 |   |_ $manifest_short_id.json      # all annotations related to a single manifest
 |_ annotations_clean_anno_to_digit  # output of `clean_anno_to_digit`, same structure as `annotations/`
```

---

## optimizations

- fast JSON parsing and stringifying with [`orjson`](https://github.com/ijl/orjson)
- entierly async, since SAS-exporting is heavily I/O-bound, using [`asyncio`](https://docs.python.org/3/library/asyncio.html) and [`aiohttp`](https://docs.aiohttp.org/en/stable/index.html).

--- 

## license

GNU GPL 3.0
