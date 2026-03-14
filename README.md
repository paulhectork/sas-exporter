# sas-exporter: export annotations from SAS

Provide the URL to an SAS endpoint, and export all annotations stored on it !

---

## Usage

requires [uv](https://docs.astral.sh/uv/getting-started/installation/) and git.

1. Install 

```bash
git clone git@github.com:paulhectork/sas-exporter.git
cd sas-exporter
uv init
```

2. Setup

```bash
cp .env.template .env
# now, complete your .env file to your needs
```

3. Use

```bash
uv run main.py
```

---

## Workflow

1. **get manifests**: query `$SAS_ENDPOINT/manifests` to get the collection of all manifests indexed in the SAS instance
2. **get annotations**: for each manifest, query the `search-api` to retrieve all annotations related to the manifest
3. **progress saving**: if the program stops before completing, the files `$OUT_DIR/_save_ok.json` and `$OUT_DIR/_save_err.json` track respectively the manifests processed successfully and the ones that failed. 
    - if `_save_ok.json` contains items, they will not be redownloaded on the next runs of `sas-exporter`
    - annotations of the manifests listed in `_save_err.json` will, however, be redownloaded. 

---

## Output structure

```
$OUT_DIR/                         # output directory
 |_ manifests_collection.json     # IIIF collection of all manifests indexed in SAS
 |_ _save_ok.json                 # log of all successful annotation downloads
 |_ _save_err.json                # log of all failed annotation downloads at the previous step
 |_ annotations/                  # folder storing all annotations
     |_ $manifest_short_id.json   # all annotations related to a single manifest
```

---

## Optimizations

- fast JSON parsing and stringifying with [`orjson`](https://github.com/ijl/orjson)
- entierly async, since SAS-exporting is heavily I/O-bound, using [`asyncio`](https://docs.python.org/3/library/asyncio.html) and [`aiohttp`](https://docs.aiohttp.org/en/stable/index.html).

--- 

## License

GNU GPL 3.0
