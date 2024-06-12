# BigEarthNet v2 Pipeline

<img alt="Powered by nix" src="https://img.shields.io/badge/Powered%20By-Nix-blue?style=flat&logo=snowflake">

## Overview

On a high-level, the pipeline:

1. Derives the original Sentinel-2 L1C tile names that were used to construct the BigEarthNet v1.0 dataset.
1. Searches for the current L1C tiles in the new [Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu/).
1. Downloads the L1C tiles from the CDSE service.
1. Processeses the L1C to L2A tiles with [sen2cor v2.11](https://step.esa.int/main/snap-supported-plugins/sen2cor/sen2cor-v2-11/).
1. Divides the tiles into 1200m x 1200m patches, associates each patch with a pixel-level reference map and scene-level multi-labels.
1. Aligns the constructed patches and metadata with the BigEarthNet v1.0 Sentinel-1 patches
to construct the _complete_ dataset.
1. Compresses the resulting dataset to prepare it for distribution.

The following sections describe how to execute the individual steps and provides
some additional information in the `Detail` subsections.

## Preparation

1. [Download & install Nix](https://github.com/DeterminateSystems/nix-installer) with `flake` support
1. Manually download the [CORINE Land Cover (CLC) 2018 vector database (`gpkg`) via the copernicus.eu website](https://land.copernicus.eu/en/products/corine-land-cover/clc2018#Download),
  as it cannot be automatically downloaded in a script.
    - Add it to the store via `nix-store --add-fixed sha256 U2018_CLC2018_V2020_20u1.gpkg`
1. Enter the prepared environment via `nix develop --impure .#`.

## Get Sentinel-2 L1C tile IDs

To generate the mapping from the original Sentinel-2 L1C names from BigEarthNet v1.0
to the new [Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu/) IDs, run:
```bash
nix run .#ben-s2-tile-names-to-ids-runner
```

This program will generate the required `bigearthnet_s2_tile_ids.csv`.

### Details

It is important to note that if the resulting CSV file differs from the reference
CSV file stored in `tracked_artifacts`, the source tiles from CDSE have changed,
and the resulting dataset will most likely differ from the current one.

## Download Sentinel-2 L1C tiles

To download the tiles with the given IDs, run:
```bash
nix run .#ben-s2-l1c-tile-downloader -- --csv-path <PATH_TO_TILE_IDS.CSV> --output-dir <PATH_TO_DIR>
```

> [!IMPORTANT]
> You might need to re-run the above line multiple times until it succeeds.
> After a few seconds, all tile downloads will have been added to the job queue and will continue
> to be downloaded even if the main process is interrupted/stopped.

### Details
Previous experience has shown that the CDSE service is quite unreliable
and often fails in the middle of the download, causing the downloader script to fail for those tiles.
The only reliable option would be to _force_ retry for every failure (as many different exit codes have
been observed in testing) multiple times.
However, even then, data was unavailable and required contacting support and for them to fix the underlying issue first.
To avoid overloading the servers due to internal service issues on their side, the recommendation is to retry the script
manually 3 times (it will skip over already successfully downloaded tiles).
If it continues to fail, please get in touch with the [CDSE support](https://helpcenter.dataspace.copernicus.eu/) by providing the error message in the associated `.err` files.

You should then check the output of the tiles by comparing the output file from:
```bash
nix run .#ben-s2-l1c-hasher <download-dir>
```
with the file in `trached-artifacts/ben_s2_l1c_hashes.csv` via `diff`.

We have seen that the resulting zip files have changed without the ID changing:
```diff
35c35
< 4754499f-f990-556c-954e-713d49128c34.out,770c9bfbda7e310a55ca07e8dc429b40c9e5e30dfb8e8fe1a304d46d9b012e08
---
> 4754499f-f990-556c-954e-713d49128c34.out,d6644add4d88e2741083c2ef1a605d140d63d037da42566572481142c271833f
50c50
< 5f680c98-a4a9-588f-963f-d30f8d2138b1.out,b5e20b02f4ae1e83e69d73269c761ed7de54ae2bd9ea3e28b111a9ab2ce33209
---
> 5f680c98-a4a9-588f-963f-d30f8d2138b1.out,51f16658059b133d89fe590eac5cd7a8ab762cc9d9272137a7a0c1f64db85694
99c99
< c07fc9d1-cb2b-5cf8-b2f1-09b80df3ca8d.out,6229d8b3dfa256ac9c7cfe23bfb5f5a3abf977feb3cd306e8ebbe62811880874
---
> c07fc9d1-cb2b-5cf8-b2f1-09b80df3ca8d.out,cb65aa1a04961107d12f21ca58aaa2911ef1c1cf3612a3d336111771ec637b3d

```

In this instance, preview images and additional HTML metadata files were added to zip file.
However, the image data itself was unchanged.

## Pre-process the L1C to L2A tiles

To convert the Sentinel-2 L1C tiles to the L2A data product, run the following command:
```bash
nix run .#ben-s2-l1c-to-l2a-converter-runner -- <download-dir> --export-dir <l2a-dir>
```

Similar to the download command above, this command will continue to work through the
queue even if the main process is interrupted. The command is quite CPU-intensive
and it might take a couple of hours until it is finished.
The progress can be tracked by checking `pueue status --group sentinel-l1c-to-l2a`

Similar to the download program mentioned earlier, this program
will keep running through the queue even if the main program is interrupted.
It is a quite CPU-intensive task and may take a few hours to complete.
You can track the progress by using the command: `pueue status --group sentinel-l1c-to-l2a`.

## Generate the BigEarthNet v2 data

Generating the BigEarthNet v2 data is the most resource-intensive part of the pipeline
and might require adjusting the values in `postgres_conf.nix` to adjust
the pre-selected parameters on smaller servers.

The required services for the data generation need to be started via:
```bash
devenv up --tui=false
# or devenv up --keep-tui if the shell is configured correctly,
# which might not be the case for some SSH connections.
```

This command starts a dedicated [PostgreSQL](https://www.postgresql.org/) server and initializes the database
with the required schemas.

If the `flyway-runner` reports that the schema has been applied successfully,
stop the processes via `Ctrl+C`.
If the command fails, please see the [Debugging](#Debugging) section of the document.

After stopping the process, restart the environment in the background via `devenv up --tui=false & disown` or `pueue add 'devenv up --tui=false'`
(might require running `pueued --daemonize` first).

Next, start the data generation also in the background by running

```bash
pueue add "\
  nix run .#ben-data-generator -- \
  --L2As-root-dir=<L2As_DOWNLOAD_PATH> \
  --export-patch-dir=<+TIFF_DIR> \
  --export-segmentation-maps-dir=<+SEGMAPS_DIR> \
  --export-metadata-dir=<+METADATA_DIR> \
  --v1-metadata-dir=$BEN_V1_METADATA_DIR \
  --clc2018-gpkg-path=$CLC2018_PATH \
  --country-geojson-path=$BEN_COUNTRY_GEOJSON_PATH"
```

Note that the export directories will be created if they do not exist and
that the input artifacts are already provided as environment variables and linked
to the hashed version.

> ![IMPORTANT] The command might take a considerable amount of time to complete, so make sure to
> run it in a way that does not require an active ssh connection if necessary.

## Finalizing the dataset

After generating data and inserting it into the PostgreSQL database and the relevant data, the remaining steps
_merge_ the original Sentinel-1 data with the newly generated Sentinel-2 data.

```bash
nix run .#ben-data-finalizer -- \
  --target-dir <ALIGNED_DIR> \
  --s2-root-dir <PREV>/tiffs/ \
  --segmentation-root-dir <PREV>/segmaps/ \
  --s1-root-dir <EXTRACTED_S1_DIR> \
  --patch-id-label-mapping-file <PREV>/metadata/patch_id_label_mapping.csv \
  --patch-id-s2v1-mapping-file <PREV>/metadata/patch_id_s2v1_mapping.csv \
  --patch-id-split-mapping-file <PREV>/metadata/patch_id_split_mapping.csv \
  --patch-id-country-mapping-file <PREV>/metadata/patch_id_country_mapping.csv \
  --old-s1s2-mapping-file /nix/store/ln2dxzpmvf8bdwb8snrpnq3bv1yrfdy7-s1s2_mapping.csv
```

> [!TIP]
> This step requires the extracted BigEarthNet v1.0 Sentinel-1 directory

### Details
It is important to note that this step not only aligns/adds the Sentinel-1 data
but _also_ aligns the different outputs! The various metadata files are generated
independently from one another and shouldn't be used directly!
The `patch_id_label_mapping.csv` only contains patches with a minimum area
covered by label information. The `patch_id_split_mapping.csv` contains
the split mapping for _all_ generated patches. For more details, see the code comments.

## Prepare for Distribution

Finally, before publishing the dataset, compress the directories and metadata files into
individual `ZSTD` compressed archives to minize the number of individual files
and to greatly reduce the required download size.

```bash
nix run .#zstd-compressor -- <ALIGNED_DIR> --output-path <TO_BE_UPLOADED_DIR>
```

### Debugging
In the following, a few common issues and solutions are provided:

#### Cannot download `tile_names_and_links.csv`

The original source of the [Sentinel-2 tile names is the RSiM GitLab repository](https://git.tu-berlin.de/rsim/BigEarthNet-S2_tools/-/raw/master/files/tile_names_and_links.csv).
However, to ensure that this file remains accessible and is not changed, a compressed copy
of the csv file is stored under `tracked-artifacts/tile_names_and_links.csv.bz2`.

The file was compressed with
```bash
bzip2 /nix/store/ams5fsnv0adsgdsg49wmlq7rnmdq8j4d-tile_names_and_links.csv --best --stdout > tracked-artifacts/tile_names_and_links.csv.bz2
```

If the GitLab repository is down, the `flake.nix` dependency can be updated to use
the local version instead.

#### Checking the output against previous runs

Some intermediate results are also stored under `tracked-artifacts` in a compressed format (`zstd -19`)
to allow easier detection of possible output changes.
However, the contents should ideally be compared after extracting the data and joining them, to avoid changes
due to the compression options and/or to the CSV escape format.

To track whether or not the image data has remained the same, the script `tiff-hasher`
can be executed, and its output compared to the output of the previous run.
The script will calculate the `sha256` hash of each file, write it as a CSV file, and
then store the hash of the CSV file itself to allow for a quick comparison.
Concretly, run:

```bash
# generate the csv and sha256 file
nix run .#tiff-hasher <path-to-patches> /tmp/patch_hashes.csv
# compare the outputs with the previous run
# if there is no output, then the files do not differ and the resulting tiff
# files are identical to the previous run!
diff /tmp/patch_hashes.csv.sha256 <repository>/tracked-artifacts/patch_hashes.csv.sha256
# same for the segmentation patches
nix run .#tiff-hasher <path-to-segmentation-dir> /tmp/segmaps_hashes.csv
diff /tmp/segmaps_hashes.csv.sha256 <repository>/tracked-artifacts/segmaps_hashes.csv.sha256
```

The specific CSV files are not tracked in `Git` but the associated checksum file is.
The main reason why not all CSV files are tracked is that they are several hundred
MBs in size.

#### devenv up

If you see errors such as `FATAL: could not map anonymous shared memory`, it means
that the selected `XXX` size is too large and should be adjusted to according to the
available RAM of the server hosting the database.

If you see errors such as `start http server on :9999 failed error="listen tcp :9999: bind: address already in use"`,
then there is probably already a running `devenv up` process running.
Stop the managing process by killing the process that is named `process-compose`.

If you see a tui starting and then immediately stopping you can investigate the logs
via `cat /tmp/process-compose-<USERNAME>`.
But by opening it via `devenv up --keep-tui` you should be able to inspect which
job is failing and investigate the error.

If no additional information is printed, then nix is probably running via `nix-portable`
and adding an explicit shell might be required, such as: `nix develop .#tile-downloader-env --command sh`

> [!IMPORTANT]: If you run `devenv up` it _must_ be run from the root directory!
> Otherwise the `state` directory will be created inside the sub-directory and cause issues!

