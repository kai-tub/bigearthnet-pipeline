# The main purpose of this code is to upload the data
# from BigEarthNet v1 and to extract only the relevant metadata
# information for the full v2 pipeline.
# But the patch-uploading code should also work for v2 if it is
# needed in the future.
# The main motivation to upload the v1 patch data to the DB
# is to ensure consistency and to keep all of the processing logic within the DB.
# This also makes it A LOT easier to run offline tests with the old and new patch
# data. Like the other DB nushell script, the code is deeply intertwined with
# the SQL code, so make sure to always change names across all files!
use std log
use utils *

# Given the path to a BigEarthNet-S2 patch directory, jump to the path,
# parse all band TIFF files that match the BigEarthNet-S2 regex
# (but only the band B01 tif if `--only-b1` is set)
# And extract its data via `raster2pgsql -a -R`
# The append (`-a`) mode is only used for easier parsing.
# NOTE: -R is used which means that the tiff file is registered as an
# out-of-db raster!
# The output will contain a table that includes the
# - patch_name
# - the read band 
# - the SQL raster data
def "postgres prepare bigearthnet-patch-dir" [
  --only-b1,
]: path -> table<patch_name: string, band: string, raster: string> {
    cd $in
    let band_re = if $only_b1 { 'B01' } else { 'B..' };
    # minimize disk access, as this is the major bottleneck with BEN 
    ls | get name | each {
      |n|
      let r = $n | parse --regex ('(?<patch_name>.*)_(?<band>' + $band_re + ')\.tiff?$')
      if ($r | is-empty) {
        return null
      }
      $r | merge (
        # if wrapped inside of do {} | complete block than the entire output of stdout
        # would be stored on disk and with the parse command it should be done on-the-fly
        raster2pgsql -a -R $n | parse --regex 'VALUES \((?<raster>.*)\)'
      )
    } | flatten
}

# Path to the BigEarthNet patch folders (should work with v1 and v2)
# Will iterate over the individual TIF files within each folder and _append_ them
# to the `table` with the expected form:
# - patch_name
# - band
# - rast
# NOTE: Currently, the function hard-codes that the data is loaded out-of-db!
# It is possible to change/parameterize this value but then the batch size
# should be greatly reduced as it will quickly become a memory heavy application!
export def "postgres insert bigearthnet-patch-dirs" [
  table: string # Name of the table the data will be *appended* to
  --only-b1 # Only extract the data from the band B01
  --conflict-resolution: string = "DO NOTHING"; # The string will be injected after `ON CONFLICT`
]: list<path> -> any {
  let dirs = $in
	# Due to the minimal data submitted, the DB should ideally sit behind a connection pooler
	# to increase the performance. NOTE: After an internal test with a connection pooler, the
	# performance did _not_ significantly increase. So I moved the comment from the doc-string to
	# here just as an FYI.
  $'BEGIN; INSERT INTO "($table)" ' + '("patch_name", "band", "rast") VALUES ' + (
    $dirs
    | each { postgres prepare bigearthnet-patch-dir --only-b1=($only_b1) }
    | flatten
		| each {|r| $"\('($r.patch_name)','($r.band)',($r.raster)\)"}
    | str join ","
  ) + $' ON CONFLICT ($conflict_resolution); END;'
	| psql --set ON_ERROR_STOP=on
	| complete
	| complete check --cmd "psql"
}

# Given a list of BigEarthNet patch directories,
# insert the data into a PostgreSQL DB.
# The connection logic assumes that the PSQL environment variables are set.
# It will also only upload the b1 patches, as for the current code, we are
# only interested in the extent of each patch and do not access the raster data!
# To minimize the DB transactions, the patches are processed in batches.
# Due to a bug in nushell at the time of writing, the parallel execution
# _swallows_ errors and will iterate over all groups and _not_ early exit!
# However, at the end an error is reported and the exit status is correctly set!
# By default the function will do nothing on conflict.
# As a result, the function should be idempotent
export def "main upload bigearthnet-patches" [
	root_directory: path # path to the BigEarthNet root directory
  --parallel-workers: int = 6 # Number of parallel workers
  --batch-size: int = 5000 # Number of PG transactions to group together before committing
  --conflict-resolution: string = "DO NOTHING" # define <conflict_action> that will be injected into SQL command
	--debug-patch-limit: int = 0 # Debug flag that stops searching for patches after reaching this number
  --only-b1 # Only upload Band01
]: nothing -> any {
  ^pg_isready | complete | complete check --cmd "pg_isready"
  # Remember: The table is linked to the database! 
  let table = "bigearthnet_v1_patches"
  # truncating for repeatability is a bad design as it should be `truncate cascade`
  # and that is something we don't want to do!
  # rather solve it by doing nothing on conflict
	# if $truncate_table {
	# 	$'truncate "($table)"'
	# 	| psql --set ON_ERROR_STOP=on
	# 	| complete
	# 	| complete check --cmd "psql"
	# }

	# --max-results=0 is the same as having no limit!
	let patches = ^fd --type directory --max-results $debug_patch_limit --absolute-path --full-path '.*/S2.*_\d\d?_\d\d?$' $root_directory
		| lines

	log info $"Found ($patches | length) patch directories that will be processed."

	# Do I need pueue?
	# I do not really think so. Yes, it takes a bit to execute but if the operation fails
	# the entire table needs to be truncated either way -> Little value in restarting the jobs
  # let inp = $in
  #
  # par-each currently swallows errors and continues with the execution:
  # https://github.com/nushell/nushell/issues/10960
	$patches | group $batch_size | par-each --threads $parallel_workers {
		|group| 
    $group | postgres insert bigearthnet-patch-dirs --only-b1=($only_b1) --conflict-resolution $conflict_resolution $table
	}
}

# Quick & dirty function to extract the label information of each .json file
# /data/datasets/BigEarthNet/BigEarthNet-S2/BigEarthNet-v1.0/S2B_MSIL2A_20180204T94160_66_57
# needs to be converted to 19-class nomenclature before uploading!
export def "main extract s2-labels" [
	root_directory: path # path to the BigEarthNet root directory
	--debug-patch-limit: int = 0 # Debug flag that stops searching for patches after reaching this number
] {
	let patches = ^fd --type directory --max-results $debug_patch_limit --absolute-path --full-path '.*/S2.*_\d\d?_\d\d?$' $root_directory
		| lines

	log info $"Found ($patches | length) patch directories that will be processed."

  $patches | par-each {
    |patch_path|
    let patch = $patch_path | path basename
    open --raw $"($patch_path)/($patch)_labels_metadata.json" 
    | from json 
    | get labels
    | {patch: $patch label_43: $in}
  }
  | flatten
  | save -f /tmp/labels.csv
}

# Simple function that uses `ogr2ogr` to extract the BigEarthNet-v1 geometries
# from a predefined table and writes them as `Geoparquet` files.
# The output will be written to the current working directory and will be called 
# `bigearthnet_v1_spatial_<crs>.parquet`
# It will use the same environment variables as `psql` to connect to the database.
# The table should have the following columns:
# - `patch_name`
# - `geom`
# - `sensing_time`
# Take a look at the related `flyway` SQL code + documentation for more information
# NOTE: Geoparquet only allows for a single CRS per geometry column!
# To be explicit, multiple files are created!
# The function will also call the associated update function that generates/fills
# the table with the data from the previously uploaded BigEarthNetv1 data.
export def "main download bigearthnet-v1-geoms" [
  --debug # If in debug mode, the function allows to create 'empty' parquet files
]: {
  ^pg_isready | complete | complete check --cmd "pg_isready"
  # PG table name that contains the polygons to export
  let table = "bigearthnet_v1_patches_geoms"
  # Idempotent sql statement that generates/updates the table above
  let bigearthnet_v1_patches_geoms_preparation_sql = 'select fill_bigearthnet_v1_patches_geoms();';

  log info "Ensuring that the table to download is up-to-date/filled."
  $bigearthnet_v1_patches_geoms_preparation_sql
	| psql --set ON_ERROR_STOP=on
	| complete
	| complete check --cmd "psql"

  let base_name = "bigearthnet_v1_spatial"
  # maybe exclude the season information, as _strictly_ speaking I am only interested the timestamp information?
  # yeah, it makes it easier to follow what I am up to
  let ben_crses = [32629 32631 32632 32633 32634 32635 32636]
  $ben_crses | each {
    |crs| 
    let name = $"($base_name)_($crs)"
    let fname = $"($name).parquet"
    # ogr2ogr is quite buggy when it comes to multi-crs within a singel geometry column
    # it will select the first CRS it sees from the table not the SQL result!
    # As a result, I have to manually set the 'correct' CRS with `-a_srs` after filtering for it
    ^ogr2ogr -progress -lco GEOMETRY_NAME=geom -a_srs $"EPSG:($crs)" $fname -sql $"select patch_name, geom, sensing_time from \"($table)\" where st_srid\(geom\) = ($crs) order by patch_name, geom, sensing_time" $"PG:dbname=($env.PGDATABASE) host=($env.PGHOST) port=($env.PGPORT)"
      | complete | complete check --cmd "ogr2ogr"

    log info $"Checking the output via ogrinfo -so ($fname) ($base_name):"

    let ogr_out = ^ogrinfo -json -so $fname $name | complete | complete check --msg "ogrinfo" | from json
    let read_crs = ($ogr_out | get layers.0.geometryFields.coordinateSystem.projjson.id.code.0)
    if ($read_crs == $crs) {
      log info $"Parquet ($fname) contains correct CRS"
    } else {
      error make -u {
        msg: $"Resulting parquet file: ($fname) does not have expected CRS ($crs) set!"
      }
    }
    if (($ogr_out | get layers.0.featureCount) == 0) {
      if $debug {
        log info $"Parquet ($fname) has empty featureCount but ignoring due to `--debug`"
      } else {
        error make -u {
          msg: "Resulting parquet file is empty!"
        }
      }
    }
  }
  null
}

# Given the path to the BigEarthNet v1 root directory
# upload the data to the database so that the relevant
# metadata can be generated and downloaded from the database
# into individual geoparquet files (one for each CRS).
# These are used in the pipeline to generate v2 of the dataset.
# The output should be stable and not change (assuming the geoparquet specification doesn't change!)
# For more details, see the subcommands, which this function wraps.
# Note that this function may take a considerable time to execute, especially the uploading part
# and that it is best to submit it to a scheduler of some sort.
# The function should be idempotent and correctly skip over the patches that were already uploaded.
# NOTE: Due to a bug in `par-each` at the time of writing, the upload won't fail even if the uploading
# produces errrors. The resulting downloaded files may be invalid in this case. Please take a look at
# standard out to ensure that no errors were reported. Though the function should remain idempotent and can
# be executed again afterwards to 'fix' the interruption.
export def main [
	v1_root_directory: path # path to the BigEarthNet v1 root directory
  --target-dir: path = "./" # path where the metadata files should be written to
  --parallel-workers: int = 6 # Number of parallel workers (can be set quite high!)
] {
  let dir = $target_dir | path expand
  mkdir $dir

  log info "About to upload the patches"
  log info "NOTE: This might take a considerable amount of time!"
  main upload bigearthnet-patches $v1_root_directory --parallel-workers=($parallel_workers) --only-b1=true

  log info "Finished uploading the data!"
  do {
    cd $dir
    log info $"About to download the metadata files to ($dir)"
    main download bigearthnet-v1-geoms
  }
  log info "Finished generating the metadata!"
}

