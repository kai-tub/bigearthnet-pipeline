use std assert
use std log

use utils *

# FUTURE: Consider adding various psql analyze calls

# Returns the names of the S2-L2A tiles that contain the desired regions
# These regions are part of the SQL pipeline and defined there in more detail!
# (Do not contain failing quality indicators, for example)
export def "main filtered_s2_tiles" []: nothing -> list<string> {
  '\copy (select distinct(patch_id).product_id from tile_patches_validation_and_alignment_result) to pstdout csv header'
  | psql --quiet --set ON_ERROR_STOP=on
	| complete
	| complete check --cmd "psql"
  | from csv
  | get product_id
}

export def "main fill tile_patches_geometrical_u2018_overlay_result" []: nothing -> nothing {
  let group = "u2018_overlay"
  let parallel_workers = 4
  pueue-check group-exists $group
  ^pueue parallel --group $group $parallel_workers
	log debug "About to submit the tile patches overlay result"

  let tiles = main filtered_s2_tiles
  for tile in $tiles {
		log debug $"Submitting for ($tile)"
    let psql_cmd = $"select tile_patches_geometrical_u2018_overlay\('($tile)'\)"
    ^pueue add --group $group psql --set ON_ERROR_STOP=on -c $' "($psql_cmd)"'
  }

	log debug "Waiting for computations to complete."
	log debug "This might take more than 15h to finish!"
  ^pueue wait --group $group
}

def "gdal postgres-connection-string" []: nothing -> string {
  $"PG:dbname=($env.PGDATABASE) host=($env.PGHOST) port=($env.PGPORT)"  
}

# Ingest the data from the parquet files
# Will skip over/ignore duplicate key errors
export def "main fill bigearthnet-patches-from-parquets" [
	parquets_dir: path # Path to the parquet files named: <bigearthnet_v1_spatial_<CRS>.parquet>
  --table-name: string = "bigearthnet_patches_alignment"
]: nothing -> nothing {
	let p = $parquets_dir | path expand --strict
  let ben_crses = [32629 32631 32632 32633 32634 32635 32636]
  $ben_crses | par-each {
    |crs|
    let name = $"bigearthnet_v1_spatial_($crs)"
    # FUTURE: Add code to ensure that these are available!
    # Main issue: Appending with ogr drops the source CRS, even if it is explicitely defined, at least with
    # the PostgreSQL target. I _could_ add an extra column that contains the CRS information and fix it afterwards,
    # but I don't really like that idea. I will _hack_ it together by writing these new tables, using those
    # as input for the common table and deleting them afterwards
    let fpath = $"($p)/($name).parquet"
		log debug $"About to upload ($fpath) to ($name)"

    ogr2ogr -overwrite -progress -nln $name $"(gdal postgres-connection-string)" $fpath
		| complete
		| complete check --cmd "ogr2ogr"

		log debug $"Finished uploading to temporary table ($name)"
		log debug $"Copy table from temporary table to target table ($table_name)"

    $"insert into ($table_name) \(patch_name, sensing_time, geom\) select patch_name, sensing_time, geom from ($name) on conflict do nothing"
		| ^psql --set ON_ERROR_STOP=on
		| complete
		| complete check --cmd "psql"

		log debug $"Finished copying table from temporary table to target ($table_name)"
		log debug $"Deleting temporary table ($name)"

    $"drop table ($name)"
		| ^psql --set ON_ERROR_STOP=on
		| complete
		| complete check --cmd "psql"
  }

  null
}

# Uploads the given geojson to the DB with the name `ne_admin_0_countries`
# Will overwrite if the table already exists
export def "main fill country" [
  geojson_path: path
] {
  log debug $"About to upload ($geojson_path)"
  ^ogr2ogr -overwrite -progress -nln ne_admin_0_countries $"(gdal postgres-connection-string)" $geojson_path
  | complete
  | complete check --cmd "ogr2ogr"
}

# Ingest the data from `U2018_CLC2018_V2020_20u1.gpkg` into the prepared database table
export def "main fill U2018_CLC2018_V2020_20u1" [u2018_gpkg_path: path]: nothing -> nothing {
  log debug "About to upload the geometries from U2018_CLC2018."
  log debug "This might take up to 10min and doesn't show any progress during that time!"
	$u2018_gpkg_path | path expand --strict
  # FUTURE: could use explodecollections to skip exploding during overlap calculation
  # FUTURE: Maybe add a spinner to show that something is happening?
  # Do NOT stop and check the error code; idea is to keep running if duplicate key was skipped
  # FUTURE: Make a stricter check to only skip over duplicate key errors!
  ^ogr2ogr -append -nln U2018_CLC2018_V2020_20u1 -nlt PROMOTE_TO_MULTI $"(gdal postgres-connection-string)" --config PG_USE_COPY YES $u2018_gpkg_path
  | complete

	log debug "Finished processing the CLC2018 geopackage"
}

# Function that calls the fill_tile_patches_validation_and_alignment_result(<TILE>) function in parallel
# This is requried as the database has trouble paralellizing over all tiles.
# By moving the call to a configurable & adjustable number of workers, the client has much more control
# how strongly these calculations affect the stress on the server.
# The flexibility to adjust the number of workers is only really worth it right here.
# Though this brings the big issue that FILE_PWD has to be correct.
export def "main fill all_tile_patches_u2018_overlay_rasterization_geoms" [
  --group: string = "fill_tile_patches_u2018_overlay_rasterization_geoms"
  --parallel-workers: int = 8
]: nothing -> nothing {
  pueue-check group-exists $group
  ^pueue parallel --group $group $parallel_workers
	log debug "About to submit patches overlay rasterization geoms result"
  # remember filtered here means those that contain aligned and valid data and not necessarily those that have an intersection with the u2018 geometries
  # could also be merged without extra function call, I feel.
  let tiles = main filtered_s2_tiles
  for $tile in $tiles {
		log debug $"Submitting for ($tile)"
    let cmd = $"use ($env.FILE_PWD)/ben-data-generator.nu *; '($tile)' | main fill tile_patches_u2018_overlay_rasterization_geoms"
    # Fixing syntax highlighting bug -> "
    ^pueue add --group $group $'nu --no-config-file --commands "($cmd)"'
  }
	log debug "Waiting for computations to complete."
  ^pueue wait --group $group
}

export def "main fill tile_patches_u2018_overlay_rasterization_geoms" []: string -> nothing {
  let tile = $in
  log debug $"About to create the raster polygons from ($tile)"

  $"select fill_tile_patches_u2018_overlay_rasterization_geoms\('($tile)'\);"
	| ^psql --set ON_ERROR_STOP=on
	| complete
	| complete check --cmd "psql"

  null
}

# Function that exports all rasterized patches that were overlayed
# from the clc2018 geometries.
# requires all overlay results to be available!
# This could be pipelined and the internal function could be called
# immediately after the overlay results for a given tile have been processed.
export def "main export reference-maps" [
  export_dir: string
  --parallel-workers: int = 6
]: nothing -> nothing {
	log debug "About to run reference-maps export jobs."
  let tiles = main filtered_s2_tiles
  $tiles | par-each --threads $parallel_workers {
    |tile|
		log debug $"Exporting reference-map for ($tile)"
    $tile | main export reference_maps_from_clc_overlayed_patches ($export_dir)
  }
	log debug "Finished waiting for reference_maps."
}

export def "main export reference_maps_from_clc_overlayed_patches" [
  export_dir: path
  # --overwrite: bool = false
]: string -> nothing {
  let tile = $in
  log debug $"About to export reference-map results from ($tile) to ($export_dir)"

  # let escaped_del = "'|'"
  let res = $"\\copy \(select * from exportable_rasterized_clc_overlayed_patches\('($tile)'\)\) to pstdout with delimiter '|';"
    | ^psql --quiet --set ON_ERROR_STOP=on
    | lines | group 10 | each { 
      |r|
      $r | parse '{filename}|{data}'
        | each {|x|
          let name_data = $x.filename
            | parse --regex '(?P<src_tile>.*)(?P<coords_suffix>_\d+_\d+)_reference_map.tif'
            | first
          let target_dir = $"($export_dir)/($name_data.src_tile)/($name_data.src_tile)($name_data.coords_suffix)"
          mkdir $target_dir
          $x.data | decode hex | save --force --raw $"($target_dir)/($x.filename)"
      }
    } | flatten
  if $res != [] {
    print $res
    error make -u {msg: "Error during export of reference maps"}
  }
  log info "Done exporting rasters!"
}

# This should print out the relevant information for me:
# with group=10 and cpress lzw + predictor 2 + num_threads=4 = 30s
# with group=20 and cpress lzw + predictor 2 + num_threads=4 = 30s
# with group=20 and cpress lzw + predictor 2 + num_threads=4 = 30s
# file /tmp/S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_00_B3.tif                                             (base)
# /tmp/S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_00_B3.tif: TIFF image data, little-endian, direntries=17, height=120, bps=16, compression=LZW, PhotometricIntepretation=BlackIsZero, width=120
# DEFLATE: .rw-r--r-- 15k kaiclasen 13 Nov 10:11  /tmp/S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_00_B3.tif
# LZW:     .rw-r--r-- 16k kaiclasen 13 Nov 10:09  /tmp/S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_00_B3.tif 
# NONE:    .rw-r--r-- 29k kaiclasen 13 Nov 10:13  /tmp/S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_00_B3.tif

export def "main export patches_from_tile" [
  export_dir: path
  # --overwrite: bool = false
]: string -> nothing {
  let tile = $in
  log debug $"About to export the valid and aligned patches from ($tile) to ($export_dir)"

  let escaped_del = "'|'"
  let res = ('\copy (select * from exportable_aligned_named_patches(' + $"'($tile)'" + ')) to pstdout with delimiter ' + $escaped_del) | psql --quiet --set ON_ERROR_STOP=on
    | lines | group 10 | each { 
      |r|
      $r | parse '{filename}|{data}'
        | each {|x|
          let name_data = $x.filename
            | parse --regex '(?P<src_tile>.*)(?P<coords_suffix>_\d+_\d+)(?P<band_suffix>_B..).tiff?'
            | first
          let target_dir = $"($export_dir)/($name_data.src_tile)/($name_data.src_tile)($name_data.coords_suffix)"
          mkdir $target_dir
          $x.data | decode hex | save --force --raw $"($target_dir)/($x.filename)"
      }
    } | flatten
  if $res != [] {
    print $res
    error make -u {msg: "Error during export patches"}
  }
  log debug "Done exporting patches!"
}

# Tiny sql wrapper around `ben19_patch_label_table` table to export labels
# Creates a file named `patch_id_label_mapping.csv`
export def "main export ben19_patch_label_csv" [export_dir: path] {
  let dir = ($export_dir | path expand --strict)
  let p = $"($dir)/patch_id_label_mapping.csv"
  log debug $"Exporting to label CSV to: ($p)"
  '\copy (select patch_identifier_to_text(patch_id, 2) as patch_id, lbl_19 as label from ben19_patch_label_table order by patch_id, lbl_19) to pstdout csv header'
  | psql --quiet --set ON_ERROR_STOP=on
  | complete
  | complete check --cmd "psql"
	| from csv
	| save --force $p
}

# Tiny SQL wrapper around `tile_border_split` to export split
# Creates a file called `patch_id_split_mapping`
# Remember: This assigns ALL possible patches to a split and not only the valid ones!
export def "main export tile_border_split" [export_dir: path] {
  let dir = ($export_dir | path expand --strict)
  let p = $"($dir)/patch_id_split_mapping.csv"
  log debug $"Exporting to split CSV: ($p)"
  '\copy (select patch_identifier_to_text(patch_id, 2) as patch_id, split from tile_border_split order by patch_id, split) to pstdout csv header'
  | psql --quiet --set ON_ERROR_STOP=on
  | complete
  | complete check --cmd "psql"
	| from csv
	| save --force $p
}

# Tiny SQL wrapper around `tile_patches_geometries_distance_to_closest_ben_country` to derive
# the closest BigEarthNet country of each patch
# Creates a file called `patch_id_country_mapping`
# Remember: This assigns ALL possible patches to country and not only the valid ones!
# Remember: The used countries are from the BigEarthNet publication and a low-resolution country border
# is used => There are quite a few patches that would strictly belong to a different country!
export def "main export patch_id_country_mapping" [export_dir: path] {
  let dir = ($export_dir | path expand --strict)
  let p = $"($dir)/patch_id_country_mapping.csv"
  log debug $"Exporting to country CSV: ($p)"
  '\copy (select patch_identifier_to_text(patch_id, 2) as patch_id, country from tile_patches_geometries_distance_to_closest_ben_country() order by patch_id, country) to pstdout csv header'
  | psql --quiet --set ON_ERROR_STOP=on
  | complete
  | complete check --cmd "psql"
	| from csv
	| save --force $p
}

# Tiny wrapper around `patches_to_old_bigearthnet_name` to export the mapping as csv
export def "main export patch_id_to_s2v1_mapping" [export_dir: path] {
  let dir = ($export_dir | path expand --strict)
  let p = $"($dir)/patch_id_s2v1_mapping.csv"
  log debug $"Exporting s2v1 mapping to: ($p)"
  '\copy (select patch_id, s2v1_name from patches_to_old_bigearthnet_name() order by patch_id, s2v1_name) to pstdout csv header'
  | psql --quiet --set ON_ERROR_STOP=on
  | complete
  | complete check --cmd "psql"
	| from csv
	| save --force $p
}


# Export the patches from the databases
# If not a list of `tile`s is provided then all
# tiles will be selected for exporting
# NOTE: The function produces quite a bit of data!
# Make sure to have around >100GB of free space! 
export def "main export patches" [
  export_dir: path
  --parallel-workers: int = 6
]: [list<string> -> nothing, nothing -> nothing] {
  let inp = $in
	log debug $"About to export patches to ($export_dir)"
  let relevant_tiles = if ($inp | is-empty) {
    main filtered_s2_tiles
  } else {
    $inp
  }

  $relevant_tiles | par-each --threads $parallel_workers {
    |t|
		log debug $"Exporting ($t)"
    $t | main export patches_from_tile ($export_dir)
  }
  log debug "Finished exporting patches!"
}

# Given path to Sentinel2-L2A directory, extract the metadata into a table
# Will raise an error if directory doesn't exist or if there is an issue
# reading the metadata
export def "sentinel2-l2a metadata" []: path -> table {
  cd $in
  "MTD_MSIL2A.xml"
	| path expand --strict
	| ^gdalinfo -json $in
	| complete
	| complete check --cmd "gdalinfo"
	| from json
}

# Path to the Sentinel-2 L2A folder
# Extracts the subdatasets on its own and uploads the data to a table
# of the form:
# - l2a_product_id
# - dataset_suffix 
#   - given SENTINEL2_L2A:<PATH>/MTD_MSIL2A.xml:10m:EPSG_32634 would return 10m:EPSG_32634
# - metadata
# - raster
# Will skip over TCI subdataset
export def "main insert sentinel2-l2a-raster" [
  table: string = "sentinel2_l2a_rasters"
]: path -> any {
  # important that sentinel2-l2a metadata expands the path, otherwise the subdatasets wouldn't
  # be absolute paths and the out-of-db usage would break!
  let metadata = $in | sentinel2-l2a metadata
  let l2a_product_id = ($metadata | get metadata."".product_uri)
  let subdatasets = $metadata | get metadata.SUBDATASETS | transpose key value | where key =~ _NAME | get value | filter {|n| ':TCI:' not-in $n}
  assert greater ($subdatasets | length) 0 "There should be more than 0 subdatasets!"
  log info $"About to process the following subdatasets: ($subdatasets | str join ' | ')"
  $subdatasets | each {
    |subdataset|
    let sub_metadata = ^gdalinfo -json $subdataset | complete | complete check --cmd "gdalinfo" | from json
    let sub_l2a_product_id = ($sub_metadata | get metadata."".product_uri)
    # trim the everyting until the dataset specific suffix that isn't shared across the different
    # subdatasets
    let subdataset_suffix = $subdataset | str replace --regex '.*MTD_MSIL2A.xml:' ''
    if $l2a_product_id != $sub_l2a_product_id {
      error make {
        msg: $"Sentinel-2 Product URI differs between main and subdataset! That should never happen! main: ($l2a_product_id), ($subdataset): ($sub_l2a_product_id)"
      }
    }
    let sub_metadata_json = ($sub_metadata | to json --raw)
    # hijacking the output of raster2pgsql and replacing the table structure with my own!
    let raster2pgsql_substring = 'INSERT INTO "mtd_msil2a" ("rast") VALUES ('
    # injecting the table name, the product_id, subdataset_suffix, and json_metadata!
    let raster2pgsql_replacement = $"INSERT INTO \"($table)\" VALUES \('($sub_l2a_product_id)', '($subdataset_suffix)', '($sub_metadata_json)', "
    # FUTURE: make out-of-db optional!
    ^raster2pgsql -a -R $subdataset | str replace $raster2pgsql_substring $raster2pgsql_replacement | psql --set ON_ERROR_STOP=on | complete | complete check --cmd "psql"
  }
}

# Given a path to a Sentinel2-L2A directory insert metadata
# into `table` where the columns are:
# - the Sentinel2-L2A product name (S2B_MSIL2A_20180422T093029_N9999_R136_T34TEQ_20230901T105139.SAFE)
# - the metadata as json/jsonb
export def "main insert sentinel2-l2a-metadata" [
  table: string = "sentinel2_l2a_metadata"
]: path -> nothing {
  let p = $in
  # for now just building up "complete" structures.
  # then I can still decide how to handle it individually
  # or if I should distribute it differently
  let metadata = $p | sentinel2-l2a metadata
  # yes, the path sadly contains an empty key...
  let l2a_product_id = ($metadata | get metadata."".product_uri)
  # raw is required for ingestion into postgresql!
  let metadata_json = ($metadata | to json --raw)
  # escaping the nested and escaped strings is absolutely horrible
  # when trying to use \copy or \COPY. Instead inject the json
  # data directly into a literal psql command and yes, this is not super efficient
  # but the json data is tiny and parallelizing the psql call instead of assembling the
  # individual rows and commiting it as a single transaction might fail harder and is probably
  # not that much faster
  psql --command $"INSERT INTO ($table) VALUES \('($l2a_product_id)', '($metadata_json)'\) on conflict do nothing" | complete | complete check --cmd "psql"

  null
}

# Given a list of Sentinel2-L2A directories,
# insert the data into a PostgreSQL DB
# See the attached scripts that generate the DB and create the table!
# The connection logic assumes that the PSQL environment variables are set.
# FUTURE: Allow to overwrite the table names
# Note: This function will try to insert the raster/metadata into the table
# but won't return an error if any of the subtasks fails!
# Usually, the error is caused if a unique-constraint is violated and
# could be checked and verified in the future
# Changed from a pueue task to a normal nushell parallel task as with
# out-of-db rasters the operation is very quick
export def "main fill sentinel2-l2a-tables" [
  --parallel-workers: int = 6
]: list<path> -> nothing {
  let inp = $in
  log info $"Starting to process given list of Sentinel2-L2A tiles"
  $inp | par-each --threads $parallel_workers {
    |x|
    # FUTURE: Add better error reporting
    log info $"Processing l2a tile ($x)"
    $x | main insert sentinel2-l2a-metadata
    # Using try to allow repeated execution of the same command
    # which would violate the unique-key constraints
    # FUTURE: Fix the smart raster2pgsql replacement to skip over conflicts
    try { $x | main insert sentinel2-l2a-raster }
  }

  null
}

# Single entrypoint to (re-)generate the metadata files
# These should be generated as part of "main"
export def "main generate-metadata-files" [
	export_metadata_dir: path # path where several metadata files will be stored
]: {
	mkdir $export_metadata_dir
	main export ben19_patch_label_csv $export_metadata_dir
  # name: export_old_to_new_s2_name_mapping
  # depends: validation_and_alignment_result
  main export patch_id_to_s2v1_mapping $export_metadata_dir
	main export tile_border_split $export_metadata_dir
  main export patch_id_country_mapping $export_metadata_dir
}

# Single entrypoint that will try to complete all individual SQL ingestion
# and processing steps.
# This is the point, where it would make a lot of sense to use `nextflow`
# to manage the intermediate steps, but this will have to do for now.
# Please note that the metadata files will be OVERWRITTEN in `v1_metadata_dir`
# as well as the other image data!
# Ensure that the `L2As_root_dir` is on the fastest storage device available.
# Otherwise the following function will take forever to complete (NFS is unuseable!)
# Due to the large amount of parameters all parameters need to be provided as flags
# to ensure better readability -> All flags have to be provided!
export def "main generate-all-data" [
	--L2As-root-dir: path # Directory that contains all the converted L2A directories
	--v1-metadata-dir: path # Path where the BEN-v1 metadata parquets are stored
	--clc2018-gpkg-path: path # Path to the CLC2018 geopackage file
  --country-geojson-path: path # Path to the country geojson file, should match ne_*_admin_0_countries
	--export-patch-dir: path # Target directory for patches (Requires several 100 GB!)
	--export-reference-maps-dir: path # Target directory for reference maps (Requires a few GB!)
	--export-metadata-dir: path # path where several metadata files will be stored
] {
  if ([
    $L2As_root_dir
    $v1_metadata_dir 
    $clc2018_gpkg_path
    $country_geojson_path
    $export_patch_dir
    $export_reference_maps_dir
    $export_metadata_dir
  ] | any {is-empty}) {
    error make {
      msg: "Please provide all flags!"
    }
  }

	let tile_dirs = ($L2As_root_dir | path expand --strict) | ls $in | where name =~ 'S.*L2A.*.SAFE$' | get name
	mkdir $export_patch_dir
	mkdir $export_reference_maps_dir

	$v1_metadata_dir | path expand --strict 
	$clc2018_gpkg_path | path expand --strict
	$country_geojson_path | path expand --strict

	# MUST run in foreground to ensure that execution only continues after this step has finished
	# name: s2-tables
	# depends: nothing
	$tile_dirs | main fill sentinel2-l2a-tables

	# name: s2-geoms
	# depends: s2-tables
  psql --set ON_ERROR_STOP=on -c 'select insert_tile_patches_geometrical_from_new_sentinel2_l2a_rasters(1200);'
	  | complete | complete check --cmd 'psql'

	# depends: s2-geoms
  psql --set ON_ERROR_STOP=on -c 'select insert_new_tile_patches_geometrical_overlaps(common_epsg => 3035);'
    | complete | complete check --cmd 'psql'

	# depends: s2-geoms
  psql --set ON_ERROR_STOP=on -c 'select insert_new_tile_border_split();'
    | complete | complete check --cmd 'psql'

	# name: invalid-regions
	# depends: s2-tables
  # this is one of the slowest functions with around 30min of execution time.
  # FUTURE: Think about also parallelizing this over the client-side! For each individual tile this should be possible!
  psql --set ON_ERROR_STOP=on -c 'select insert_new_sentinel2_invalid_regions();'
	  | complete | complete check --cmd 'psql'

	# name: v1-metadata
	# depends: nothing
	main fill bigearthnet-patches-from-parquets $v1_metadata_dir

	# name: validation_and_alignment_result
	# depends: v1-metadata & invalid-regions
  # requires the two above functions to finish: ...-from-parquets and ...-invalid_regions()
  # Took about 1min
  psql --set ON_ERROR_STOP=on -c 'select fill_tile_patches_validation_and_alignment_result();'
	  | complete | complete check --cmd 'psql'

	# name: clc2018
	# depends: nothing
	main fill U2018_CLC2018_V2020_20u1 $clc2018_gpkg_path 

  # name: ne_admin_0_countries
  # depends: nothing
  main fill country $country_geojson_path

	# name: tile_clc_overlay
	# depends: validation_and_alignment_result & clc2018
  # if validation_and_alignment_result is empty, this will be skipped!
  # this will PIN the server to its limits (ideally) and can be manually adjusted with `pueue parallel --group u2018_overlay`
  # this should also be the slowest operation and take the most amount of time.
  # Took around 15h (this is significantly slower than expected, there must be a mis-configuration!)
  # Now it took around 3:30h which is way more logic
  # but this run had to be repeated a few times. It is highly likely that
  # auto vacuum/analyze fixed the underlying issue
  main fill tile_patches_geometrical_u2018_overlay_result
	# name: export_patches
	# depends: overlay
	# Took about 2:30h
	[] | main export patches $export_patch_dir

	# name: rasterization_geoms
	# depends: tile_clc_overlay
	main fill all_tile_patches_u2018_overlay_rasterization_geoms

	# Took about 15min
	# name: export_ref_maps
	# depends: rasterization_geoms
	main export reference-maps $export_reference_maps_dir

	# FUTURE: Make the underlying function idempotent!
	'select update_ben19_patch_label_table();'
  | psql --quiet --set ON_ERROR_STOP=on
	| complete
	| complete check --cmd "psql"

  main generate-metadata-files $export_metadata_dir
}

# Main entry point. You probably want to call the
# "main generate-all-data" function to do everything in a single step!
export def main [] {}

