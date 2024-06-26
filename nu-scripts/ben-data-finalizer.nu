use std log
use std assert

# https://reproducible-builds.org/docs/archives/
const COMMON_TAR_OPTS = [
  "--anchored"
  "--owner=0"
  "--group=0"
  # I would prefer the following time but it triggers warnings on most tar systems
  # as it is a 'unlikely timestamp'...
  # "--mtime=1970-01-01 00:00:00" 
  "--mtime=2024-01-01 00:00:00" 
  "--numeric-owner"
  "--format=gnu"
]

# HERE:
# update the code to use duckdb for alignment to speed up the slow join operation

def "check columns" [columns: list, name: string] {
  let inp = $in
  assert not ($inp | is-empty) $"($name) cannot be empty!"
  assert equal ($inp | columns) $columns $"($name) has unexpected columns names"
}

export def "main generate-metadata-files" [
  target_dir: path
  patch_id_label_mapping_file: path
  patch_id_s2v1_mapping_file: path
  patch_id_split_mapping_file: path
  patch_id_country_mapping_file: path
  old_s1s2_mapping_file: path
  old_patches_with_cloud_and_shadow_file: path
  old_patches_with_seasonal_snow_file: path
] {
  assert ($patch_id_label_mapping_file | path exists)
  assert ($patch_id_s2v1_mapping_file | path exists)
  assert ($patch_id_split_mapping_file | path exists)
  assert ($patch_id_country_mapping_file | path exists)
  assert ($old_s1s2_mapping_file | path exists)
  assert ($old_patches_with_cloud_and_shadow_file | path exists)
  assert ($old_patches_with_seasonal_snow_file | path exists)

  let tmp_dir = mktemp --directory
  ^ln -sf $patch_id_label_mapping_file ($tmp_dir | path join patch_id_label_mapping.csv)
  ^ln -sf $patch_id_s2v1_mapping_file ($tmp_dir | path join patch_id_s2v1_mapping.csv)
  ^ln -sf $patch_id_split_mapping_file ($tmp_dir | path join patch_id_split_mapping.csv)
  ^ln -sf $patch_id_country_mapping_file ($tmp_dir | path join patch_id_country_mapping.csv)
  ^ln -sf $old_s1s2_mapping_file ($tmp_dir | path join old_s1s2_mapping.csv)
  ^ln -sf $old_patches_with_cloud_and_shadow_file ($tmp_dir | path join patches_with_cloud_and_shadow.csv)
  ^ln -sf $old_patches_with_seasonal_snow_file ($tmp_dir | path join patches_with_seasonal_snow.csv)
  # FUTURE: think about how a closure can be written to delete the tmp_dir even on an error
  # but this isn't super important, as I am only creating symbolic links either way
  with-env ['BEN_UNALIGNED_METADATA_DIR': $tmp_dir] {
    mkdir $target_dir
    log debug $"Generating metadata files and storing results to: ($target_dir)"
    cd $target_dir
    # at the time of writing this is ONLY valid when it is executed as a script and NOT
    # when it is loaded as a module!
    let status = do { ^duckdb -c $".read ($env.FILE_PWD)/ben-metadata-finalizer.sql" } | complete
    if ($status.exit_code != 0) {
      error make --unspanned  {
        msg: $"Execution failed:\n\tstdout:\n($status.stdout)\n\n\tstderr:\n($status.stderr)"
      }
    }
  }
  rm -r $tmp_dir
}

def "build-archive s1" [
  target_dir: path,
  s1_root_dir: path, 
  s1_names: table<s1_name: string>
] {
  log debug "About to process S1 data"
  cd $s1_root_dir

  let s1_hits = ^fd  --threads=32 --no-ignore 'S1[AB]_.*_V.\.tif$' --max-depth=2
    | lines | wrap fp
    | merge ($in.fp | path parse | get parent | path basename | wrap s1_name) 

  let found_s1_names = $s1_hits | get s1_name | uniq | wrap s1_name

  if ($s1_hits | length) == 0 {
    error make {
      msg: $"Could not find any matching S1 patches under: ($s1_root_dir). Did you provide the actual root of the S1 directory?"
    }
  } else if (($s1_hits | length | $in mod 2) != 0) {
    error make {
      msg: "Number of matched files isn't devisible by 2! This should never happen! Some data must be missing."
    }
  }
  log debug $"Found ($s1_hits | length) files from ($found_s1_names | length) patches."

  let s1_compress_files = $s1_hits
    | join $s1_names s1_name
    | get fp

  assert less or equal ($s1_compress_files | length) ($s1_hits | length)
  log debug $"After aligning with given input, will tar ($s1_compress_files | length) files"

  $s1_compress_files
  | sort
  | save -f /tmp/s1_tar.txt

  log debug "Creating S1 archive"

  let out_file = $target_dir | path join "Sentinel_1.tar"

  # I have no idea why the inverse bracket expansion [^/] only
  # works with \+ and NOT *
  # S1A_IW_GRDH_1SDV_20170613T165043_33UUP_74_73/
  let s1_tar_opts = $COMMON_TAR_OPTS ++ [
    -cf $out_file
    "--files-from=/tmp/s1_tar.txt"
    '--transform=s#\([^/]\+\)\(_[0-9A-Z]\+_[0-9]\+_[0-9]\+\)#Sentinel_1/\1/\1\2#'
  ]

  # transform will match everything from start until `/` that ends with a _XX_YY 
  # where xx_yy are numbers. this is important as it would otherwise also gobble up the `/` symbol
  # the escaping is required when working with tar...
  # so for the root of the S1 directory, it will select the patch directories and move
  # them into the 'tile' directory
  ^tar ...$s1_tar_opts
  log debug "Finished creating S1 archive"

  # Assert that the layout structure is identical to the expected layout structure!
  let path_components = ^tar --list --file $out_file
    | lines
    | first
    | path split

  # Sentinel_1/tile/<tile>_<patch>/<tile>_<patch>_V[VH].tif
  assert equal ($path_components | length) 4
  assert equal $path_components.0 "Sentinel_1"
  assert str contains $path_components.2 $path_components.1
  assert str contains $path_components.3 $path_components.2
  assert equal ($path_components.3 =~ '.*_V[VH]\.tif$') true
}

def "build-archive s2" [
  target_dir: path,
  s2_root_dir: path,
  patch_ids: table<patch_id: string>
] {
  log debug "Processing S2 data"
  cd $s2_root_dir
  # I need access the patch_id to filter out those that are unnecessary
  # stored inside of the reference-map directory
  let s2_hits = ^fd  --threads=32 --follow --no-ignore 'S2[AB]_MSIL2A_.*_B..\.tiff?$'
    | lines | wrap fp
    | merge ($in.fp | path parse | get parent | path basename | wrap patch_id) 
  let found_s2_patch_ids = $s2_hits | get patch_id | uniq | wrap patch_id

  if ($s2_hits | length) == 0 {
    error make {
      msg: $"Could not find any matching S2 patches under: ($s2_root_dir). Did you provide the actual root of the S2 directory?"
    }
  } else if (($s2_hits | length | $in mod 12) != 0) {
    log error "The number of found S2 files is not devisible by 12! This should not happen!"
  }
  log debug $"Found ($s2_hits | length) S2 files to process from ($found_s2_patch_ids | length) patches"

  let s2_compress_files = $s2_hits
    | join $patch_ids patch_id
    | get fp

  assert less or equal ($s2_compress_files | length) ($s2_hits | length)
  log debug $"After aligning with given input, will tar ($s2_compress_files | length) files"

  $s2_compress_files
  | sort
  | save -f /tmp/s2_tar.txt
  
  let out_file = ($target_dir | path join "Sentinel_2.tar")
  
  log debug "Creating S2 archive"
  let s2_tar_opts = $COMMON_TAR_OPTS ++ [
    -cf $out_file
    "--files-from=/tmp/s2_tar.txt"
    '--transform=s#\(.\+\)#Sentinel_2/\1#'
  ]
  ^tar ...$s2_tar_opts
  log debug "Finished creating S2 archive"

    # Assert that the layout structure is identical to the expected layout structure!
    let path_components = ^tar --list --file $out_file
      | lines
      | first
      | path split

    # Sentinel_2/tile/<tile>_<patch>/<tile>_<patch>_BXX.tif
    assert equal ($path_components | length) 4
    assert equal $path_components.0 "Sentinel_2"
    assert str contains $path_components.2 $path_components.1
    assert str contains $path_components.3 $path_components.2
    assert equal ($path_components.3 =~ '.*_B..\.tiff?$') true
}

def "build-archive reference-maps" [
  target_dir: path
  reference_maps_root_dir: path, 
  patch_ids: table<patch_id: string>
] {
  log debug "Processing reference-maps data"
  do {
    cd $reference_maps_root_dir
    let ref_maps_hits = ^fd  --threads=32 --follow --no-ignore 'S2[AB]_MSIL2A_.*_reference_map\.tiff?$'
      | lines | wrap fp | merge ($in.fp | path basename | parse --regex '(?P<patch_id>.*)_reference_map\.tiff?$')

    log debug $"Found ($ref_maps_hits | length) reference-map files."

    let ref_maps_compress_files = $ref_maps_hits
      | join $patch_ids patch_id
      | get fp
    assert less or equal ($ref_maps_compress_files | length) ($ref_maps_hits | length)
    log debug $"After aligning with given input, will tar ($ref_maps_compress_files | length) files"

    $ref_maps_compress_files
    | sort
    | save -f /tmp/ref_maps_tar.txt

    # decided to re-order the directory structure here.
    # it could've also been fixed in the export function from the `data_generator`
    # but I do believe that for the export function the previous version still makes sense
    # but I also understand that this "aligned" version is easier for an end-user, as they can
    # simply copy the resulting folder into the S2 folder and merge them in that way.
    let out_file = $target_dir | path join "Reference_Maps.tar"
    log debug "Creating reference-map archive"
    let ref_maps_tar_opts = $COMMON_TAR_OPTS ++ [
      -cf $out_file
      "--files-from=/tmp/ref_maps_tar.txt"
      # the following transform adds another directory level without the `_reference_map.tif` suffix
      # the first part is the tile component and the second part the name
      '--transform=s#\(.\+\)#Reference_Maps/\1#'
    ]
    ^tar ...$ref_maps_tar_opts
    log debug "Finished creating Reference_Maps archive"

    # Assert that the layout structure is identical to the expected layout structure!
    let path_components = ^tar --list --file $out_file
      | lines
      | first 
      | path split

    # Reference_Maps/tile/<tile>_<patch>/<tile>_<patch>_reference_map.tif
    assert equal ($path_components | length) 4
    assert equal $path_components.0 "Reference_Maps"
    assert str contains $path_components.2 $path_components.1
    assert equal $path_components.3 $"($path_components.2)_reference_map.tif"
  }
}

# Build the ALIGNED archives from the given directories
# The `patch_id_s1_mapping_file` is used as the _single-source of truth_
# for the alignment! The file will be used to select only 
# those patches that are listed inside of the file, either
# from the s1_name (for S1 files) or patch_id (for S2 and reference map file) column.
# The motivation is simple. The current PostgreSQL pipeline exports
# _all_ valid (non no-data pixels) and BEN-aligned patches as S2 patches, irrespective
# if label information is available (as this could be used for self-supervised training).
# The reference maps directory contains all patches that contain _any_ label information
# from the CLC2018 source. As not all BEN-aligned patches are covered by CLC2018
# geometries, the reference-maps directory contains fewer patches as the S2 directory.
# However, our current goal is to provide aligned sets that all contain the same patches.
# And in the "main build-metadata" step the `patch_id_s1_mapping_file` is aligned with the
# _label_ data. And the label data is derived by setting a custom threshold and applying other
# filtering steps to clean-up the data. As a result, the archive files need to be
# aligned to the `patch_id_s1_mapping_file`!
export def "main build-archives" [
  target_dir: path
  s1_root_dir: path
  s2_root_dir: path
  reference_maps_root_dir: path
  patch_id_s1_mapping_file: path
] {
  log debug $"Generating archives and storing results to: ($target_dir)"
  mkdir $target_dir
  assert ($s1_root_dir | path exists)
  assert ($s2_root_dir | path exists)
  assert ($reference_maps_root_dir | path exists)
  assert ($patch_id_s1_mapping_file | path exists)

  let patch_id_s1_mapping = open $patch_id_s1_mapping_file
  $patch_id_s1_mapping | check columns [patch_id s1_name] $patch_id_s1_mapping_file

  build-archive s1 $target_dir $s1_root_dir ($patch_id_s1_mapping | select s1_name)
  build-archive s2 $target_dir $s2_root_dir ($patch_id_s1_mapping | select patch_id)
  build-archive reference-maps $target_dir $reference_maps_root_dir ($patch_id_s1_mapping | select patch_id)
}


# some checks to ensure that the generated
# output looks correct
# 1. check that Sentinel_1, Sentinel_2, Reference_Maps.tar
#    all contain matching data and NOT more than necessary!
# tars have tile/tile-patch-id/patch-file data
# 2. report the directory structure in some way or another
#    that can be imported into the main directory

# Check if the Sentinel-1/2 tar structure is as expected!
def "check sentinel-tar" [tar_file: path expected_ids: list<string>] {
  # assert sentinel_files layout structure
  let sentinel_files = tar --list -f $tar_file | lines
  let structure = $sentinel_files | parse --regex '^(?<dir>[^/]+)/(?<tile>[^/]+)/(?<patch>[^/]+)/(?<file>.+.tiff?)$'
  # -1 because of the root directory
  assert equal ($sentinel_files | length) ($structure | length) "Difference while parseing expected tile/patch/patch_band.tif structure!"

  let incorrect_structures = $structure | filter {|r| not (($r.patch | str contains $r.tile) and ($r.file | str contains $r.patch)) }
  if (($incorrect_structures | length) != 0) {
    print $incorrect_structures
    error make {
      msg: "The naming linkage with <tile>/<tile><patch>/<tile><patch><band> is violated!"
    }
  }
  
  let mapping_alignments = ($structure | get patch | uniq | wrap patch)
  | join ($expected_ids | wrap patch) "patch"
  | get "patch"
  | uniq
  | length

  assert equal $mapping_alignments ($expected_ids | length) "Difference between the expected_ids and the files inside of the tar!"

  log debug $"Found no issues inside: ($tar_file)"
}

def "check reference-map-tar" [tar_file: path expected_ids: list<string>] {
  # assert sentinel_files layout structure
  let ref_maps_files = tar --list -f $tar_file | lines
  let structure = $ref_maps_files | parse --regex '^(?<dir>[^/]+)/(?<tile>[^/]+)/(?<patch>.+)_reference_map.tiff?$'
  assert equal ($ref_maps_files | length) ($structure | length) "Difference while parseing expected tile/patch_reference_map.tif structure!"

  let incorrect_structures = $structure | filter {|r| not (($r.patch | str contains $r.tile)) }
  if (($incorrect_structures | length) != 0) {
    print $incorrect_structures
    error make {
      msg: "The naming linkage with <tile>/<reference-maps-patch>.tif is violated!"
    }
  }
  
  let mapping_alignments = $structure
  | join ($expected_ids | wrap patch) "patch"
  | get "patch"
  | uniq
  | length

  assert equal $mapping_alignments ($expected_ids | length) "Difference between the expected_ids and the files inside of the tar!"

  log debug $"Found no issues inside: ($tar_file)"
}

# Check if the output of the main script has an _aligned_ output
# and that the tar files have the expected structure!
export def "main check-output" [dataset_directory: path] {
  cd $dataset_directory
  
  let patch_id_s1_mapping = open patch_id_s1_mapping.csv
  assert ("Sentinel_1.tar" | path exists)
  assert ("Sentinel_2.tar" | path exists)
  assert ("Reference_Maps.tar" | path exists)
  # by checking if the tar files ONLY contain the expected files from the
  # mapping file, we implicitely check that they contain ONLY the same patches!
  check sentinel-tar "Sentinel_1.tar" ($patch_id_s1_mapping | get s1_name)
  check sentinel-tar "Sentinel_2.tar" ($patch_id_s1_mapping | get patch_id)
  check reference-map-tar "Reference_Maps.tar" ($patch_id_s1_mapping | get patch_id)

  log debug "Everything is aligned!"
}

# Given the data produced by the `ben-data-generator`
# and the S1 data from BigEarthNet-v1.0, generate the
# new final & aligned data for BigEarthNet-v2.0.
# Note that the `s1-root-dir` needs to point to an _extracted_
# Sentinel-1 directory and not the compressed `tar` archive!
export def "main finalize" [
  --target-dir: path
  --s1-root-dir: path
  --s2-root-dir: path
  --reference-maps-root-dir: path
  --patch-id-label-mapping-file: path
  --patch-id-s2v1-mapping-file: path
  --patch-id-split-mapping-file: path
  --patch-id-country-mapping-file: path
  --old-s1s2-mapping-file: path  
  --old-patches-with-cloud-and-shadow-file: path
  --old-patches-with-seasonal-snow-file: path
]: {
  if ([
    $target_dir
    $s1_root_dir
    $s2_root_dir
    $reference_maps_root_dir
    $patch_id_label_mapping_file
    $patch_id_s2v1_mapping_file
    $patch_id_split_mapping_file
    $patch_id_country_mapping_file
    $old_s1s2_mapping_file
    $old_patches_with_cloud_and_shadow_file
    $old_patches_with_seasonal_snow_file
  ] | any {is-empty}) {
    error make {
      msg: "Please provide all flags!"
    }
  }
  main generate-metadata-files $target_dir $patch_id_label_mapping_file $patch_id_s2v1_mapping_file $patch_id_split_mapping_file $patch_id_country_mapping_file $old_s1s2_mapping_file $old_patches_with_cloud_and_shadow_file $old_patches_with_seasonal_snow_file
  main build-archives $target_dir $s1_root_dir $s2_root_dir $reference_maps_root_dir ($target_dir | path join "patch_id_s1_mapping.csv")
  main check-output $target_dir
}

# Main entrypoint. You probably want to call
# the complete function "main finalize"
export def main [] {}

def build-tree [] {
  let base_path = ($nu.temp-path | path join $"test_dirs_(random uuid)")
  log debug $"Temporary base path created at: ($base_path)"

  ### s2 logic
  let s2_base = $base_path | path join "S2"
  let s2_sample_patch_directory = $s2_base
    | path join "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER" "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_47"

  mkdir $s2_sample_patch_directory 

  let s2_sample_patch_files = [B01 B02 B03 B04 B05 B06 B07 B08 B8A B09 B11 B12]
    | each { |band| $"($band).tif" }
    | append [ "invalid.tif" "invalid.json" ]
    | each {
      |suffix|
      $s2_sample_patch_directory
        | path join ($s2_sample_patch_directory | path basename | $"($in)_($suffix)")
    }

  $s2_sample_patch_files | each {|f| touch $f}
  let patch_id = $s2_sample_patch_directory | path basename
  let patch_id_label_mapping = [[patch_id label]; [$patch_id "Arable land"] [$patch_id "Broad-leaved forest"]]
  let patch_id_label_mapping_file = $base_path | path join "patch_id_label_mapping.csv"
  $patch_id_label_mapping | save -f $patch_id_label_mapping_file
  # s2v1 mapping may contain patch_ids that are NOT inside of the `patch_id_label_mapping` file!
  let patch_id_s2v1_mapping = [[patch_id s2v1_name]; [$patch_id S2A_MSIL2A_20170613T101031_0_47] [to_be_filtered_id S2A_MSIL2A_20170613T101031_0_48]]
  let patch_id_s2v1_mapping_file = $base_path | path join "patch_id_s2v1_mapping.csv"
  $patch_id_s2v1_mapping | save -f $patch_id_s2v1_mapping_file
  # split mapping may contain patch_ids that are NOT inside of the `patch_id_label_mapping` file!
  let patch_id_split_mapping = [[patch_id split]; [$patch_id train] [to_be_filtered_id_too S2A_MSIL2A_20170613T101031_0_48]]
  let patch_id_split_mapping_file = $base_path | path join "patch_id_split_mapping.csv"
  $patch_id_split_mapping | save -f $patch_id_split_mapping_file
  # country mapping may contain patch_ids that are NOT inside of the `patch_id_label_mapping` file!
  let patch_id_country_mapping = [[patch_id country]; [$patch_id Austria] [to_be_filtered_id_in_country Austria]]
  let patch_id_country_mapping_file = $base_path | path join "patch_id_country_mapping.csv"
  $patch_id_country_mapping | save -f $patch_id_country_mapping_file

  ### reference-maps logic

  let ref_maps_base = $base_path | path join "ref_mapsmaps"
  let ref_maps_sample_tile_directory = $ref_maps_base
    | path join "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER"
  mkdir $ref_maps_sample_tile_directory 
  let ref_maps_sample_files = [
    "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_47_reference_map.tif"
    "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_11_47_reference_map.tif" # extra reference-map file that should be skipped
    "S2A_MSIL2A_20170613T101031_N9999_R022_T34VER_00_47.tif" # simulate accidental inclusion
  ] | each {|f| $ref_maps_sample_tile_directory | path join $f}

  mkdir $ref_maps_sample_tile_directory
  $ref_maps_sample_files | each {|f| touch $f}

  ### s1 logic

  let s1_base = $base_path | path join "S1"
  let s1_sample_patch_directory = $s1_base | 
    path join "S1A_IW_GRDH_1SDV_20170613T165043_33UUP_65_63"

  mkdir $s1_sample_patch_directory

  let s1_sample_patch_files = ["VV.tif" "VH.tif" "labels_metadata.json" "invalid.json"]
    | each {
      |suffix|
      $s1_sample_patch_directory
        | path join ($s1_sample_patch_directory | path basename | $"($in)_($suffix)")
    }
  $s1_sample_patch_files | each {|f| touch $f}
  # add one patch dir that isn't included in the mapping and check that this one
  # is NOT included! and yes, I cannot be bothered to externalize this as a clean function
  let s1_sample_patch_directory2 = $s1_base | 
    path join "S1A_IW_GRDH_1SDV_20170613T165043_33UUP_65_62"

  mkdir $s1_sample_patch_directory2

  let s1_sample_patch_files2 = ["VV.tif" "VH.tif" "labels_metadata.json" "invalid.json"]
    | each {
      |suffix|
      $s1_sample_patch_directory2
        | path join ($s1_sample_patch_directory2 | path basename | $"($in)_($suffix)")
    }
  $s1_sample_patch_files2 | each {|f| touch $f} 
  ###

  ### fake `s1s2_mapping.csv`
  let fake_s1s2_mapping_file = $base_path | path join "s1s2_mapping.csv"
  let s2v1_name = $patch_id_s2v1_mapping | get s2v1_name.0
  let s1_name = $s1_sample_patch_directory | path basename
  # create a raw csv file because the original one doesn't contain any header information
  # and yes, these two patche names aren't from the correct mapping, but that is not relevant here
  # The second _missing_ S1 patch name must be correct as this will be used to extract the base directory
  $"($s2v1_name),($s1_name)\nmissing_s2,S1A_IW_GRDH_1SDV_20170613T165043_33UUP_65_64" | save --raw $fake_s1s2_mapping_file

  {
    base_path: $base_path
    s1_base_path: $s1_base
    s2_base_path: $s2_base
    ref_maps_base_path: $ref_maps_base
    patch_id_label_mapping_file: $patch_id_label_mapping_file
    patch_id_s2v1_mapping_file: $patch_id_s2v1_mapping_file
    patch_id_split_mapping_file:$patch_id_split_mapping_file
    patch_id_country_mapping_file:$patch_id_country_mapping_file
    old_s1s2_mapping_file: $fake_s1s2_mapping_file
    # FUTURE: Fix this bugga
  }
}

export def "main test" [] {
  let inp = build-tree
  let target_dir = $inp.base_path | path join "target_dir"
  main finalize --target-dir $target_dir --s1-root-dir $inp.s1_base_path --s2-root-dir $inp.s2_base_path --reference-maps-root-dir $inp.ref_maps_base_path --patch-id-label-mapping-file $inp.patch_id_label_mapping_file --patch-id-s2v1-mapping-file $inp.patch_id_s2v1_mapping_file --patch-id-split-mapping-file $inp.patch_id_split_mapping_file --patch-id-country-mapping-file $inp.patch_id_country_mapping_file --old-s1s2-mapping-file $inp.old_s1s2_mapping_file

  assert equal (open ($target_dir | path join 'patch_id_country_mapping.csv') | length) 1
  let s1_files = ^tar --list -f ($target_dir | path join "Sentinel_1.tar") | lines
  assert equal ($s1_files | length) 2 "Only one valid patch directory with two valid tifs is used in the test"
  # log debug ($s1_files | table)
  $s1_files | each {
    |fp|
    # patch_name 
    let patch_name = ($fp | path parse | get parent) 
    # parent should also be the root!
    let tile_name = ($patch_name | path parse | get parent)
    assert not ($tile_name | is-empty)
    assert ($patch_name | str contains $tile_name)
  }

  let s2_files = ^tar --list -f ($target_dir | path join "Sentinel_2.tar") | lines
  assert equal ($s2_files | length) 12 "Only one valid patch directory with 12 valid tifs is used in the test"
  # log debug ($s2_files | table)

  let ref_maps_files = ^tar --list -f ($target_dir | path join "Reference_Maps.tar") | lines
  assert equal ($ref_maps_files | length) 1 "Only one valid Reference-Map was used in the test"
  rm -r $inp.base_path
}

