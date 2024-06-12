use std assert
use std log

### Extract this discussion to a separat file!
# TODO: How am I sure that the hour format is the one with the missing zero? Maybe everyone date entry with a
# leading zero is missing
# A quick investigation with:
# open data/S2_metadata.csv | get s2_name | par-each {|r| ($r | s2 parse patch_name | get hour.0 | str length) == 1 } | length
# returns 590_326, without any errors, which means that the regular expression is able to parse the patch name
# where only the hour slightly changes
# no, this is still wrong!
# If hour is 10 and minute 5 and second 12 then the same regex would still parse it correctly
# but then the hours are even more out of place!
# => Decided to drop the hour/minute/second regex and instead call it `time`
# Comparing the parsed time with the _correct_ tile names, indicates that this is truly only the case for the hour field
# 
# After parsing the patch name according to 
# https://api-depositonce.tu-berlin.de/server/api/core/bitstreams/d4b228a4-01db-4b92-afdc-8850c10d4ffb/content
# though it fixes a bug where the actual patch data does not include a leading 0 for the hour spec
# This is an example patch that violates the own spec:
# S2B_MSIL2A_20180204T94160_20_72
# according to JSON document, the value links to
# `S2B_MSIL1C_20180204T94160_N0206_R036_T35VNJ_20180204T115809.SAFE`
# but this tile doesn't exist.
# The correct tile source (tile_names_and_links.csv) is 
# `S2B_MSIL1C_20180204T094159_N0206_R036_T35VNJ_20180204T115809.SAFE`
# which seems to contain the leading 0 AND has a differing SECONDS value 60 vs 59.
# A second value of 60 is illegal and is probably caused by the 'second-fraction' of the
# original XML file, see the attributes value under the copernicus API
# https://apihub.copernicus.eu/apihub/odata/v1/Products('8ea5c739-1df7-4df7-b65a-a107090c7648')/Attributes
# Sensing start = 2018-02-04T09:41:59.027Z
# Ironically, not all patches are missing the leading zero for the hour format, the patch called:
# S2A_MSIL2A_20171208T093351_45_20 does have a leading 0 in the hour and tile format.

#### Start of generic tools to work with the old BigEarthNet v1 datasets/json files

export def "s2 find metadata-files" [--limit: int] {
  let glob_pattern = 'S2*_labels_metadata.json'
  # undocumented but if max-results is 0 it means the same as no limit for fd
  let max_results = if $limit == null { 0 } else { $limit }
  fd --absolute-path --glob --max-results $max_results $glob_pattern | lines
}

export def "s2 parse patch_name" []: string -> table {
  $in | parse --regex '(?x)
    (?P<sentinel_mission>S2[AB])
    _
    MSI
    (?P<product_level>L\d.) # Sentinel-2 data product; should be 2A for all BEN patches
    _
    (?P<year>\d{4})
    (?P<month>\d{2})
    (?P<day>\d{2})
    T
    (?P<time>\d{5,6}) 
    # After further analysis I can _ensure_ that only the hour field may have a missing
    # leading 0. But this is NOT wrong for all patches that have a single digit hour entry
    # to highlight this bug, I will leave the regex as is! A special function + documentation
    # should highlight what happened.
    #(?P<hour>\d{1,2}) # Bug
    #(?P<minute>\d{2})
    #(?P<second>\d{2})
    _
    (?P<horizontal_id>\d{1,2})
    _
    (?P<vertical_id>\d{1,2})
  '
}

#[test]
def test_s2_parse_patch_name [] {
  # actual name from BEN-v1.0 with name that violates own spec
  let broken_patch_name = 'S2B_MSIL2A_20180204T94160_20_72'
  let out = $broken_patch_name | s2 parse patch_name | get 0
  assert equal $out.sentinel_mission 'S2B'
  assert equal $out.product_level 'L2A'
  assert equal $out.year '2018'
  assert equal $out.month '02'
  assert equal $out.day '04'
  assert equal $out.time '94160'
  assert equal $out.horizontal_id '20'
  assert equal $out.vertical_id '72'
}

# TODO: Think about what minimal should look like!
# maybe projection should be included in minimal?
export def "s2 parse json" [p: path, minimal: bool = true] {
	let s2_name = ($p | path basename | parse --regex '(?<s2_name>S2.*)_labels_metadata.json' | get 0)
	open $p
	| if $minimal { $in | select "tile_source" "acquisition_date" } else { $in }
	| merge $s2_name
}

export def "s2 get metadata" [--limit: int, minimal: bool = true] {
	s2 find metadata-files --limit $limit
	| par-each --threads 4 {|fp| s2 parse json $fp $minimal}
}

# FUTURE: Compare these results to fd via hyprfine!
# fd is A LOT faster for large directories!
# but it does have an overhead for smaller directories
# For the entire BEN dataset, fd takes only about 20s on a cold cache.
#  Time (mean ± σ):      9.455 s ± 10.749 s    [User: 20.377 s, System: 31.466 s]
#  Range (min … max):    5.277 s … 39.852 s    10 runs
# and with pure: it takes too long on cold cache...
export def "s1 find metadata-files" [--limit: int] {
  let glob_pattern = 'S1*_labels_metadata.json'
  # undocumented but if max-results is 0 it means the same as no limit for fd
  let max_results = if $limit == null { 0 } else { $limit }
  fd --absolute-path --glob --max-results $max_results $glob_pattern | lines
}

# manually ran psql:
# create table s1_metadata ("acquisition_time" text, "scene_source" text, "corresponding_s2_patch" text, "s1_name" text);
# \copy "s1_metadata" from '~/out.csv' delimiter ',' CSV HEADER;
export def "s1 parse json" [p: path, minimal: bool = true] {
	let s1_name = ($p | path basename | parse --regex '(?<s1_name>S1.*)_labels_metadata.json' | get 0)
	open $p
	| if $minimal { $in | select "acquisition_time" "scene_source" "corresponding_s2_patch" } else { $in }
	| merge $s1_name
}

export def "s1 get metadata" [--limit: int, minimal: bool = true] {
	s1 find metadata-files --limit $limit
	| par-each {|fp| s1 parse json $fp $minimal}
}

# Quickly merge all S1 metadata files into a single CSV file
# that only contains the columns: `corresponding_s2_patch` `scene_source` `acquisition_time`
export def "s1 generate single-minimal-metadata-file" [] {
  s1 get metadata true | save --force S1_minimal_metadata.csv
}

# Given a `root-directory` that contains BigEarthNet folders
# in the BigEarthNet folder-structure, it will merge the
# RGB-bands (B04,B03,B02) into a single `X_rgb.tif` GeoTIFF file
# for quick and easy visualization in QGIS or similar.
def "ben-tools merge-tiffs-to-rgb" [
  target_directory: path # will contain a "flat-list" of all _rgb.tif files; Will be created if not existing.
  --root_directory: path = ./
] {
  let target_directory = ($target_directory | path expand --strict)
  mkdir $target_directory
  cd $root_directory
  let dirs = (glob --no-file '*_MSIL2A_*')
  let gdal_merge_options = [
    "-separate",
    "-ot",
    "UInt16",
    "-of",
    "GTiff",
    "-co",
    "COMPRESS=DEFLATE",
    "-co",
    "PREDICTOR=2",
    "-co",
    "ZLEVEL=9",
  ]
  # get temporary out-name that includes _rgb.tif
  let tmp = (mktemp -d)
  let merge_results = (
    $dirs | par-each { |dir| 
      let base_dir = ($dir | path basename)
      let res = (
        do {
          ^gdal_merge.py -o $"($tmp)/($base_dir)_rgb.tif" $gdal_merge_options $"($dir)/($base_dir)_B04.tif" $"($dir)/($base_dir)_B03.tif" $"($dir)/($base_dir)_B02.tif"
        } | complete
      )
      if $res.exit_code != 0 {
        $res.stderr
      }
    }
  )

  if not ($merge_results | is-empty) {
    print "Error occured during execution!"
    print "Will remove all temporarily created files!"
    print $merge_results
    rm --recursive $tmp
    return
  }

  mv ($tmp + "/*") $target_directory
  $target_directory
}

# XML == pain
# as each XML node is represented as a `tag` `attribute` `content` triplet,
# where each `content` field contains a list of possible children nodes, it is very painful
# to access/transform the data into a more 'natural' nushell data structure.
# The code to access the relevant nodes was fairly complex and hard to reason about
# + it would've broken with the slightest change to the XML structure.
# To work with XML data, I utilize dedicated XML tools that utilize Xquery.
# Even if I dislike this particular DSL and find the learning resources quite bad,
# it is A LOT easier to understand (after some googling) and shouldn't be as sensitive as the previous version
# The xquery searches for the `properties` field (ignoring the namespace) and returns the data in a (pseudo)csv format
# BUT with a special separator=` which is used to make sure that the multi-line values (that exist!) are not changed.
# Each field starts with the name of the properties XML node (for example: `Id`, `Category`)
# followed by a colon `:` and the value of that field surrounded by the separator=`
# The resulting (pseudo)-csv string has _no_ header!
# Each row indicates the contents of a single properties node!

# Convert an S2 xml file to a nushell table
# The xml file can be loaded from apihub.copernicus.eu Attributes:
# https://apihub.copernicus.eu/apihub/odata/v1/Products('fb6918c2-a737-4332-a1f2-754e10e49fd5')/Attributes
# Note: Uses `xidel` under the hood and _requires_ the data to be in an XML file
export def "s2 open xml" [p: path] {
  (xidel --xquery
    'for $entry in //*[local-name() = "properties"] return string-join(for $prop in $entry/* return concat("`", local-name($prop), ":", $prop, "`"), "," )'
    $p)
  # This is read by nushell and transformed into the correct table format.
  # this is my "intermediate" pseudo-csv format
  | from csv --quote '`' --noheaders
  # now 
  | each { 
    |row|
    $row
    | columns
    # build a single table row (record)
    # by extracting the encoded data:
    # <col_name>:<col_value>
    # tranforming it to a record and merge them
    # for all encoded columsn of the row
    | reduce --fold {} {
      |it, acc|
      $acc
      | merge (
        $row
        | get $it
        | parse --regex "(?s)(.*?):(.*)"
        | transpose --as-record --ignore-titles --header-row
      )
    }
  }
}

