# Tiny script that utilizes the copernicus_dataspace module
# to query the dataspace API and get the data ids for the
# original BigEarthNet-S2 L1C tiles.
# As these tiles are derived from the `tile_names_and_links.csv`
# file, the script also provides some functions to work with this specific
# csv file.
# NOTE: This script uses the copernicus api but for such queries no tokens
# are required. Meaning that this script does not need any special manual
# pre-processing.
use std assert
use copernicus_dataspace *

# Load `tile_names_and_links.csv` file with the columns
# `source`, `tile_source`, and `url`.
export def "open tile_names_and_links" [
	p: path # Path to the tile_names_and_links.csv file
] {
  open --raw  $p
  | from csv --noheaders
  | rename source tile_source url
}

# Get the unique tiles with their associated url
# Returns `tile_source` and `url`
export def "tile_names_and_links unique-tiles-and-urls" [
	p: path # Path to the tile_names_and_links.csv file
] {
  open tile_names_and_links $p
  | select tile_source url
  | uniq
}

# Extract the old IDs from the old scihub copernicus API
# Was only used for debugging
export def "tile_names_and_links extract ids" [
	p: path # Path to the tile_names_and_links.csv file
] {
  tile_names_and_links unique-tiles-and-urls $p
  | get url
  | parse --regex `.*?Products\('(?<id>.*?)'\).*`
}

# Load the tiles from the input file `tile_names_and_links.csv`
# and map them to the newest product ids from the copernicus dataspace api
# The result of the query is sorted by the tile name and the output is
# stored as a csv file named `bigearthnet_s2_tile_ids.csv`
# with the columns `tile` and `id` in the current directory
export def "main" [
	tile_name_and_links_path: path # Path to the tile_names_and_links.csv file
] {
  let data = tile_names_and_links unique-tiles-and-urls $tile_name_and_links_path
    | get tile_source
    | each {|tile| {tile: $tile, id: ($tile | copernicus name-to-id)}}
    | sort-by tile

  # Ensure that we only have unique columns!
  for d in ($data | columns) {
    assert equal ($data | length) ($data | select $d | uniq | length)
    assert equal ($data | length) ($data | select $d | uniq | length)
  }
  
  print "Storing final result in bigearthnet_s2_tile_ids.csv"
  $data | save --force bigearthnet_s2_tile_ids.csv
}

