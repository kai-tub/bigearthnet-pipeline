use utils *
use copernicus_dataspace *
use std log

# Given a list of IDs download them in parallel in the background
# The downloads will be scheduled and run in parallel through `pueue`
def "copernicus ids download" [
  --parallel-downloads: int = 2 # number of parallel downloads, for normal users 4 is max!
  --group: string = "copernicus-download"
  --background # Flag that indicates whether or not to directly return and not to wait until the downloads are finished
  --force # Force re-download of all files
]: list<string> -> list<string> {
  let inp = $in
  pueue-check group-exists $group
  ^pueue parallel --group $group $parallel_downloads
  # check manually at the beginning to catch potential 'unverified' input
  copernicus credentials token-safe-read-with-refresh --force-refresh
  $inp | each {
    |x|
    # https://github.com/nushell/nushell/issues/9776
    # FUTURE: Fix the requirment of this file to live next to the evaluating script
    # A nix solution would be to replace this path with the path to the nix store
    # of this script!
    let cmd = $"use ($env.FILE_PWD)/copernicus_dataspace *; '($x)' | copernicus id download --force=($force)"
    ^pueue add --group $group $'nu --no-config-file --commands "($cmd)"'
  }
  log info $"Wait for pueue ($group) to finish downloading. This might take a long while!"
  # FUTURE: add option to background the task and not wait until completion!
  if not $background {
    ^pueue wait --group $group
  }
}

# Given a list of IDs download them in the foreground for easier debugging
def "copernicus ids download-direct" [
	--force # Force re-download of all tiles
]: list<string> -> nothing {
	let inp = $in
	for x in $inp {
		print $x
		try {
			$x | copernicus id download --force=($force)
		}
	}
}

# Tiny script that utilizes the copernicus_dataspace module
# to download the BigEarthNet Sentinel-2 tiles.
# The script requires a path to a CSV file that contains an `ID`
# column. This copernicus dataspace ID is used to download the entire tile.
# If no path is given via the command-line, the environment variable $TILE_IDS_CSV
# is used.
# The script will download all of the files into the given directory (current directory by default)
# but skip over those that were already downloaded previously.
export def main [
	--csv-path: path, # Path to CSV file with `id` column. Default $TILE_IDS_CSV
	--force-redownload # Redownload files even if they already exist.
  --parallel-downloads: int = 1 # number of parallel downloads, for normal users 4 is max but the servers break frequently and 1 is the only stable solution...
	--output-dir: path = "." # Directory to download files into. Must exist!
	--debug # if running in debug mode, will only use a single thread to download in the foreground without using pueue!
] {
	let env_path = $env.TILE_IDS_CSV?
	let p = if ($csv_path | is-empty) {
		print $"No csv path provided, will read from TILE_IDS_CSV ($env_path)"
		if ($env_path | is-empty) or (not ($env_path | path exists)) {
			error make -u {
				msg: "No csv path is given via CLI nor via TILE_IDS_CSV environment variable!"
			}
		}
		$env_path | path expand
	} else {
		$csv_path | path expand
	}
	let ids = open $p --raw | from csv | get id
	cd $output_dir
	copernicus credentials generate-tokens
	if $debug {
		$ids | copernicus ids download-direct --force=($force_redownload)
	} else {
		pueue-autostart
		$ids | copernicus ids download --parallel-downloads=($parallel_downloads) --force=($force_redownload)
	}
}
