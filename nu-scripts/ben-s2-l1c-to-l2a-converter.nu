use l1c_to_l2a *
use utils *
use std log

# Given a list of paths to the l1c directory or zip files,
# call `L1C-to-L2A` in parallel via pueue
def "L1Cs-to-L2As" [
  --export-dir: string = "."
  --group: string = "sentinel-l1c-to-l2a"
  --parallel-conversions: int # the number of parallel l1c-to-l2a conversion.
  # This conversion itself has parallel phases. By default it will use the number of cores / 2.
  --background # Flag that indicates whether or not to directly return and not to wait until the downloads are finished
]: list<string> -> nothing {
  let inp = $in
  if (($inp | length) == 0) {
    log warning "Empty list provided. Nothing to convert!"
    return
  }
  pueue-check group-exists $group
  let p = if ($parallel_conversions | is-empty) {
    (sys | get cpu | length) // 2
  } else {
    $parallel_conversions
  }
  ^pueue parallel --group $group $p

  $inp | each {
    |x|
    # how to handle this if it is embedded inside of the module?
    # should it forward the NU_LIB_DIRS?
    let cmd = $"use ($env.FILE_PWD)/l1c_to_l2a *; '($x)' | L1C-to-L2A --export-dir=($export_dir)"
    ^pueue add --group $group $'nu --no-config-file --commands "($cmd)"'
  }
  log info $"Wait for pueue ($group) to finish converting. This might take a long while!"
  # FUTURE: add option to background the task and not wait until completion!
  if not $background {
    ^pueue wait --group $group
  }
}

# Given a path to the tile download directory from
# `ben-s2-tile-downloader.nu` extract and convert all L1C tiles to L2A tiles
# and try to fix potential issues from the Copernicus Dataspace API,
# as they sometimes provide invalid
# tiles as they are skipping over empty folders which were part of an older
# tile specification and is required for `sen2cor` to work.
# Note that `sen2cor` needs to be accessible within the shell for this to work!
# IMPORTANT: This function is NOT idempotent! It will break if it was called once before!
# Check the `L1C-to-L2A` function for details
export def main [
  tile_directory: path 
  --export-dir: string = "."
]: nothing -> nothing {
  pueue-autostart
  ls $tile_directory
  | get name 
  | filter {|x| $x =~ '.*\.out'}
  | L1Cs-to-L2As --export-dir=$export_dir
}
