use std log

def "L1C-fix-dataspace-bug" [
  top_folder: string
  potentially_missing_subfolder: string
]: path -> nothing {
  let inp = $in
  print $inp
  let top_folder_files = ls ([ $inp $top_folder ] | path join) | get name
  if ($top_folder_files | length) != 1 {
    error make -u {
      msg: $"($top_folder) folder has multiple sub-folders, which isn't allowed! ($top_folder_files | str join ',')"
    }
  }
  let subdir = $top_folder_files | first;

  if $potentially_missing_subfolder not-in (ls $subdir | get name | path basename) {
    # The folder may be empty for older tiles which only produced the QI_DATA if there was an issue."
    # not sure why AUX is also empty
    log warning ([
      $"The L1C tile is missing the ($potentially_missing_subfolder) folder inside of the ($top_folder) folder."
      "This is a known bug from Copernicus Data Space that skips over empty directories during the download."
      "Resulting in a non-compliant L1C tile."
      $"This code will create an empty ($potentially_missing_subfolder) to fix the issue but this is NOT an official fix and might cause issues with sen2cor."
      "(Although this is unlikely)"
    ] | str join "\n")
    mkdir ([$subdir $potentially_missing_subfolder] | path join)
  }
}

# Sometimes the Copernicus Dataspace API provides invalid
# tiles as they are skipping over empty folders which were part of an older
# tile specification and is required for `sen2cor` to work.
# This function tries to re-add those empty folders.
# They are only added if the explicitely caused issues in the past, so there might
# be many more empty folders that are required for future tiles to work
export def "L1C-fix-dataspace-bugs" []: path -> nothing {
  let l1c_path = $in
  $l1c_path | L1C-fix-dataspace-bug "DATASTRIP" "QI_DATA"
  $l1c_path | L1C-fix-dataspace-bug "GRANULE" "AUX_DATA"
}

# Works on an Sentinel-L1C directory or zip file.
# The command will create a temporary work directory
# to extract the zip file to and try to fix potential issues
# from the Copernicus Dataspace API, as they sometimes provide invalid
# tiles as they are skipping over empty folders which were part of an older
# tile specification and is required for `sen2cor` to work.
# Note that `sen2cor` needs to be accessible within the shell for this to work!
# The L2A directory will be placed next to the L1C input.
# NOTE: sen2cor is NOT idempotent! It assumes that the target directory does NOT exist!
# As the patched sen2cor program is forcing a consistent processing time-stamp
# re-executing the program inside of the same directory will lead to the following (misleading) error:
# `AttributeError: 'bool' object has no attribute 'SPACECRAFT_NAME'`
# FUTURE: I could write the data to another temporary directory and recursively update the data
export def "L1C-to-L2A" [
  --export-dir: string = "." # Directory to export the patches to. Defaults to the current working directory
]: path -> nothing {
  let inp = $in | path expand --strict
  let exp_dir = $export_dir | path expand --strict
  let directory = ^mktemp --directory | complete | get stdout | str trim
  let l1c_path = if (^file --brief --mime-type $inp) == "application/zip" {
    log info "Zip detected, unpacking it first!"
    if (^unzip -d $directory $inp | complete | get exit_code) != 0 {
      rm -r $directory
      error make -u {
        msg: "Issue while trying to unzip file!"
      }
    }
    if (ls $directory | length) != 1 {
      error make -u {
        msg: $'The uncompressed zip file either contained multiple folders, or temp dir was not empty.
        The temporary directory will not be deleted to allow introspectiong: ($directory)'
      }
    }
    ls $directory | get name.0
  } else {
    if ($inp | path type) == file {
      rm -r $directory
      error make {
        msg: "Input is not a directory and not a zip file!"
      }
    }
    $inp
  }
  # ^L2A_Process --resolution $resolution $l1c_path
  log info "Fixing up l1c tiles if necessary:"
  $l1c_path | L1C-fix-dataspace-bugs

  log info $"About to process L1C tile: ($l1c_path)"
  # do not use complete here because I don't want to capture
  # stdout... I think
  let test_call = do { ^L2A_Process --help } | complete
  if ($test_call | get exit_code) != 0 {
    rm -r $directory
    error make -u {
      msg: $"Error during execution of L2A_Process: ($test_call.stderr)!"
    }
  }
  ^L2A_Process --output_dir ($exp_dir) $l1c_path
  if $env.LAST_EXIT_CODE != 0 {
    rm -r $directory
    # print $status
    error make -u {
      msg: "Error during execution of L2A_Process!"
    }
  }
  log info $"Finished to process L1C tile: ($l1c_path)"
  rm -r $directory
}

