# In my testing:
# Sentinel-2.tar
# [3 6 12 15 18]:
# ╭───┬────────────┬────────────┬────────────────────╮
# │ # │  duration  │    size    │ compression_factor │
# ├───┼────────────┼────────────┼────────────────────┤
# │ 0 │ 97.31 sec  │ 65617.1 MB │               1.56 │
# │ 1 │ 134.30 sec │ 62744.3 MB │               1.63 │
# │ 2 │ 194.78 sec │ 62312.2 MB │               1.64 │
# │ 3 │ 352.71 sec │ 61240.6 MB │               1.67 │
# │ 4 │ 493.50 sec │ 61064.2 MB │               1.68 │
# │ 5 │ 930.64 sec │ 59351.8 MB │               1.73 │
# ╰───┴────────────┴────────────┴────────────────────╯
#
# --> In my opinion zstd -6 it is
# lz4 could also be a viable alternative
# and more dictionary tuning could be done for 
# better zstd results, but this is 'good enough'.
# Decompression on the server took only around 2min30sec
# 1min20 sec for S1

const COMMON_ZSTD_OPTS = [
      "--threads=0"
      "--keep"
]

export def "main benchmark" [inp_file: path] {
  let inp_dir = $inp_file | path expand | path dirname
  let inp_f = $inp_file | path basename
  cd $inp_dir
  let compress_levels = [3 6 9 12 15 18]
  let res = $compress_levels | each {
    |compress_level|
    let out_file = $"($inp_f).zstd($compress_level)"
    print $out_file
    let zstd_opts = $COMMON_ZSTD_OPTS ++ [
      $"-($compress_level)"
      $inp_f
      "-o"
      $"($out_file)"
    ]
    print $zstd_opts
    let duration = timeit { ^zstd ...$zstd_opts }
    let size = ls $out_file | get size.0
    let comp_factor = (ls $inp_file | get size.0) / $size
    rm $out_file
    { duration: ($duration | format duration sec) size: ($size | format filesize MB) compression_factor: $comp_factor }
  }
  print $res
  $res | save -f $"zstd_($inp_f).csv"
}

# Compress an individual file with the tuned zstd options
# or if pointed to a directory, will compress all files inside of the
# directory (non-recursively & skipping over directories!). 
# The output will overwrite existing files!
# If an output path is given, then the resulting `zst` file(s) will be
# written into that directory.
export def main [
  inp: path
  --output-path: path
] {
  let inp_arg = if (($inp | path type) == "file") {
    $inp
  } else {
    ls $inp | get name | save -f /tmp/zstd_files.txt
    "--filelist=/tmp/zstd_files.txt" 
  }
  # auto-derive output file name
  mut zstd_opts = $COMMON_ZSTD_OPTS ++ [
    "--force" # overwrite the output if it already exists
    "-6" # see benchmark results above
    $inp_arg
  ]
  if (not ($output_path | is-empty)) {    
    mkdir $output_path
    $zstd_opts = $zstd_opts ++ [$"--output-dir-flat=($output_path)"]
  }

  ^zstd ...$zstd_opts
}
