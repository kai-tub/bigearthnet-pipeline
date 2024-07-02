# Takes the patch_id/name as input
def rgb_files [] {
  [
    $"($in)_B04.tif"
    $"($in)_B03.tif"
    $"($in)_B02.tif"
  ]
}

def "main single" [
  --root-dir: path 
  --out-dir: path = "/tmp"
  --patch-id: string 
] {
  do {
    cd $root_dir
    let tile = $patch_id | parse --regex '(.*)_.._..' | get capture0.0
    let p = $tile | path join $patch_id
    cd $p
    # alternative contrast-stretch 1%x1%
    let out_p = $"($out_dir)/($patch_id).tif"
    print $"Writing to ($out_p)"
    # ^convert ...($patch_id | rgb_files)  -combine -normalize -scale 480x480 $out_p
    ^gdal_merge.py -separate ...($patch_id | rgb_files) -o $out_p
    $out_p
  }
}

# render multiple patches from a file that
# has a unique patch_id on each individual line
def "main multiple" [
  --root-dir: path
  --out-dir: path = "/tmp"
  --file-path: path
] {
  open --raw $file_path | lines | par-each {
    |line|
    let l = $line | parse --regex '(S2._MSIL2A_[A-Z0-9_]+)' | get capture0.0? 
    if ($l | is-empty) {
      print $"Couldn't find patch_id in ($line); skipping"
      return null
    }
    main single --root-dir $root_dir --out-dir $out_dir --patch-id ($l | str trim)
  }
}

# Tiny hacky script that quickly renders the RGB bands from a given S2 patch_id
# as a jpeg with a sensible normalization and upscaling for presentational purposes.
def main [] {}
