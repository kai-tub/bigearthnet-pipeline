# over 8min did not complete
def main [
	--bigearthnet_dir: path = '/mnt/storagecube/data/datasets/BigEarthNet-S2/raw/BigEarthNet-v1.0_raw/BigEarthNet-v1.0/'
	target_dir: path
]: {
	let src_dir_span = (metadata $bigearthnet_dir).span;
	mkdir $target_dir
	let src_dir_abs = $bigearthnet_dir | path expand
	if not ($src_dir_abs | path exists) {
		error make {
			msg: "Directory doesn't exist!"
			label: {
				start: $src_dir_span.start
				end: $src_dir_span.end
				text: "Please select existing directory!"
			}
		}
	}
	# knowing that these names are identical to the folder names!
	let c = open --raw artifacts/patches_with_cloud_and_shadow.csv | from csv --noheaders | get column1
	let s = open --raw artifacts/patches_with_seasonal_snow.csv | from csv --noheaders | get column1
	# they do not overlap but this is safer
	let comb = $s | append $c | uniq
	cd $target_dir
	# this is highly inefficient for slow storages! Simply link _directly_ and then check if the link exists!
	# fd . $abs_dir | lines | filter { |x| ($x | path basename) in $comb } | par-each {|x| ^ln --symbolic --force $x }
	$comb | par-each { |x| ln --symbolic --force $"($src_dir_abs)/($x)" }
}