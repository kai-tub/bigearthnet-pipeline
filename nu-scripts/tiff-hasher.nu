use std log

# Tiny script that recursively searches through a directory to find all tif/tiff files
# and calculates their hashes in parallel.
# The result is sorted and stored to `export_file_path`.
# The file itself is then again hashed and stored next to it with the `.sha256` extension
# Currently, the script assumes that the file basename is unique and will raise an error
# if this isn't the case, as this is used as the unique `name` and will be sorted by it!
def main [
	tiff_root_directory: path # Root directory that contains the tiffs files (may be nested)
	export_file_path: path # Target file path, suffix should be either csv or parquet!
] {
	let p = ($tiff_root_directory | path expand --strict)
	^fd --type f --no-ignore --full-path '^.*/.*\.tiff?$' $p
		| lines 
		| par-each {
			|fp| {
				name: ($fp | path parse | get stem)
				hash-sha256: (open --raw $fp | hash sha256)
			}
		} 
		| sort-by name
		| save --force $export_file_path

	let data = (open $export_file_path)

	if (($data | length) != ($data | get name | uniq | length)) {
		let err_path = ($export_file_path + ".err")
		log error "The file names are not unique!"
		log error $"Inspect the result at ($err_path)"
		mv $export_file_path $err_path
		return 1
	}

	log info $"Exported to ($export_file_path) and calculated (open $export_file_path | length) hashes."		
	let full_hash = (open --raw $export_file_path | hash sha256)
	let hash_path = $export_file_path + ".sha256"
	$full_hash | save --force $hash_path
	log info $"Calculated the sha256 hash of the resulting file and stored it under ($hash_path)"
}
