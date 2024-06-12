use std log
use std assert

# A small script that searches for the previously downloaded L1C tiles
# from the previous script (so it assumes that the file name is <ID>.out)
# and checks if they are valid zip files and then computes the sha256 hashes,
# storing the resulting CSV file in the current working directory.
def main [
	source_directory # The directory that will be opened to find the previously downloaded tiles
] {
	log info "Checking zip status of downloaded files"
	cd $source_directory

	let checked_zip_status = ls *.out | get name | par-each {
		|n|
		let v = do { ^zip -T $n } | complete
		if ($v.exit_code != 0) {
			log warning $"The downloaded file ($n) should be a valid zip!"
		}

		{file: ($n | path expand) valid_zip: $v.exit_code}
	}

	let status_path = "/tmp/ben_s2_l1c_zip_status.csv"
	log debug $"Storing final result as csv under: ($status_path)"
	log debug ($checked_zip_status | table)
	$checked_zip_status | save --force $status_path

	log info "About to hash valid zip files..."

	let hashes = $checked_zip_status | where {|r| $r.valid_zip == 0} | par-each {
			|r|
			{name: ($r.file | path basename) hash-sha256: (open $r.file --raw | hash sha256)}
	} | sort-by name

	log info $"Hashed ($hashes | length) valid zip files."

	assert equal ($hashes | length) ($hashes | get hash-sha256 | uniq | length) "All files hashes should be unique! Report this message!"

	log info "About to store hash result"

	$hashes | save --force ben_s2_l1c_hashes.csv
}

