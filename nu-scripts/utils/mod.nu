# Check if input is not-null and return the expanded path
# and with it, checking if the path exists, or returns the
# `default` argument.
export def "path default_or_expand" [default: path] {
  let option = $in
  if ($option | is-empty) {
    $default | path expand
  } else {
    $option | path expand
  }
}

# Given the output of the `complete` command, return the
# `msg` if `complete` had a non-zero exit code or `cmd` if msg isn't given.
# Returns stdout on success
export def "complete check" [--msg: string, --cmd: string]: record -> string {
  let res = $in
  let err_msg = if not ($msg | is-empty) {
    $msg
  } else if not ($cmd | is-empty) {
    $"Error while calling ($cmd)"
  } else {
    "Error calling command!"
  }
  if $res.exit_code != 0 {
    print $res
    error make -u {
      msg: $err_msg
    }
  }
  $res.stdout
}

# check if pueue is running and start
# the daemon manually if it isn't
export def "pueue-autostart" [] {
	try {
		pueue-check is-up
	} catch {
		print "Starting pueue daemon from within script!"
		^pueued --daemonize
    sleep 2sec # give the daemon enough time to start up!
	}
	pueue-check is-up
}

# Quickly check if pueue is running
export def "pueue-check is-up" [] {
  ^pueue status | complete | complete check --msg "Seems like 'pueue' is not running! Start it with 'pueued --daemonize'"
}

# Check if pueue group exists and create the group if it doesn't exist
export def "pueue-check group-exists" [group: string] {
  pueue-check is-up
  if $group not-in (^pueue group --json | from json | columns) {
    ^pueue group add $group | complete | complete check --msg $"pueue: creating group ($group) failed"
  }
}


