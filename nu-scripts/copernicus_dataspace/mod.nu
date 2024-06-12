# Collection of tools to work with the copernicus dataspace API
# Most functions assume to have the COPERNICUS_TOKEN_PATH and
# COPERNICUS_REFRESH_TOKEN_PATH set.
# The first call should be to:
# `copernicus credentials generate-tokens` to generate the tokens
# It may be required to re-generate the tokens after a few hours as the refresh_token
# isn't valid for too long.
# The current code does NOT store the username/password and thus requires the manual login
# at the start!
# This means that before long running tasks are started, the user should check the status
# of the tokens by calling `copernicus credentials check` first!
use std log
use utils *

export-env {
  $env.COPERNICUS_CACHE_PATH = (
    $env.HOME | path join ".cache/copernicus-dataspace"
  )
  mkdir $env.COPERNICUS_CACHE_PATH

  # Just my manual configuration to return a special write-out configuration for my curl commands
  $env.COPERNICUS_CURL_W_CONFIG = ($env.COPERNICUS_CACHE_PATH | path join "curl_write_out.conf")
  $'\n# extra curl write-out output\n# in TOML style \nexit_code = %{exitcode}\nhttp_code = %{http_code}\nfile_path = "%{filename_effective}"'
  | save -f $env.COPERNICUS_CURL_W_CONFIG

  $env.COPERNICUS_TOKEN_PATH = (
    $env.COPERNICUS_TOKEN_PATH?
    | path default_or_expand ($env.COPERNICUS_CACHE_PATH | path join "copernicus.token")
  )
  $env.COPERNICUS_REFRESH_TOKEN_PATH = (
    $env.COPERNICUS_REFRESH_TOKEN_PATH?
    | path default_or_expand ($env.COPERNICUS_CACHE_PATH | path join "copernicus.refresh.token")
  )
}

# Read the token and raise error if not available
def "copernicus credentials token" []: nothing -> string {
  # lock this read!
  # this read should only be done if there is no .lock file!
  if ($env.COPERNICUS_TOKEN_PATH | path exists) {
    open --raw $env.COPERNICUS_TOKEN_PATH
  } else {
    error make -u {
      msg: $"($env.COPERNICUS_TOKEN_PATH) does not exist! Run `copernicus credentials generate-tokens!`"
    }
  }
}

# Read the refresh token if available and raise error if not available
def "copernicus credentials refresh-token" [
]: nothing -> string {
  if ($env.COPERNICUS_REFRESH_TOKEN_PATH | path exists) {
    open --raw $env.COPERNICUS_REFRESH_TOKEN_PATH
  } else {
    error make -u {
      msg: $"($env.COPERNICUS_REFRESH_TOKEN_PATH) does not exist! Run `copernicus credentials generate-tokens!`"
    }
  }
}

# Set the copernicus credentials for future use.
# Provide them directly in the CLI via arguments, or
# if data is missing, input them when the function asks for them.
# The function will store the resulting token, as well as a
# refresh token that will be used to update the token without
# user interaction if possible.
export def "copernicus credentials generate-tokens" [
  --user: string = "", # user name
  --password: string = "", # password
] {
  let user = if ($user | is-empty) {
    input "Please provide your copernicus user-name:\n"
  } else {
    $user
  }
  let password = if ($password | is-empty) {
    input "Please provide your copernicus password (input won't be shown on screen!):\n" --suppress-output
  } else {
    $password
  }

  # generate token function?
  let res = [
    $"data username=($user)"
    $"data password=($password)"
    "data grant_type=password"
    "data client_id=cdse-public"
    "location"
    "request POST"
  ]
  | str join "\n"
  | ^curl -K - --fail-with-body "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
  | complete

  if $res.exit_code != 0 {
    print $res.exit_code
    error make -u {
      msg: $"Credentials are invalid! Try setting them again.\nFull message:\n ($res.stdout)"
    }
  }

  let auth_data = $res.stdout | from json

  if ($auth_data.access_token? | is-empty) or ($auth_data.refresh_token? | is-empty) {
    error make -u {
      msg: $"Data returned from authentity provider is incomplete: ($auth_data)"
    }
  }
  $auth_data.access_token | save --force --raw $env.COPERNICUS_TOKEN_PATH
  $auth_data.refresh_token | save --force --raw $env.COPERNICUS_REFRESH_TOKEN_PATH
  if not (copernicus credentials check) {
    error make -u {
      msg: $"The token request was succesful but there was an issue with using the token: ($res)"
    }
  }
  log info "Credentials stored."
}

# Check copernicus token and returns a boolean.
# The token is read from the environment variable!
export def "copernicus credentials check" []: nothing -> bool {
  # if nothing is provided, set "random" invalid data to surpress
  # curl's warning
  # I think it could just error out as well and provide a hint
  # to first set function.
  let token = (copernicus credentials token)

  # magic url from their example page; just used to check if authentication works
  # Unfortunately, the server doesn't allow HEAD request, so I cannot "probe" a file 
  # The only solution I could come up with was to access an "invalid" path
  # that is behind an "authorization" path, so I am relying on the fact
  # that server first verifies if the user is correctly authenticated and
  # then returns an Forbidden error.
  let res = $'header "Authorization: Bearer ($token)"'
    | ^curl -K - --location --fail-with-body --silent "https://zipper.dataspace.copernicus.eu/odata/v1/Products(060882f4-0a34-5f14-8e25-6876e4470b0d)/Online/$value"
    | complete

  # may return "Expired signature!" if the token isn't used fast enough
  # expect path to be "Forbidden" and not "Unauthorized" or similar
  if ($res.stdout | from json | get detail) !~ "Forbidden" {
    log info "Copernicus authentication failed!"
    return false
  }

  log info "Copernicus authentication successful!"
  return true
}

# Refresh the token with the given refresh token or with the token given
# in the file.
# At the time of writing, the token expires after 10min
# and the refresh token after 60min
# NOTE: This thread unsafe! 
# use the parallel safe `copernicus credentials token-safe-read-with-refresh --force-refresh`
# to safely refresh the tokens instead!
# It may happen that if multiple clients try to refresh the data that
# the token might be overwritten
# Or worse all tokens might be invalidated.
# https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/#Refresh-Token-Automatic-Reuse-Detection
export def "copernicus credentials refresh" []: nothing -> string {
  let refresh_token = (copernicus credentials refresh-token)
  
  let res = [
    $"data refresh_token=($refresh_token)"
    "data grant_type=refresh_token"
    "data client_id=cdse-public"
    "location"
    "request POST"
  ]
  | str join "\n"
  | ^curl -K - --fail-with-body "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
  | complete

  if $res.exit_code != 0 {
    print $res.exit_code
    error make -u {
      msg: $"Failed to refresh the token! Try updating the token manually!.\nFull message:\n ($res.stdout)"
    }
  }
  let auth_data = $res.stdout | from json

  $auth_data.access_token | save --force --raw $env.COPERNICUS_TOKEN_PATH
  $auth_data.refresh_token | save --force --raw $env.COPERNICUS_REFRESH_TOKEN_PATH
  log info "Tokens refreshed."

  return $auth_data.access_token
}

# A thread-safe function that will acquire a lock to check if the credentials
# are still valid and tries to refresh them if possible with the refresh_token.
# Then it will read the token, remove the lock and return the current token
export def "copernicus credentials token-safe-read-with-refresh" [--force-refresh]: nothing -> string {
  let lfile = $env.COPERNICUS_CACHE_PATH | path join ".lock"
  ^lockfile -1 -r 10 $lfile | complete | complete check --msg "Failed to acquire lock!"

  try {
    if $force_refresh {
      log info "Force-refreshing token!"
      copernicus credentials refresh
      log info "Token refreshed!"
    } else if not (copernicus credentials check) {
      log info "Token expired! Trying to refresh token."
      copernicus credentials refresh
      log info "Token refreshed!"
    }
  }
  rm -f $lfile
  # let token = copernicus credentials token
  # $token
  copernicus credentials token
}

# A tiny wrapper around `curl` that embeds the `Authorization` header
# from the local copernicus credentials
# It will refresh the token if necessary in a thread-safe manner!
# This function can be executed in parallel
# NOTE: --write-out does NOT overwrite the downloaded object given by -o !
export def "copernicus curl" [
  --url: string = "",
  ...config_options: string,
] {
  # FUTURE: Add retry wrapper around it and manually inspect if the output looks correct
  # in my latest tests, the copernicus servers are quite unstable and frequently break the
  # with various erros, such as 500 - Internal server error, unexpectatily closing the stream
  # etc.
  let token = copernicus credentials token-safe-read-with-refresh

  log info $"Trying to curl: url= ($url)"

  let config = ([$'header "Authorization: Bearer ($token)"'] ++ $config_options)
    | str join "\n"

  # A curl wrapper for interacting with the copernicus api that
  # tries to set the best default options for working with the Copernicus API:
  #   --progress-bar (will only be visualized if stderr is not consumed)
  #   --retry 3 (the old api was quite unstable, keeping it just in case)
  #   --fail-with-body (ensure that curl propagates issues)
  #   --fail-early (make sure that curl stops when the first error is encountered)
  #   --location-trusted (required as the service has changed end-points multiple times already!)
  #      As tokens are used either way to authenticate, the security issue is limited.
  # Additional options can be defined by providing additional parameters
  # where each will be processed as an individual **configuration** line
  # The tool tries to download the file with the given token (or from the env-file)
  # and tries to refresh the token if necessary.

  $config
  | ^curl --retry 5 --location-trusted --progress-bar --fail-with-body --fail-early --config - $url
}

# Return the `odata/v1` base url given a product id as an input
#  'https://catalogue.dataspace.copernicus.eu/odata/v1/Products(060882f4-0a34-5f14-8e25-6876e4470b0d)/$value
export def "copernicus id to-download-url" [id: string] {
  # $"https://catalogue.dataspace.copernicus.eu/odata/v1/Products\(($id)\)/$value"
  $"https://zipper.dataspace.copernicus.eu/odata/v1/Products\(($id)\)/$value"
}

#[test]
def test_copernicus_id_to_download_url [] {
  use std assert
  assert equal (copernicus id to-download-url "ab-cd-ef") "https://zipper.dataspace.copernicus.eu/odata/v1/Products(ab-cd-ef)/$value"
}

# Queries the `dataspace.copernicus` server for the given name
# Returns a table of results.
# The table contains the internal `Id` and the `Name` of the queried tile
export def "copernicus filter name" []: string -> table {
  let inp = $in
  log info $"About to query dataspace.copernicus server for ($inp)"
  let query_result = ^curl --get --silent --fail-with-body --data-urlencode $"$filter=Name eq '($inp)'" https://catalogue.dataspace.copernicus.eu/odata/v1/Products
    | complete
    | complete check --msg $"Issue while trying to query copernicus server and filter by name:\n($inp)"
  $query_result | from json | get value
}

# Given the name of the product (like S2 tile name)
# get the _single_ matching product ID
# If none or multiple matches are found, `null` is returned
export def "copernicus name-to-id" []: string -> any {
  let inp = $in
  let matches = $inp | copernicus filter name 
  return (
      match ($matches | length) {
      0 => {
        info log $"($inp) did not return any match."
        null
      },
      1 => {
        ($matches | get Id.0)
      },
      _ => {
        info log $"($inp) matched multiple IDs! Make your query more explicit."
        null
      }
    }
  )
}

# Given a copernicus dataspace ID download the file
# Will update the tokens if necessary (and possible with refresh-token)
# and is parallel-safe
export def "copernicus id download" [--force]: string -> path {
  let id = $in
  let fname = $"($id).out"
  let errname = $"($fname).err"

  if ($fname | path exists) and (not $force) {
    log info $"Seems like ($fname) was already successfully downloaded\nSkipping... Use `--force` to overwrite."
    return $fname
  }
  
  let url = (copernicus id to-download-url $id)
  log info $"Trying to download: ($id)"

  # https://everything.curl.dev/usingcurl/verbose/writeout

  # Service doesn't seem to support "--continue-at -" ...
  # Soo... No way to 'skip' files if identical
  let download_metadata = copernicus curl --url $url $"write-out=@($env.COPERNICUS_CURL_W_CONFIG)\noutput=($id).out"
    | from toml

  if $download_metadata.exit_code != 0 {
    print $download_metadata
    mv -f $fname $errname
    error make -u {
      msg: $"Issue while downloading file! See the output in ($errname). Token may be expired, potentially re-authenticate."
    }
  }
  log info $"Successfully downloaded ($id)"
  return $download_metadata.file_path
}

