# A tiny wrapper around flyway for postgresql
# that utilizes the postgres environment variables to connect
# and that uses the flake's options by default or if `--local`
# is provided the migrations folder from the CWD
export def main [
  command: string # flyway subway command that will be executed
	--migration-directory: path # Use local `migrations` directory instead of flake
] {
  # using the gnuutils version now!
  let user = whoami
  let migration_dir = $"filesystem:($migration_directory)" 
	# FUTURE: maybe link directly to $PGHOST?
  flyway $command $'-url=jdbc:postgresql://localhost:($env.PGPORT)/($env.PGDATABASE)' $'-user=($user)' $'-locations=($migration_dir)'
}
