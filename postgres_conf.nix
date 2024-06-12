# see devenv postgres.settings for more information!
# this is the server configuration!
{
  # Recommended value: about 75% of database memory
  # other value I've read is about 40% of the system RAM -> Note that this is a 'hard-allocation' and is initialized
  # when the database starts! So I shouldn't be too overly aggressive
  # I selected 6GB on my 16GB laptop and could've increased it a bit more
  # but I wanted to work on other things while waiting, so I set it this low
  shared_buffers = "750GB";
  # Recommended value: any amount of “free” memory expected to be around under ordinary operating conditions
  # Same here; could've increased this by 2GB for my 16GB laptop
  effective_cache_size = "800GB";
  # Because there will only be a single connection, this may help to speed up calculation
  maintenance_work_mem = "2GB";
  checkpoint_completion_target = 0.9;
  wal_buffers = "16MB";
  default_statistics_target = 500;
  # work_mem = "1024MB";
  # work_mem = "8196MB";
  work_mem = "1GB";
  huge_pages = "try";
  min_wal_size = "4GB";
  max_wal_size = "16GB";

  #max_worker_processes is the total maximum number of background services the system can support
  #is the upper limit for many other values
  # https://www.postgresql.org/docs/15/runtime-config-resource.html#GUC-MAX-WORKER-PROCESSES
  # from https://youtu.be/i_91jNrRYWk
  # they used approx ~5x the number of available workers;
  # Because we are only using a _single_ connection, we must be aware that Postgres
  # will be very conservative with reaching the max numbers for a single connection
  # for my 6-core 12 threads laptop I selected 60 worker_processes and 56 parallel_workers
  # also set max_parallel_workers_per_gather to 56 to maximize utilitzation and it went
  # quite well, it spawned around 11-12 workers. This always seems to be the maximum
  # number of workers I can get... In the online tutorial they mentioned that this is
  # set logarithmitically, so maybe this needs to be set extremly high on a server...
  # With this configuration it took about 6h on my laptop
  max_worker_processes = 2048;
  max_parallel_workers = 1024;
  # limited by max_parallel_workers; each worker gets their own work_mem
  max_parallel_workers_per_gather = 1024;
  max_parallel_maintenance_workers = 64;
  # set to 2 if reading from HDD!
  random_page_cost = "1.1";
  #
  max_connections = 30;
  logging_collector = "true";
  session_preload_libraries = "auto_explain";
  "auto_explain.log_min_duration" = "3s";
  "auto_explain.log_analyze" = "true";
  "auto_explain.log_buffers" = "true";
  "auto_explain.log_timing" = "true";
  "auto_explain.log_verbose" = "true";
  "auto_explain.log_triggers" = "true";
  "auto_explain.log_settings" = "true";
  "auto_explain.log_wal" = "true";
  "auto_explain.log_nested_statements" = "true";
}
