{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    devenv = {
      url = "github:cachix/devenv/v1.0.1";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    systems.url = "github:nix-systems/x86_64-linux";
    sen2cor = {
      url = "github:kai-tub/nix-sen2cor";
    };
    nix-filter.url = "github:numtide/nix-filter";
    zenodo-upload = {
      url = "github:jhpoelen/zenodo-upload";
      flake = false;
    };
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = {
    self,
    nixpkgs,
    devenv,
    sen2cor,
    systems,
    nix-filter,
    ...
  } @ inputs: let
    eachSystem = nixpkgs.lib.genAttrs (import systems);
    filter = nix-filter.lib;
    pkgsFor = eachSystem (system:
      (nixpkgs.legacyPackages.${system}.extend (final: prev: {
        sen2corPackage = sen2cor.packages.${system}.sen2cor;
      }))
      .extend (final: prev: {nu = prev.nushellFull;}));
    # use non-standard port to avoid conflicts with potentially other local postgres instances!
    pg_port = 6543;
    pg_host = "127.0.0.1";
    pg_database = "bigearthnet";
    postgresFor = eachSystem (system: pkgsFor.${system}.postgresql_16);
    postgresWithExtensionsPkgFor =
      eachSystem
      (system: postgresFor.${system}.withPackages (p: [p.postgis p.pgtap]));
  in {
    formatter = eachSystem (system: pkgsFor.${system}.alejandra);
    checks = eachSystem (
      system:
        self.packages.${system}
    );
    devShells = eachSystem (system: let
      pkgs = pkgsFor.${system};
      postgresPkg = postgresWithExtensionsPkgFor.${system};
      flake-packages = self.packages.${system};
      inherit (pkgs) lib;
      bigearthnet-s2-tile-ids = flake-packages.bigearthnet-s2-tile-ids;
      stage1-packages =
        (with pkgs; [
          nu
          procmail # for `lockfile`
          pueue
        ])
        ++ [bigearthnet-s2-tile-ids];
    in {
      # if this is transfered into a runner, it might cause issues that
      # I am not explicitely adding pueue and lockfile in the nushell scripts.
      tile-downloader-env = pkgs.mkShell {
        name = "tile-downloader-env";
        buildInputs = stage1-packages;
        shellHook = ''
          export TILE_IDS_CSV="${bigearthnet-s2-tile-ids}/bigearthnet_s2_tile_ids.csv"
          echo "Set the environment variable TILE_IDS_CSV to $TILE_IDS_CSV"
          echo "Execute the 'nu downloader.nu'"
        '';
      };
      default = devenv.lib.mkShell {
        inherit pkgs inputs;
        modules = [
          ({
            pkgs,
            config,
            ...
          }: {
            # https://devenv.sh/reference/options/
            # ensure that the data is stored in the "state" folder!
            # This makes it easy to "relocate/link" the data to a different folder
            env.DEVENV_STATE =
              lib.mkForce "${config.env.DEVENV_ROOT}/state/";

            process.implementation = "process-compose";
            processes = {
              setup-db = {
                exec = "${pkgs.lib.getExe flake-packages.setup-db}";
                process-compose = {
                  availability.restart = "exit_on_failure";
                  depends_on = {
                    # process name `postgres` from services.postgres.nix
                    postgres.condition = "process_healthy";
                  };
                };
              };
              flyway-runner = {
                exec = "${pkgs.lib.getExe flake-packages.flyway-runner} migrate --migration-directory ${./migrations}";
                process-compose = {
                  availability.restart = "exit_on_failure";
                  depends_on = {
                    setup-db.condition = "process_completed_successfully";
                  };
                };
              };
              # pgbouncer didn't significantly increase the performance
              # of uploading the polygons. I will keep it as is for now without pgbouncer
              # this would have to be initalized with an init script
              # that creates the file and replace the output with the current
              # user...
              # hardcoding it for now
              # pgbouncer = {
              #   exec = let
              #     conf = pkgs.writeText "pgbouncer.ini" ''
              #       [pgbouncer]
              #       listen_port = 6789
              #       pool_mode = session
              #       listen_addr = *
              #       # could be adjusted depending on CPU cores?
              #       min_pool_size = 10
              #       default_pool_size = 40
              #       ignore_startup_parameters = options,extra_float_digits
              #       # just making it simple for now
              #       auth_type = any

              #       [databases]
              #       bigearthnet = host=${config.env.PGDATA} dbname=${pg_database} user=kai
              #     '';
              #   in "${pkgs.lib.getExe pkgs.pgbouncer} ${conf}";
              #   process-compose = {
              #     depends_on = {
              #       setup-db.condition = "process_completed_successfully";
              #     };
              #   };
              # };
            };

            services.postgres = {
              enable = true;
              package = postgresPkg;
              # Not using, initialDatabases as I want to manage it "manually"
              # same for initialScript

              port = pg_port;
              listen_addresses = "${pg_host}";
              settings = import ./postgres_conf.nix;
            };

            env.PGDATABASE = pg_database;
            env.CLC2018_PATH = "${flake-packages.U2018_CLC2018_V2020_20u1}";
            env.BEN_V1_METADATA_DIR = "${config.env.DEVENV_ROOT}/tracked-artifacts/ben-v1-metadata";
            env.BEN_V1_S1S2_MAPPING = "${flake-packages.bigearthnet-v1-s1s2-mapping}";
            env.BEN_COUNTRY_GEOJSON_PATH = "${flake-packages.country-boundaries-geojson}";
            env.NU_LOG_LEVEL = "DEBUG";
            env.RUST_LOG = "INFO";
            # all of my development dependencies as well for the
            # remote server...
            packages =
              stage1-packages
              ++ (with pkgs; [
                unzip
                zip
                proj
                gdal
                sen2corPackage
                fd
                ripgrep
                perl536Packages.TAPParserSourceHandlerpgTAP
                watchexec
                gitui
                git
                # helix
                imagemagick_light
              ])
              ++ (with flake-packages; [
                nuke
                setup-db
                flyway-runner
              ]);

            # set it dynamically to the correct path here
            # or... I could also set it in the code to ensure that the
            # specific version is used.
            # env.BEN_TILE_NAMES_AND_LINKS_PATH = "${flake-packages.bigearthnet-tile-names-and-links}/tile_names_and_links.csv";

            # https://postgis.net/docs/manual-2.1/RT_FAQ.html
            # https://postgis.net/docs/manual-2.1/postgis_installation.html#install_short_version
            env.POSTGIS_ENABLE_OUTDB_RASTERS = 1;
            env.POSTGIS_GDAL_ENABLED_DRIVERS = "ENABLE_ALL";

            enterShell = ''
                  echo '
              0. Update the values of `postgres_conf.nix` according to given hardware (see comments in file).
              1. Run `devenv up --keep-tui` to start the postgres server.
              '
            '';
          })
        ];
      };
    });
    packages = eachSystem (system: let
      pkgs = pkgsFor.${system};
      postgres = postgresFor.${system};
      flake-packages = self.packages.${system};
      bigearthnet-s2-tile-ids = flake-packages.bigearthnet-s2-tile-ids;
      stage1-packages =
        (with pkgs; [
          nu
          procmail # for `lockfile`
          pueue
        ])
        ++ [bigearthnet-s2-tile-ids];
    in {
      devenv-up = self.devShells.${system}.default.config.procfileScript;
      nuke = pkgs.writeShellApplication {
        name = "nuke";
        runtimeInputs = [postgres];
        text = ''
          psql --dbname=template1 --command "DROP DATABASE ${pg_database}"
        '';
      };
      flyway-runner = let
        p = pkgs.stdenvNoCC.mkDerivation {
          name = "flyway-runner.nu";
          src = filter {
            root = ./nu-scripts;
            include = ["./flyway-runner.nu"];
          };
          installPhase = ''
            # do not write anything in `bin` or `lib` !
            # I just need to add it to the store so that I can call it
            # via the runner wrapper!
            cp $src/flyway-runner.nu $out
          '';
        };
      in
        pkgs.writeShellApplication {
          name = "flyway-runner";
          runtimeInputs = [pkgs.nu pkgs.flyway];
          text = ''
            set -e
            exec nu --no-config-file ${p} "$@"
          '';
        };

      setup-db = pkgs.writeShellApplication {
        name = "setup-db";
        runtimeInputs = [postgres];
        text = ''
          set -o xtrace

          # in the future, this should work with a given input
          psql --dbname=template1 --file ${./createdb.sql} --variable=dbname=${pg_database}
        '';
      };

      ben-s2-tile-names-to-ids-runner = pkgs.writeShellApplication {
        name = "ben-s2-tile-names-to-ids-runner";
        runtimeInputs = [pkgs.nu pkgs.curl];
        text = let
          tile_names_csv_path =
            self.packages.${system}.bigearthnet-v1-tile-names-and-links;
          # p = self.packages.${system}.ben-s2-tile-names-to-ids;
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-s2-tile-names-to-ids";
            src = filter {
              root = ./nu-scripts;
              # need to include utils to correctly resolve the symbolic link
              include = [
                "./ben-s2-tile-names-to-ids.nu"
                "copernicus_dataspace"
                "utils"
              ];
            };
            installPhase = ''
              # do not write anything in `bin` or `lib` !
              # I just need to add it to the store so that I can call it
              # via the runner wrapper!
              mkdir $out
              cp -r $src/* $out/
            '';
          };
        in ''
          set -e
          exec nu --no-config-file ${p}/ben-s2-tile-names-to-ids.nu ${tile_names_csv_path}/tile_names_and_links.csv
        '';
      };

      ben-s2-l1c-tile-downloader = pkgs.writeShellApplication {
        name = "ben-s2-l1c-tile-downloader";
        runtimeInputs = stage1-packages ++ [pkgs.curl];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-s2-tile-downloader.nu";
            src = filter {
              root = ./nu-scripts;
              include = [
                "ben-s2-tile-downloader.nu"
                "utils"
                "copernicus_dataspace"
              ];
            };
            installPhase = ''
              mkdir $out
              cp -r $src/* $out/
            '';
          };
        in ''
          echo "Flake runner note:"
          echo "The servers are quite unstable and have frequently broken during development!"
          echo "It might happen that the download fails. Simply re-run this command until it succeeds."
          echo "If it keeps failing, please contact the copernicus dataspace support forum."
          echo ""
          echo ""
          export TILE_IDS_CSV="${bigearthnet-s2-tile-ids}/bigearthnet_s2_tile_ids.csv"
          exec nu --no-config-file ${p}/ben-s2-tile-downloader.nu "$@"
        '';
      };

      ben-s2-l1c-hasher = pkgs.writeShellApplication {
        name = "ben-s2-l1c-hasher";
        runtimeInputs = stage1-packages ++ [pkgs.zip pkgs.unzip];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-s2-l1c-hasher.nu";
            src = filter {
              root = ./nu-scripts;
              include = [
                "ben-s2-l1c-hasher.nu"
              ];
            };
            installPhase = ''
              mkdir $out
              cp $src/* $out/
            '';
          };
        in ''
          exec nu --no-config-file ${p}/ben-s2-l1c-hasher.nu "$@"
        '';
      };

      ben-s2-l1c-to-l2a-converter-runner = pkgs.writeShellApplication {
        name = "ben-s2-l1c-to-l2a-converter-runner";
        runtimeInputs = [pkgs.nu pkgs.sen2corPackage pkgs.pueue];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-s2-l1c-to-l2a-converter";
            src = filter {
              root = ./nu-scripts;
              include = ["ben-s2-l1c-to-l2a-converter.nu" "l1c_to_l2a" "utils"];
            };
            installPhase = ''
              mkdir $out
              cp -r $src/* $out/
            '';
          };
        in ''
          set -e
          exec nu --no-config-file ${p}/ben-s2-l1c-to-l2a-converter.nu "$@"
        '';
      };

      ben-v1-metadata-extractor-runner = pkgs.writeShellApplication {
        name = "ben-v1-metadata-extractor-runner";
        runtimeInputs = [pkgs.nu postgres];
        text = let
          extractor = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-v1-metadata-extractor";
            src = filter {
              root = ./nu-scripts;
              include = ["./ben-v1-metadata-extractor.nu" "utils"];
            };
            installPhase = ''
              mkdir $out
              cp -r $src/* $out/
            '';
          };
        in ''
          exec nu --no-config-file ${extractor}/ben-v1-metadata-extractor.nu "$@"
        '';
      };

      ben-data-generator = pkgs.writeShellApplication {
        name = "ben-data-generator";
        runtimeInputs = [pkgs.nu postgres pkgs.pueue pkgs.gdal];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-data-generator";
            src = filter {
              root = ./nu-scripts;
              include = [
                "ben-data-generator.nu"
                "utils"
              ];
            };
            installPhase = ''
              mkdir $out
              cp -r $src/* $out/
            '';
          };
        in ''
          echo "Hint:"
          echo "The following paths should be used:"
          echo "--clc2018-gpkg-path=${flake-packages.U2018_CLC2018_V2020_20u1}"
          echo "--country-geojson-path=${flake-packages.country-boundaries-geojson}"
          echo "--v1-metadata-dir=<PROJECT_ROOT/tracked-artifacts/ben-v1-metadata"
          echo ""
          exec nu --no-config-file ${p}/ben-data-generator.nu "generate-all-data" "$@"
        '';
      };

      ben-data-finalizer = pkgs.writeShellApplication {
        name = "ben-data-finalizer";
        runtimeInputs = [
          pkgs.nu
          pkgs.fd
          pkgs.gnutar
          flake-packages.bigearthnet-v1-s1s2-mapping
          flake-packages.bigearthnet-v1-patches-with-seasonal-snow
          flake-packages.bigearthnet-v1-patches-with-cloud-and-shadow
        ];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "ben-data-finalizer";
            src = filter {
              root = ./nu-scripts;
              include = [
                "ben-data-finalizer.nu"
                "ben-metadata-finalizer.sql"
              ];
            };
            installPhase = ''
              mkdir $out
              cp $src/* $out/
            '';
          };
        in ''
          echo "Hint:"
          echo "The paths from the 'ben-data-generator' script should be used."
          echo "The 's2-root-dir' is the generated 'patches' directory"
          echo "from the 'ben-data-generator' step."
          echo "The other '*-mapping-file's can be found in the metadata directory."
          echo "Except for the old-* files that can be found under:"
          echo "${flake-packages.bigearthnet-v1-s1s2-mapping}"
          echo "${flake-packages.bigearthnet-v1-patches-with-seasonal-snow}"
          echo "${flake-packages.bigearthnet-v1-patches-with-cloud-and-shadow}"
          echo ""
          exec nu --no-config-file ${p}/ben-data-finalizer.nu "finalize" "$@"
        '';
      };

      tiff-hasher = pkgs.writeShellApplication {
        name = "tiff-hasher";
        runtimeInputs = [pkgs.nu pkgs.fd];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "tiff-hasher.nu";
            src = filter {
              root = ./nu-scripts;
              include = ["./tiff-hasher.nu"];
            };
            installPhase = ''
              cp $src/tiff-hasher.nu $out
            '';
          };
        in ''
          exec nu --no-config-file ${p} "$@"
        '';
      };

      zstd-compressor = pkgs.writeShellApplication {
        name = "zstd-compressor";
        runtimeInputs = [pkgs.nu pkgs.zstd];
        text = let
          p = pkgs.stdenvNoCC.mkDerivation {
            name = "zstd-compressor.nu";
            src = filter {
              root = ./nu-scripts;
              include = ["zstd-compressor.nu"];
            };
            installPhase = ''
              cp $src/zstd-compressor.nu $out
            '';
          };
        in ''
          exec nu --no-config-file ${p} "$@"
        '';
      };

      zenodo-upload = pkgs.stdenvNoCC.mkDerivation {
        name = "zenodo-upload";
        src = inputs.zenodo-upload;
        runtimeInputs = with pkgs; [curl jq gnused];
        installPhase = ''
          mkdir -p $out/bin
          cp zenodo_upload.sh $out/bin/
        '';
      };

      ######  File Dependencies ######

      # Can be generated via ben-s2-tile-names-to-ids-runner !
      bigearthnet-s2-tile-ids = pkgs.stdenvNoCC.mkDerivation {
        name = "bigearthnet_s2_tile_ids";
        src = filter {
          root = ./tracked-artifacts;
          include = ["./bigearthnet_s2_tile_ids.csv"];
        };
        installPhase = ''
          mkdir $out
          ln -s $src/bigearthnet_s2_tile_ids.csv $out/
        '';
        meta = {
          description = "The Sentinel-2 L1C tile IDs that were downloaded from the Copernicus Dataspace Ecosystem to generate the dataset.";
          longDescription = ''
            The Sentinel-2 L1C tile IDs that were downloaded from the Copernicus Dataspace Ecosystem to generate the dataset.
            The CSV is generated (and can be re-generated) by the `ben-s2-tile-names-to-ids-runner`.
          '';
        };
      };

      # If this file isn't available anymore simply change out the following link
      # with a new instance hosting the file.
      # Or use the locally bzip2 compressed version under `tracked-artifacts/`
      bigearthnet-v1-tile-names-and-links = pkgs.stdenvNoCC.mkDerivation {
        name = "tile_names_and_links";
        src = pkgs.fetchurl {
          url = "https://git.tu-berlin.de/rsim/BigEarthNet-S2_tools/-/raw/master/files/tile_names_and_links.csv";
          hash = "sha256-znPd5NOOWrg1NIDgS+kI+AQbCvMNX46ebLHo1/GgWDo=";
        };
        dontUnpack = true;
        dontConfigure = true;
        dontBuild = true;
        installPhase = ''
          mkdir -p $out
          ln -s $src $out/tile_names_and_links.csv
        '';
        meta = {
          description = "The original BigEarthNet-v1.0 S2 patch to L1C tile name mapping.";
          homepage = "https://git.tu-berlin.de/rsim/BigEarthNet-S2_tools/";
        };
      };

      bigearthnet-v1-s1s2-mapping = pkgs.stdenvNoCC.mkDerivation {
        name = "s1s2_mapping.csv";
        src = pkgs.fetchurl {
          url = "https://git.tu-berlin.de/rsim/BigEarthNet-MM_tools/-/raw/master/files/s1s2_mapping.csv";
          hash = "sha256-xZ68o+K42a9bAaZSQzHwkdVJBb0jIv7gVhdt3UyWPjA=";
        };
        phases = [
          "installPhase"
        ]; # fixup phase tries to update symlinks but that isn't possible!
        installPhase = ''
          ln -s $src $out
        '';
        meta = {
          description = "The original BigEarthNet-v1.0 S2 to S1 patch name mapping.";
          homepage = "https://git.tu-berlin.de/rsim/BigEarthNet-MM_tools";
        };
      };
      bigearthnet-v1-patches-with-cloud-and-shadow = pkgs.stdenvNoCC.mkDerivation {
        name = "patches_with_cloud_and_shadow.csv";
        src = ./tracked-artifacts/patches_with_cloud_and_shadow.csv;
        phases = [
          "installPhase"
        ]; # fixup phase tries to update symlinks but that isn't possible!
        installPhase = ''
          ln -s $src $out
        '';
        meta = {
          description = "The BigEarthNet-v1.0 list of patches with cloud and shadow.";
          # homepage = "https://git.tu-berlin.de/rsim/BigEarthNet-MM_tools";
        };
      };
      bigearthnet-v1-patches-with-seasonal-snow = pkgs.stdenvNoCC.mkDerivation {
        name = "patches_with_seasonal_snow.csv";
        src = ./tracked-artifacts/patches_with_seasonal_snow.csv;
        phases = [
          "installPhase"
        ]; # fixup phase tries to update symlinks but that isn't possible!
        installPhase = ''
          ln -s $src $out
        '';
        meta = {
          description = "The BigEarthNet-v1.0 list of patches with seasonal snow.";
          # homepage = "https://git.tu-berlin.de/rsim/BigEarthNet-MM_tools";
        };
      };

      # https://gis.stackexchange.com/questions/143426/how-to-import-country-administrative-boundaries-from-osm-planet-to-postgis-polyg
      country-boundaries-geojson = pkgs.stdenvNoCC.mkDerivation {
        name = "country-boundaries.geojson";
        src = pkgs.fetchurl {
          url = "https://github.com/nvkelso/natural-earth-vector/raw/v5.1.2/geojson/ne_110m_admin_0_countries.geojson";
          hash = "sha256-aGbId9Ocupw1diCHiDmzNtVp+MZi08+rTLHb4tOcl38=";
        };

        phases = ["installPhase"];
        installPhase = ''
          ln -s $src $out
        '';
        meta = {
          description = "The 110m resolution administrative country mapping fixed at v5.1.2.";
          homepage = "https://www.naturalearthdata.com/";
        };
      };

      U2018_CLC2018_V2020_20u1 = pkgs.requireFile rec {
        name = "U2018_CLC2018_V2020_20u1.gpkg";
        url = "https://land.copernicus.eu/en/products/corine-land-cover/clc2018#Download";
        # calculated with nix-hash --sri --type sha256 <PATH>
        hash = "sha256-rTvuPmzLvbh+ugsiVM9Bd/aaVbWqa8GYmZk8kEt3oOY=";
        message = ''
          Unfortunately, we cannot download file ${name} automatically.
          Please go to ${url}, and download it manually.
          Make sure to download the Vector version of the dataset!
          It might take a while until the download can be started and finished.
          After downloading the file, please add it to the Nix store using:
            nix-store --add-fixed sha256 U2018_CLC2018_V2020_20u1.gpkg
          If there is a mismatch with the hash, please report this as a bug!
        '';
      };
    });
  };
}
