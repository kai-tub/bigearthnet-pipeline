-- sentinel2 tables could also get additional domains with implicit checks
-- but for quick check and for later use, this is sufficient.

CREATE TABLE sentinel2_l2a_metadata (
	l2a_product_id text CHECK (l2a_product_id ~ 'S2._MSIL2A_.*.SAFE$') PRIMARY KEY,
	metadata jsonb
);

create function sentinel2_raster_embed_nodata_from_metadata (rast raster, metadata jsonb)
	returns raster
	immutable
	strict
	parallel safe
	language sql
as $$
	with bands as (
		select 
			band_name,
			band_id,
			case -- json requires quote escaping so the output will contain quotes!
				when band_name ~ '^"B[1-9][1-2A]?"' then
					(metadata -> 'metadata' -> '' ->> 'SPECIAL_VALUE_NODATA')::int
				else NULL
			end as nodata_value
		from
			lateral (
				select
					jsonb_path_query(metadata, '$.bands[*].metadata.""."BANDNAME"')::text as band_name,
					jsonb_path_query(metadata, '$.bands[*]."band"')::int as band_id
			) bn
	)
	select
		-- FUTURE: This function REQUIRES the input rasters to be out-db
		-- I could fix it in the future, or provide an boolean to switch the behaviour
		-- I am starting to think that I made a grave mistake and that even if the documentation
		-- doesn't mention it, it looks like st_band does indeed return an out-of-db raster 'copy'
		-- instead of creating a new 'filled' raster
		st_addband(null::raster, array_agg(
			st_addband(
				null::raster,
				-- Setting the band for the band path here doesn't change the output, as all
				-- bands from the same tile subdataset point to the same 'file' (S2 subdataset folder)
				ST_BandPath(rast, bands.band_id),
				-- This IS required though, as we only want the bands that contain data
				array[bands.band_id],
				nodataval => bands.nodata_value
			-- the following order-by is crucial! Otherwise the ordering might break/be undefined!
			-- we want that our raster contains the exact same data in the exact same order!
			) order by bands.band_id)
		)
	from bands
$$;

comment on function sentinel2_raster_embed_nodata_from_metadata is 'This function was a beast! If you are this far down the code I hope you are here to learn
and not trying to find a bug...
If any part of the code base is error-prone it is this function.
The main idea is to have an inlineable, parallel safe sql function
that _fixes_ the wrong/unset nodata_values and sets them according to the metadata.

One could argue that this is the job of the upload script (raster2pgsql) but I assume that the
Sentinel2-L2A data source does not correctly indicate this value for GDAL or the tool itself reports it wrongly.
I assume the major issue is that the nodata value is 0 ONLY for the spectral bands and that it is valid for the
statistics bands. To correctly upload the data, I would have to patch raster2pgsql but that is not something
I am willing to do at the moment.

Also, I would argue that it modifies the _clean-state_ of the tiles
and that it should be done in a separat "step".

The next major issue was that I am trying to minimize the size of the database, as there is no
need at the moment to keep all of the band data within the DB, especially since this would require copying the
data from the slower storage to the highspeed storage.
If I simply create st_addbands without thinking about it, I am duplicating the tile data to ONLY change
a single nodata flag!

As I am currently using out-of-db tiles, the following function accounts for it
and works with the "band pointers" instead of the raster data.
This function WILL break at the moment if the tiles are stored within the db but it could be fixed in the future!

Additional documentation:
- https://docs.sentinel-hub.com/api/latest/data/sentinel-2-l2a/
- https://gis.stackexchange.com/questions/233874/what-is-the-range-of-values-of-sentinel-2-level-2a-images

FUTURE: Allow in-db raster within this function!
FUTURE: What about SATURATED?
';

-- l2a rasters are 'sub'-datasets from the top-level l2a dataset
CREATE TABLE sentinel2_l2a_rasters (
	l2a_product_id text check (l2a_product_id ~ 'S2._MSIL2A_.*.SAFE$'),
	dataset_name text,
	metadata jsonb,
	rast raster check (
		st_width(rast) = st_height(rast)
		and st_pixelheight(rast) = st_pixelwidth(rast)
		and (st_width(rast) * st_pixelwidth(rast)) = 109800
	),
	rast_with_nodata_set raster generated always as (sentinel2_raster_embed_nodata_from_metadata(rast, metadata)) stored,
	PRIMARY KEY (l2a_product_id, dataset_name)
);

comment on table sentinel2_l2a_rasters is '
The table contains the data of a Sentinel2-L2A tile including its metadata.
The primary key consists of the `l2a_product_id` (Sentinel2-L2A name) and its `dataset_name`. 
The `metadata` is stored as a `jsonb` type and contains the other relevant information.
The actual data is contained in `rast` but the Sentinel2-L2A format does NOT correctly
register the NODATA fields in the source images. To fix this, the table
is linked to a function that creates another raster where the correct NODATA value is
extracted from the metadata field and stored.

A given constraint is that the `rast` data covers the same amount of ground
with a quadratic shape! The test checks if the pixel-width and height are identical
in terms of the number of pixels and in the geometric units of the CRS (here meters).
And if the total length in the geometric unit is equal to 109800.
This side-length constant is important for some optimizations later!
';

-- TODO: Add tests to this function!
create function timestamptz_to_meteorological_season (timestamptz)
	returns text
	immutable
	strict
	parallel safe
	language sql
as $$
     select case 
         when $1 between 
             make_date(extract(year from $1)::int, 3, 1)
             and
             make_date(extract(year from $1)::int, 5, 31)
         then 'spring'
         when $1 between 
             make_date(extract(year from $1)::int, 6, 1)
             and
             make_date(extract(year from $1)::int, 8, 31)
         then 'summer'
         when $1 between 
             make_date(extract(year from $1)::int, 9, 1)
             and
             make_date(extract(year from $1)::int, 11, 30)
         then 'fall'
         else 'winter'
     end
$$;

-- TODO: Test this function!
create function extract_timestamptz_from_s2_metadata (jsonb)
	returns timestamptz
	immutable
	strict
	parallel safe
	language sql
as $$
	select to_timestamp(
		$1 -> 'metadata' -> '' ->> 'PRODUCT_START_TIME', 
		'YYYY-MM-DD"T"HH:MI:SS.MS"Z"'
	) at time zone 'UTC' at time zone 'UTC'
$$;

create view sentinel2_product_ids_failing_quality_indicators as (
	select distinct(l2a_product_id)
	from sentinel2_l2a_rasters slar 
	-- S2B_MSIL2A_20170818T112109_N9999_R037_T29SNB_20230901T112819.SAFE
	where 
		jsonb_path_exists(metadata, 'strict $.**.GEOMETRIC_QUALITY ? (@ != "PASSED")') -- 5 other tiles
		or 
		jsonb_path_exists(metadata, 'strict $.**.GENERAL_QUALITY ? (@ != "PASSED")')
		or
		jsonb_path_exists(metadata, 'strict $.**.RADIOMETRIC_QUALITY ? (@ != "PASSED")') -- 1 fails
		or
		jsonb_path_exists(metadata, 'strict $.**.SENSOR_QUALITY ? (@ != "PASSED")')
);

-- TODO: HERE: Think about switching out the x/y ints
-- with text that only consists of numeric values!
-- The motivation would be to already 'pad' the x/y values
-- so that they could be used to create a padded file name
-- As the x/y values already require ordering all patch-data
-- it should be mostly 'free' to also get the maximum pad-size
-- this would also make the code easier to read where the file name is generated
-- I also believe that the performance penalty shouldn't be too severe for this case
create type patch_identifier_components as (
	product_id text,
	x int,
	y int
);

create domain patch_identifier
	as patch_identifier_components
	check (
		-- I won't be checking the product_id here in detail!
		(value).product_id <> ''
		and (value).x >= 0
		and (value).y >= 0
	);

create table tile_patches_geometrical (
	patch_id patch_identifier,
	sensing_time timestamptz,
	season text generated always as (timestamptz_to_meteorological_season(sensing_time)) stored,
	geom geometry check (st_area(geom) = (1200 * 1200)), -- hardcoded for now TODO: extract as test later on!
	PRIMARY KEY (patch_id)
);

comment on table tile_patches_geometrical is 'A table that contains the patches generated from a S2 tile operation
from a geometrical viewpoint.
It contains:
- `patch_id` which consists of:
	- `product_id`
	- `x` and `y`:
		- These values uniquely identify the patch relative to the other patches from the source tile!
		- NOTE: The ordering/origin value is implementation dependent! There is no requirement to ensure that
			x=0, y=0 is at the top-left corner, for example!
		- A future version _could_ add additional constraints 
- `sensing_time`:
	- The start of the acquisition time
- `season`:
	- Is a generated column that derives the meterological season from the given sensing time
- `geom`:
	- Geometry
';

-- HERE: TODO: Decide for one semantic!
-- Understand what exactly 'stable' means in this context!
-- Update all other places that might require this!
-- NEAR-FUTURE: px_length should be extracted from the raster! Not the dataset name!
-- Then I should be able to rewrite this as a SQL function and move the
-- side-length check to an internal function or outside of the function as a pre-requisite call?
-- The only _required_ input parameter would be the raster itself
-- but then I should think long and hard what the output should look like!
create function sentinel2_l2a_product_to_patches (l2a_raster sentinel2_l2a_rasters, square_side_length_in_m integer)
	returns table(patch_id patch_identifier, dataset_name sentinel2_l2a_rasters.dataset_name%type, rast raster) 
	language plpgsql
	stable -- adding "stable" seems to have fixed the parallel use-case!
	parallel safe
as $$
	declare
		px_length integer;
		side_length_in_px integer;
begin
	px_length = case 
		when l2a_raster.dataset_name like '60m%' then 60
		when l2a_raster.dataset_name like '20m%' then 20
		when l2a_raster.dataset_name like '10m%' then 10
		else null
	end;
	if px_length is null then
		raise exception 'Unknown dataset_name provided!'
			using hint = 'Provide a valid dataset_name parameters!';
		return;
	end if;
	
	if mod(square_side_length_in_m, px_length) <> 0 then
		raise exception 'Side length in meter must be a multiple of % for the % otherwise there would be a fraction of a pixel left', px_length, dataset_name
			using hint = ('Provide a square side length that is a multiple of %, like 1200', px_length);
		return;
	end if;
	side_length_in_px = (square_side_length_in_m / px_length)::integer;
	-- if parallel safe and called in a parallel function then:
	-- cannot start commands during a parallel operation
	return query with 
	patches as (
		select l2a_raster.l2a_product_id as l2a_product_id, l2a_raster.dataset_name as dataset_name,
			st_tile(l2a_raster.rast_with_nodata_set, side_length_in_px, side_length_in_px, false) as rast
	), complete_patches as (
		select *
		from patches p
		where st_width(p.rast) = side_length_in_px and st_height(p.rast) = side_length_in_px
	)
	select 
		(
			p.l2a_product_id,
			-- strategy to ensure that upper-left corner is 0, 0
			-- assuming that CRS relates to larger x values to more 'right'
			-- and smaller y values to more 'up' (=> order by DESC !)
			(dense_rank() over (order by st_x(st_centroid(st_envelope(p.rast)))) - 1)::integer,
			(dense_rank() over (order by st_y(st_centroid(st_envelope(p.rast))) desc) - 1)::integer
		)::patch_identifier as patch_id,
		p.dataset_name,
		p.rast
	from complete_patches p;

	if not found then
		raise exception 'Non-existent product-id --> %', product_id
			using hint = 'Please check the provided product_id and the sentinel2_l2a_raster table';
	end if;
	return;
end
$$;

comment on function sentinel2_l2a_product_to_patches is 'Returns patches with the given spatial extent.
The input row from `sentinel2_l2a_rasters` will be used to provide the raster data and the dataset name
(spatial resolution). The raster with the nodata values set will be split into individual patches
of the given `square_side_length_in_m`. Consequently, the number of pixels per `dataset_name` will
vary as the spatial resolutions differ!
The function will drop the _incomplete_ border patches if they do not have the given spatial extent.
So for example, with a spatial resolution of 1200m per patch, the last column and row will always be
dropped.
The function return a `patch_identifier` that embeds the source `l2a_product_id` but extends it
with x/y coordinates. The origin (0/0) is the upper-left part of the given CRS
(which is the upper-left pixel with our given tiles).

NEAR-FUTURE: Could derive the spatial resolution of each pixel not by the dataset name but by the
raster data via ST_PixelHeight and ST_PixelWidth!
';

create function insert_tile_patches_geometrical_from_new_sentinel2_l2a_rasters (square_side_length_in_m integer) 
	returns integer
	language plpgsql
as $$
	declare
		inserted_rows integer;
		epsgs int[];
		epsg int;
begin
	-- Don't do anything on conflict!
	-- drop index to allow fast data ingestion!
	select array_agg(distinct st_srid(geom)) from tile_patches_geometrical into strict epsgs;
	if epsgs is not null then
		foreach epsg in array epsgs
		loop
			execute format('drop index if exists tile_patches_geometrical_geom_%s', epsg);
		end loop;
	end if;
	drop index if exists tile_patches_geometrical_srid;

	with inserted as (
		insert into tile_patches_geometrical (patch_id, sensing_time, geom) 
			with sentinel2_l2a_rasters_60m as (
				select *
				from sentinel2_l2a_rasters slar
				where dataset_name like '60m%'
			)
			select p.patch_id, sensing_time, st_envelope(p.rast) as geom
				from sentinel2_l2a_rasters_60m s,
					lateral extract_timestamptz_from_s2_metadata(s.metadata) as sensing_time,
					lateral sentinel2_l2a_product_to_patches(s.*, square_side_length_in_m) as p
		on conflict do nothing
		returning patch_id -- FUTURE: Understand the performance implications of this!
	) select count(*) into strict inserted_rows
	from inserted;

	create index tile_patches_geometrical_srid on tile_patches_geometrical using btree(st_srid(geom));
	-- needs to be updated, as their might've been new areas added
	select array_agg(distinct st_srid(geom)) from tile_patches_geometrical into strict epsgs;
	if epsgs is not null then
		foreach epsg in array epsgs
		loop
			raise notice 'Creating gist index tile_patches_geometrical_geom_%', epsg;
			execute format(
				'create index tile_patches_geometrical_geom_%s on tile_patches_geometrical using gist(geom) where st_srid(geom) = %s', 
				epsg, 
				epsg
			);
		end loop;
	end if;
	
	analyze tile_patches_geometrical;
	return inserted_rows;
end
$$;

comment on function insert_tile_patches_geometrical_from_new_sentinel2_l2a_rasters is 'Generate all the geometric patches from the current sentinel2_l2a_rasters table by
calling `sentinel2_l2a_raster_to_geom_patches` under the hood and formatting the output, i.e. extracting the timstamptz value from the metadata.
Will drop the indexes, insert new data, i.e. skipping existing/conflicting rows, and then rebuild the indexes.
This should allow for the fastest possible ingestion of large tables, while also keeping the index/index bloat minimal.
The function will return the number of actually inserted rows as an integer!
IMPORTANT: The function will simply return the extent of the patches as polygons and NOT the valid regions.
So these geometries show the geographical locations of the patches WITH nodata values!

The indexes that are created are an btree index on the st_srid(geom) expression
and partial indexes on the geometry columns that use a gist index on the
geometry column that share a common epsg value. This makes it easy to quickly
query the geometry data with predefined espg regions.
It will also manually call analyze to ensure that follow-up operations can be optimally planned by the query planner.
';

-- maybe the filling of the return rows is causing a bottle-neck!
-- FUTURE: Drop the temporary table and use a temporary expression index instead!
-- I am quite sure that there is little to be gained on the temporary table
-- if a gist index expression is combined with the partial index on season
-- -> With the season wise filtering it might make no difference as it covers
-- the exact same areas
create function find_overlap_pairs_in_tile_patches_geometrical (common_epsg integer, minimum_common_area_relative_to_patch_area_threshold numeric default 0.01)
	returns table(patch_id_1 patch_identifier, patch_id_2 patch_identifier, geom geometry)
	language plpgsql
	-- stable?
as $$
begin
	-- TODO: ensure that minimum_common_area_relative_to_patch_area_threshold is between 0 and 1!
	-- TODO: Also compare it with a multi-column gist index, where the first column
	-- is the expression and the second column is the season! 
	-- https://www.postgresql.org/docs/current/indexes-multicolumn.html
	create temporary table __patches_same_epsg 
		on commit drop -- a function is executed as a single transaction block!
		as
		select patch_id, 
			season,
			st_transform(tp.geom, common_epsg) as geom
		from tile_patches_geometrical tp;

	create index patches_same_espg_spgist_geom on __patches_same_epsg using spgist(geom);
	-- index on composite type has weird formatting, see:
	-- https://stackoverflow.com/questions/15040135/postgresql-create-an-index-for-fields-within-a-composite-type
	-- splitting the index resulted in postgresql not composing the index as I would've expected
	-- TODO: Check again in the end if this stays performant!
	create index patches_same_epsg_keys on __patches_same_epsg using btree(((patch_id).product_id), season);
	analyze __patches_same_epsg; -- 5 failed geometric-- important to update the statistics of the temporary table!
	
	return query select p1.patch_id as patch_id_1, p2.patch_id as patch_id_2, st_intersection(p1.geom, p2.geom) as geom
		from __patches_same_epsg p1, __patches_same_epsg p2
		where (
				(p1.patch_id).product_id <> (p2.patch_id).product_id
				-- This function does NOT test self-intersections within a tile!
				-- or p1.x <> p2.x
				-- or p1.y <> p1.y
			)	
			and p1.season = p2.season -- season is defined as the time-step!
			and st_intersects(p1.geom, p2.geom)
			-- maybe a bit uncommon to use the following equation but generally area(p1) ~ area(p2)
			-- then the equation is roughly area(p1 intersection p2) > area(p1) * threshold, which is the documented
			-- behavior. The following equation ensures symmetry and accounts for possible different reprojection variances 
			and st_area(st_intersection(p1.geom, p2.geom)) / (st_area(p1.geom) + st_area(p2.geom)) > (0.5 * minimum_common_area_relative_to_patch_area_threshold);

	if not found then
		raise info 'No overlaps across tiles found.'
			using hint = 'This is quite unusual with larger sets of tiles. Maybe check tile_patches_geometrical.';
	end if;
	return;
end
$$;

comment on function find_overlap_pairs_in_tile_patches_geometrical is 'The function will return all overlapping patch pairs from tile_patches_geometrical.
The result will return two columns `patch_id_1` and `patch_id_2` which are the `patch_id` keys from the
source `tile_patches_geometrical` table, as well as the intersected area as a geometry.
An overlap is only counted as an overlap if the area covers more than the given relative threshold,
where the threshold is the percentage of a patch area.
If the threshold is set to 1 it means that an overlap is only counted if both patches
cover the same area! A threshold of 0.5 means that an overlap is counted if the one
patch overlaps more than 50% of the other patch.

Please note that function will ONLY check overlaps between DIFFERENT tiles (l2a_product_ids)!
The caller is responsible to ensure that this is the case.
Otherwise it would lead to a very high performance impact and make the query 5-10x slower
and will _only_ propagate an error from an earlier stage.

Internally, the function reprojects ALL patches into the provided `common_epsg`
and then checks the total overlapping area between two patches and will return
the pair if the overlapping area is larger than the threshold.
It will do some _tricks_ to ensure that possible reprojection errors between
two patches will overlap in both cases.
';

create table tile_patches_geometrical_overlaps (
	patch_id patch_identifier primary key references tile_patches_geometrical
);

comment on table tile_patches_geometrical_overlaps is '
	A simple table that is used to track whether or not a specific
	patch in `tile_patches_geometrical` overlaps with ANY other patch.
	The idea is that this table is only used for quick filtering/grouping
	of overlapping patches. As these usually need to be handled in a specific
	way when creating the database.
';

create or replace function insert_new_tile_patches_geometrical_overlaps (common_epsg integer, minimum_common_area_relative_to_patch_area_threshold numeric default 0.01) 
	returns integer
	language plpgsql
as $$
	declare
		inserted_rows integer;
begin
	-- Don't do anything on conflict!
	with overlap_pairs as (
		select (find_overlap_pairs_in_tile_patches_geometrical(common_epsg, 0.01)).*
	), distinct_patch_ids as ( 
		select patch_id_1 as patch_id
		from overlap_pairs
		union -- union also removes duplicate rows from the result set!
		select patch_id_2 as patch_id
		from overlap_pairs
	), inserted as (
		insert into tile_patches_geometrical_overlaps
			select patch_id
			from distinct_patch_ids
			on conflict do nothing
			returning patch_id
	) select count(*) into strict inserted_rows
	from inserted;
	analyze tile_patches_geometrical_overlaps;
	return inserted_rows;
end
$$;

comment on function insert_new_tile_patches_geometrical_overlaps is 'A tiny wrapper
to insert new data from `tile_patches_geometrical` into the `tile_patches_geometrical_overlaps` table.
';

create domain split_identifier
	as text
	check (
		value = 'train'
		or value = 'validation'
		or value = 'test'
	);

-- By creating this table with a primary key, there is NO NEED
-- for a uniqueness test anymore!
create table tile_border_split (
	patch_id patch_identifier primary key,
	split split_identifier not null
);

-- How many patches contain FULLY labeled areas after rasterization of U2018?
-- -> This is most likely a join operation at the end to filter the final patches
-- 		as this could also happen AFTER grouping into train/val/test split buckets!
-- 		Also has the nice 'benefit' that it theoretically allows the same split strategy to be used by segmentation with UNLABELED areas
-- -> Theoretically, same holds for alignment & valid patch tests. 
-- -> Simply use patches_geometrical
-- Formula for a 2/4 train, 1/4 val, 1/4 test split:
-- {a_1: s*(2 - sqrt(2))/4, a_2: s*(-1/4 + sqrt(2)/4)}
-- NOTE: David had a much nicer formulae + derivation use that in paper!
create or replace function insert_new_tile_border_split ()
	returns integer
	language plpgsql
as $$
	declare inserted_rows integer;
begin
	-- FUTURE: Could get the magic number from an inlineable SQL function
	-- but this constant is safe as it is a constraint for the sentinel2_l2a_rasters table!
	-- NEAR-FUTURE: Could also externalize it as a separat function to allow easier testing
	-- and visualizations for later
	with tile_based_border_split as (
		select l2a_product_id, train_area, val_area, test_area
		from sentinel2_l2a_rasters slar,
			lateral st_envelope(slar.rast) as tile_geom,
			lateral (select 109800 as s) const,
			lateral (select s * (2.0 - sqrt(2.0)) / 4.0 as a1) res_a1,
			lateral st_buffer(tile_geom, -1 * a1) as val_and_test,
			lateral st_difference(tile_geom, val_and_test) as train_area,
			lateral (select s * (-1.0/4 + sqrt(2.0)/4.0) as a2) res_a2,
			lateral st_buffer(val_and_test, -1.0 * a2) as test_area,
			lateral st_difference(val_and_test, test_area) as val_area
		where dataset_name like '10m%' -- <- shouldn't make a difference
	), patch_split_assignment_wide as (
			select tpg.patch_id,
			st_intersects(centroid, s.train_area) as train,
			st_intersects(centroid, s.val_area) as validation,
			st_intersects(centroid, s.test_area) as test
		from tile_patches_geometrical tpg
			join tile_based_border_split s
			on (tpg.patch_id).product_id = s.l2a_product_id,
			lateral st_centroid(tpg.geom) as centroid
	), inserted as (
		insert into tile_border_split (patch_id, split) 
			select patch_id, 'train'::split_identifier as split
			from patch_split_assignment_wide sa
			where sa.train
			union
			select patch_id, 'validation'::split_identifier as split
			from patch_split_assignment_wide sa
			where sa.validation
			union
			select patch_id, 'test'::split_identifier as split
			from patch_split_assignment_wide sa
			where sa.test
			on conflict do nothing
			returning patch_id
	) select count(*) into strict inserted_rows
	from inserted;
	analyze tile_border_split;
	return inserted_rows;
end
$$;


-- can this work on nodata if I provide a different nodata value than the actual one provided?
create function regclassarg_find_value_in_raster (rast raster, search_value bigint, out result reclassarg)
	returns reclassarg
	immutable
	strict
	parallel safe
	language plpgsql
as $$
	declare 
		max_band_val bigint;
		t text;
--		result alias for $0;
begin
	if st_numbands(rast) <> 1 then
		raise exception 'This function must be called with a single-band raster!'
			using hint = 'Call st_band(rast, Y) first';
		return;
	end if;

	if st_bandpixeltype(rast) <> '16BUI' then
		raise exception 'Only 16BUI is currently supported!';
			return;
	end if;

	max_band_val = 65535; -- power(2, 16) - 1;
	
	-- case if search_value = 0 then 0:0, 1-MAX
	-- if search_value > 0 then MIN-search_val:1 
	if search_value <> 0 then
		raise exception 'This function currently only supports searching for 0 values';
		return;
	end if;

	t = search_value || ':1, (' 
			|| search_value || '-' || max_band_val || 
		']:0';
	
	raise notice 'reclassexpr %', t;

	result = row(1, t, '1BB', NULL)::reclassarg;
	return;
end
$$;

comment ON FUNCTION regclassarg_find_value_in_raster IS '
	A function that produces a regclassarg row for use in `st_reclass`.
	It will return an expression that will look for a specific value inside of
	the raster and return 1 if the value is equal to the `search_value` or 0
	if it is not. The resulting band is therefore a single bit boolean band
	that makes it highly efficient for further querying, especially in combination
	with st_dumpaspolygons to produce geometries of these values.
	It will ensure that the input raster only consists of a single band to minimize
	accidental implicit behavior.
	Due to time constraints, the current function only works for 16BUI bands and a
	search value of 0. Though the function body shows how the code could/should be extended
	in the future.
	In our tests with the sentinel2 data, we could observe that this function (and therefore
	st_reclass) could be executed on the entire S2 tile without the need for tiling the input!
';


-- FUTURE: Upgrade these types to a common domain type and shared them across the tables!
create table sentinel2_invalid_regions (
	l2a_product_id text check (l2a_product_id ~ 'S2._MSIL2A_.*.SAFE$'),
	dataset_name text,
	band_name text,
	band_id int check (band_id > 0),
	geom geometry check (st_isvalid(geom))
);

CREATE function sentinel2_invalid_data_geom_finder (sentinel2_l2a_rasters)
	returns setof sentinel2_invalid_regions
	stable
	parallel safe
	language sql
as $$
	-- TODO: Add a test that ensures that the nodata values from different bands are respected
	-- the result does not provide multi-polygons but rather single polygons, potentially
	-- multiple Polygons per id/dataset/band_id pair if they are further apart
	-- It seems like this isn't causing any issues
	select l2a_product_id, dataset_name, band_name, band_id, (dp).geom
	from (
			select $1.*, b.band_id, b.band_name, band.*
			from lateral (select -- this SHOULD be the current row in use 
				jsonb_path_query($1.metadata, '$.bands[*].metadata.""."BANDNAME"')::text as band_name,
				jsonb_path_query($1.metadata, '$.bands[*]."band"')::int as band_id
			) b,
			-- Note: here I am using the 'original' raster without the set `nodata` value
			-- as the 'value_finder' could otherwise skip over the nodata values!
			-- As I am setting the nodata value within the finder function to NULL
			-- it shouldn't make a difference but better safe than sorry
			lateral (select st_band($1.rast, b.band_id) as r, st_bandnodatavalue($1.rast_with_nodata_set) as nodata) band
		where
			band_name ~ '^"B[1-9][1-2A]?"' -- TODO: Externalize this function!
	) as filtered,
	lateral (select
		-- https://postgis.net/workshops/de/postgis-intro/rasters.html#reclassify-your-raster-using-st-reclass
		st_reclass(
			r,
			regclassarg_find_value_in_raster(r, nodata::bigint)
--			-- https://postgis.net/docs/reclassarg.html
		) as raster
	) as rec,
	lateral st_dumpaspolygons(rec.raster, 1, exclude_nodata_value => true) as dp
	where 
		val = 1
$$;

comment ON FUNCTION sentinel2_invalid_data_geom_finder IS '
	Given a sentinel2 raster row, return polygons that span across
	rasters that contain invalid/nodata values. It will merge all pixels with
	shared borders into a single polygon. But if multiple pixels contain nodata and
	do not share any borders, they will result in independent polygons.
	As a result, the function might return multiple rows per product/dataset/band combination.
	It will do so for each input band. Please note that there is no guarantee that the
	nodata values are shared across the datasets/bands.
	In our tests, we have found datasets from the same product that contain nodata values
	at specific pixel values but not in the lower resolution band!
';

create function insert_new_sentinel2_invalid_regions ()
	returns integer
	language plpgsql
as $$
declare
		inserted_rows integer;
begin
	-- for this operation this seem to help significantly
	set min_parallel_table_scan_size to 0;
	set min_parallel_index_scan_size to 0;
	set parallel_setup_cost to 0.0;
	set parallel_tuple_cost to 0.0;

	create temporary table __invalid_regions_res
	as (
		select (sentinel2_invalid_data_geom_finder (slar.*)).*
		from sentinel2_l2a_rasters slar
	);

	with inserted as (
	insert into sentinel2_invalid_regions
		select *
		from __invalid_regions_res
		on conflict do nothing
		returning l2a_product_id
	) select count(*) into strict inserted_rows
	from inserted;

	create index if not exists sentinel2_invalid_regions_id_idx on sentinel2_invalid_regions using btree(l2a_product_id);

	analyze sentinel2_invalid_regions;
	set min_parallel_table_scan_size to default;
	set min_parallel_index_scan_size to default;
	set parallel_setup_cost to default;
	set parallel_tuple_cost to default;

	return inserted_rows;
end
$$;

create FUNCTION sentinel2_l2a_raster_split_into_bands (
	metadata sentinel2_l2a_rasters.metadata%TYPE,
	rast sentinel2_l2a_rasters.rast%TYPE
)
	RETURNS table(
		band_name text,
		band_id int,
		rast sentinel2_l2a_rasters.rast%type
	)
	immutable
	strict
	parallel safe 
	LANGUAGE SQL
AS $$
	select b.band_name, b.band_id, b_raster.rast
			from lateral (select
				jsonb_path_query(metadata, '$.bands[*].metadata.""."BANDNAME"')::text as band_name,
				jsonb_path_query(metadata, '$.bands[*]."band"')::int as band_id
			) b,
			lateral (select 
				st_band(rast, b.band_id) as rast
			) b_raster;
$$;

comment on function sentinel2_l2a_raster_split_into_bands is 
'Given the Sentinel2-L2A product metadata explode the given raster into the indivdual bands while adding the
`band_name` from the metadata file and keeping track of the `band_id` to ensure that the original band
can be back-tracked. This function can be used not only for the entire raster but also patches/subviews of
the S2 tile, as long as the correct metadata file is provided. 
NOTE: In the current implementation, we are calling this function with rast_with_nodata_set to ensure
that the derived raster contains the up-to-date information!';

create function derive_s2_tile_patches_pad_size (square_side_length_in_m integer)
	returns integer
	immutable
	parallel safe
	language sql
as $$
	-- derived via:
	-- select distinct(st_pixelwidth(rast) * st_width(rast)) from sentinel2_l2a_rasters;
	select length(
		(
			floor(109800 / square_side_length_in_m) -- since we only keep "FULL" patches
		- 1 -- as we start counting with 0 for patch_id
		)::text
	) -- derive max pad size
$$;

comment on function derive_s2_tile_patches_pad_size is '
	Simple function to quickly derive the max `pad_size` required for use in
	`patch_identifier_to_text`. It will take advantage of the fact that the
	Sentinel2-L2A tiles have a constant width/height (which is also enforced
	as a constraint in the s2 table!) and use it to derive the pad size
	given the `square_side_length_in_m`, where the parameter should have also
	been used to create the patches.
';

create function patch_identifier_to_text (p patch_identifier, pad_size int)
	returns text
	immutable
	language sql
	parallel safe
as $$
	select regexp_replace(p.product_id, '_[0-9T]+\.SAFE$', '')
	|| '_'
	|| lpad(p.x::text, pad_size, '0') 
	|| '_'
	|| lpad(p.y::text, pad_size, '0')
$$;

-- I assume this has to do with the table parameter! I _want_ it to operate on the row not table!
create function sentinel2_l2a_rasters_to_named_patches(sentinel2_l2a_rasters, square_side_length_in_m integer default 1200)
	returns table(l2a_patch_id patch_identifier, pretty_patch_name text, pretty_band_name text, rast raster)
	stable
	language sql
as $$
	with patches as (
		select $1.metadata,
			(sentinel2_l2a_product_to_patches($1, square_side_length_in_m)).*
	), exploded_patches as (
	select p.patch_id, band_data.*
		from patches p,
		-- Remember: p.rast from `sentinel2_l2a_product_to_patches` already returns the rast_with_nodata_set!
		lateral sentinel2_l2a_raster_split_into_bands(p.metadata, p.rast) as band_data
		where band_data.band_name ~ '^"B[1-9][1-2A]?"'
		-- the ordering will populate one band-group after each other but this is the 'correct'
		-- way, since it minimizes opening the different tiles, i.e. do NOT order by patch_id!
		-- order by patch_id
	) select 
		patch_id,
		patch_identifier_to_text(patch_id, derive_s2_tile_patches_pad_size(square_side_length_in_m)) as pretty_patch_name,
			regexp_replace(
				trim('"' from band_name), -- yes, we keep the " until here, to highlight that it comes from the JSON file!
				'B([1-9])$',
				'B0\1'
			) as pretty_band_name, 
		-- Consider adding the band name/id!
		rast
		from exploded_patches
$$;

comment ON FUNCTION sentinel2_l2a_rasters_to_named_patches IS '
Creates patches using `sentinel2_l2a_product_to_patches` from the given
`sentinel2_l2a_raster` row. The patches will have the
`product_id`, a `pretty_patch_name` that can be used as a template for the file name,
as well as the (per-band) raster data.
The per-band data is generated by calling `sentinel2_l2a_raster_split_into_bands`,
where it will also only keep the data spectral bands.
The x and y positions of the patch_id component of the pretty name will be padded
until the character length is equal to the largest possible character length
(usually 2 places). ([...]_1_0 -> [...]_01_00)
The band name will also be padded if it is a single digit band name (and not B8A), so
for example, B1 -> B01.

NOTE: Be vary of changing this function! 
The main goal is to ensure that no data will be materialized within the database
and that no data needs to be copied from this function.
It is important that the function can be inlined for further optimizations
=> Requires stable SQL function!
';

-------------------- CLC specific

create domain clc_code_18 as text
check (
	VALUE in (
		'111', '112', 
		'121', '122', '123', '124',
		'131', '132', '133', 
		'141', '142', 
		'211', '212', '213', 
		'221', '222', '223', 
		'231', 
		'241', '242', '243', '244',
		'311', '312', '313',
		'321', '322', '323', '324', 
		'331', '332', '333', '334', '335',
		'411', '412',
		'421', '422', '423',
		'511', '512',
		'521', '522', '523', 
		'999'
	)
);

-- TODO: Add a self-overlapping test!
create table U2018_CLC2018_V2020_20u1 (
	id text primary key,
	code_18 clc_code_18,
	remark text,
	area_ha numeric check (area_ha > 0),
	-- ensure that the geometry isn't invalid! May happen with non gpkg source!
	-- maybe also check that the SRID is not null? <- This should avoid issues caused by
	-- weird geometry behavior
	geom geometry check (st_isvalid(geom)
		and st_srid(geom) <> 0)
);

comment on table U2018_CLC2018_V2020_20u1 is '
This table should contain the data from `U2018_CLC2018_V2020_20u1.gpkg`.
It will enforce additional constraints/checks to ensure that the data is valid.
One goal was to also ensure that the CRS is set *but* this is currently not working
with ogr2ogr.
I have no idea, why the GPKG driver or PostGIS writer has so many issues setting the SRID field...
I am falling back to a trigger to enforce the correct SRID. It is important for me to guarantee
that this value is set. Ideally, once I understand how to enforce the SRID value when calling
ogr2ogr I can drop the trigger but until then, it is a viable work-around.
One can check the individual geometries in which case the CRS is correctly identified by GDAL...
';

create function set_srid_to_3035 ()
	returns trigger
	immutable
	parallel safe
	language plpgsql as 
$$
begin
	new.geom := st_setsrid(new.geom, 3035);
	return new;
end
$$;

create trigger set_srid_to_3035_trigger
before insert or update on U2018_CLC2018_V2020_20u1
for each row execute function set_srid_to_3035();

create table clc_code_18_to_clc_name (
	clc_code_18 clc_code_18 primary key,
	clc_name text not null check (clc_name <> '')
);

-- Generated by `generate_clc_code_to_name.nu`
insert into clc_code_18_to_clc_name values
    ('111'::clc_code_18, 'Continuous urban fabric'),
    ('112'::clc_code_18, 'Discontinuous urban fabric'),
    ('121'::clc_code_18, 'Industrial or commercial units'),
    ('122'::clc_code_18, 'Road and rail networks and associated land'),
    ('123'::clc_code_18, 'Port areas'),
    ('124'::clc_code_18, 'Airports'),
    ('131'::clc_code_18, 'Mineral extraction sites'),
    ('132'::clc_code_18, 'Dump sites'),
    ('133'::clc_code_18, 'Construction sites'),
    ('141'::clc_code_18, 'Green urban areas'),
    ('142'::clc_code_18, 'Sport and leisure facilities'),
    ('211'::clc_code_18, 'Non-irrigated arable land'),
    ('212'::clc_code_18, 'Permanently irrigated land'),
    ('213'::clc_code_18, 'Rice fields'),
    ('221'::clc_code_18, 'Vineyards'),
    ('222'::clc_code_18, 'Fruit trees and berry plantations'),
    ('223'::clc_code_18, 'Olive groves'),
    ('231'::clc_code_18, 'Pastures'),
    ('241'::clc_code_18, 'Annual crops associated with permanent crops'),
    ('242'::clc_code_18, 'Complex cultivation patterns'),
    ('243'::clc_code_18, 'Land principally occupied by agriculture, with significant areas of natural vegetation'),
    ('244'::clc_code_18, 'Agro-forestry areas'),
    ('311'::clc_code_18, 'Broad-leaved forest'),
    ('312'::clc_code_18, 'Coniferous forest'),
    ('313'::clc_code_18, 'Mixed forest'),
    ('321'::clc_code_18, 'Natural grassland'),
    ('322'::clc_code_18, 'Moors and heathland'),
    ('323'::clc_code_18, 'Sclerophyllous vegetation'),
    ('324'::clc_code_18, 'Transitional woodland/shrub'),
	('331'::clc_code_18, 'Beaches, dunes, sands'),
    ('332'::clc_code_18, 'Bare rock'),
    ('333'::clc_code_18, 'Sparsely vegetated areas'),
    ('334'::clc_code_18, 'Burnt areas'),
    ('335'::clc_code_18, 'Glaciers and perpetual snow'),
    ('411'::clc_code_18, 'Inland marshes'),
    ('412'::clc_code_18, 'Peatbogs'),
    ('421'::clc_code_18, 'Salt marshes'),
    ('422'::clc_code_18, 'Salines'),
    ('423'::clc_code_18, 'Intertidal flats'),
    ('511'::clc_code_18, 'Water courses'),
    ('512'::clc_code_18, 'Water bodies'),
    ('521'::clc_code_18, 'Coastal lagoons'),
    ('522'::clc_code_18, 'Estuaries'),
    ('523'::clc_code_18, 'Sea and ocean'),
    ('999'::clc_code_18, 'UNLABELED');

create table clc_code_18_to_ben19_name (
	clc_code_18 clc_code_18 primary key,
	ben19_name text not null check (ben19_name <> '')
);

-- Generated by `generate_clc_code_to_name.nu`
insert into clc_code_18_to_ben19_name values
  	('111'::clc_code_18, 'Urban fabric'),
	('112'::clc_code_18, 'Urban fabric'),
	('121'::clc_code_18, 'Industrial or commercial units'),
	('122'::clc_code_18, 'UNLABELED'),
	('123'::clc_code_18, 'UNLABELED'),
	('124'::clc_code_18, 'UNLABELED'),
	('131'::clc_code_18, 'UNLABELED'),
	('132'::clc_code_18, 'UNLABELED'),
	('133'::clc_code_18, 'UNLABELED'),
	('141'::clc_code_18, 'UNLABELED'),
	('142'::clc_code_18, 'UNLABELED'),
	('211'::clc_code_18, 'Arable land'),
	('212'::clc_code_18, 'Arable land'),
	('213'::clc_code_18, 'Arable land'),
	('221'::clc_code_18, 'Permanent crops'),
	('222'::clc_code_18, 'Permanent crops'),
	('223'::clc_code_18, 'Permanent crops'),
	('231'::clc_code_18, 'Pastures'),
	('241'::clc_code_18, 'Permanent crops'),
	('242'::clc_code_18, 'Complex cultivation patterns'),
	('243'::clc_code_18, 'Land principally occupied by agriculture, with significant areas of natural vegetation'),
	('244'::clc_code_18, 'Agro-forestry areas'),
	('311'::clc_code_18, 'Broad-leaved forest'),
	('312'::clc_code_18, 'Coniferous forest'),
	('313'::clc_code_18, 'Mixed forest'),
	('321'::clc_code_18, 'Natural grassland and sparsely vegetated areas'),
	('322'::clc_code_18, 'Moors, heathland and sclerophyllous vegetation'),
	('323'::clc_code_18, 'Moors, heathland and sclerophyllous vegetation'),
	('324'::clc_code_18, 'Transitional woodland, shrub'),
	('331'::clc_code_18, 'Beaches, dunes, sands'),
	('332'::clc_code_18, 'UNLABELED'),
	('333'::clc_code_18, 'Natural grassland and sparsely vegetated areas'),
	('334'::clc_code_18, 'UNLABELED'),
	('335'::clc_code_18, 'UNLABELED'),
	('411'::clc_code_18, 'Inland wetlands'),
	('412'::clc_code_18, 'Inland wetlands'),
	('421'::clc_code_18, 'Coastal wetlands'),
	('422'::clc_code_18, 'Coastal wetlands'),
	('423'::clc_code_18, 'UNLABELED'),
	('511'::clc_code_18, 'Inland waters'),
	('512'::clc_code_18, 'Inland waters'),
	('521'::clc_code_18, 'Marine waters'),
	('522'::clc_code_18, 'Marine waters'),
	('523'::clc_code_18, 'Marine waters'),
	('999'::clc_code_18, 'UNLABELED');

-- clc_code_18_to_8bit_value
-- Thought about making a 'smarter' mapping before exporting the segmentation
-- maps as TIFFs but in the end,
-- it is never intuitiv and one should always just look up the
-- values and color it accordingly
-- Even if the patch is encoded with uint16 although uint8 would
-- be sufficient after remapping, it is just not that intuitiv
-- and compressing the segmentation maps is actually not _that_ bad...
-- Otherwise, the alternative would be to map the values from '111'...'999'
-- to increasing integers and then keeping track of these values in all
-- down-stream tasks...

-------------------- BIGEARTHNET specific
-- currently these domains are only used for BEN patches!
create domain s2_mission_id as text
check (
	VALUE ~ '^S2[AB]$$'
);

create domain s2_product_level as text
check (
	VALUE ~ '^MSIL(1B|1C|2A)$'
);

create type bigearthnet_patch_name_components as (
	mission_id s2_mission_id,
	product_level s2_product_level,
	sensing_time timestamptz,
	x int,
	y int
);

create domain bigearthnet_patch_name
	as bigearthnet_patch_name_components
	check (
		(value).x >= 0
		and (value).y >= 0
	);

comment on domain bigearthnet_patch_name is 'A composite data type for data embedded inside of bigearthnet patch file names
It contains:
- `s2_mission_id`: 
	- Source satellite, which is either S2A/S2B
	- Dedicated data-type to enforce constraint
- `s2_product_level`:
	- MSIL1C/MSIL2A are the product levels we process, see the product specification document for more details
- `sensing_time`
	- The datatake acquisition time in the format YYYYMMDDThhmmss
	- BigEarthNet v1 patches may have missing leading 0 for hour, which is a bug in v1
		- may also have seconds >60 which is another bug from v1
- x position of the patch from the source tile
- y position of the patch from the source tile

IMPORTANT: Due to the bugs in the v1 patch file names, this composite type
does not allow one to back-reference the original filename!
The original file name should be kept around inside of the table!
This may be used as a generated column to quickly access the relevant data.
';

-- ' Fix tree-sitter issue

create function text_to_bigearthnet_patch_name (text)
	returns bigearthnet_patch_name
	immutable
	strict
	parallel safe
	language sql
as $$
	-- hour always has a leading zero; bug inherited from BEN
	with with_double_digit_hour as (
		select regexp_replace(split_part($1, '_', 3), 'T(\d{5}$)', 'T0\1') as t
	), fixed_seconds_overflow as (
	-- another bug inherited from BEN are seconds over >60
		select regexp_replace(t, '([6-9]\d)$', '59') as t
		from with_double_digit_hour
	)
	select (
		split_part($1, '_', 1)::s2_mission_id,
		split_part($1, '_', 2)::s2_product_level,
		-- the first AT tells postgres to interpret the input string as timestamp in utc
		-- the second converts the resulting timestamp to utc
		to_timestamp(
			regexp_replace(t, 'T', ''),
			'YYYYMMDDHHMISS'
		) at time zone 'utc' at time zone 'utc',
		split_part($1, '_', 4)::int,
		split_part($1, '_', 5)::int
	)::bigearthnet_patch_name
	from fixed_seconds_overflow;
$$;

comment on function text_to_bigearthnet_patch_name is 'Conver the name of BigEarthNet patch
to the domain type `bigearthnet_patch_name`.
The function will also fix bugs from the BigEarthNet v1 dataset.
See the domain comment for more details! 
';

--- Start of code that exists to derive the original patch locations from v1
--- to ensure that the annotated cloudy/snowy patches can be re-used.
--- # To correctly separate the original V1 logic with the `pipeline` logic,
--- # I should consider adding these parts to a separate schema that 'hides' these
--- # artifact generator functions/logic.


-- simply drag the patch_name data along, as I never know when this could
-- be useful 
create table bigearthnet_v1_patches (
	patch_name text check (patch_name ~ 'S2._MSIL2A_.*_\d\d?_\d\d?$'),
	band text check (band ~ 'B[018][1-9A]'),
	patch_name_data bigearthnet_patch_name generated always as (text_to_bigearthnet_patch_name(patch_name)) stored,
	rast raster,
	primary key (patch_name, band)
);

comment on table bigearthnet_v1_patches is 'Table for the BigEarthNetv1 patches.
This table should be filled from the associated nushell script.
';

-- No need to duplicate the patch_name_data
-- Just copy the sensing_time and then it can also be used
-- to auto-generate the season column!
create table bigearthnet_v1_patches_geoms (
	patch_name text primary key,
	geom geometry check (st_area(geom) = (1200 * 1200)), --hardcoded for now
	sensing_time timestamptz,
	season text generated always as (timestamptz_to_meteorological_season(sensing_time)) stored
);

comment on table bigearthnet_v1_patches_geoms is 'BigEarthNet v1 table
that should be used to extract the necessary metadata for the v2 pipeline.
The table should be filled via the `fill_bigearthnet_v1_patches_geoms` function
and the metadata should be extracted via the associated nushell script.';

create function fill_bigearthnet_v1_patches_geoms ()
	returns void
	language plpgsql
as $$
begin
	insert into bigearthnet_v1_patches_geoms (patch_name, sensing_time, geom)
		select distinct on (patch_name)
			patch_name,
			(patch_name_data).sensing_time,
			st_polygon(rast) as geom
		from bigearthnet_v1_patches
		order by
			patch_name,
			band
		on conflict do nothing;
end
$$;

comment on function fill_bigearthnet_v1_patches_geoms is 'Fill the table `bigearthnet_v1_patches_geoms`
with the data derived from the `bigearthnet_v1_patches` table.
This function will only consider a single band per patch_name and will take the first patch_name
ordered by the band data. If there is a conflict nothing will be done.
As we are extracting the geographical information, we only need a single band, as all bands
cover the exact same region.
';

create table bigearthnet_countries (
	name text primary key
);

insert into bigearthnet_countries (name)
values 
	('Austria'),
	('Belgium'),
	('Finland'),
	('Ireland'),
	('Kosovo'),
	('Lithuania'),
	('Luxembourg'),
	('Portugal'),
	('Serbia'),
	('Switzerland');

comment on table bigearthnet_countries is 'The countries are listed in the original
BigEarthNet publication. 
The english names are used to describe them.
';


create function tile_patches_geometries_distance_to_closest_ben_country () 
	returns table(patch_id patch_identifier, country bigearthnet_countries.name%type, dist float)
	language plpgsql
as $$
	declare
		inserted_rows integer;
		epsgs int[];
		epsg int;
begin
	create temporary table __ben_countries
	on commit drop -- a function is executed as a single transaction block!
	as (
		select bc.name, c.wkb_geometry as geom
		from ne_admin_0_countries c, bigearthnet_countries bc
		where c.name_long = bc.name
	);

	select array_agg(distinct st_srid(tp.geom)) from tile_patches_geometrical tp into strict epsgs;

	if epsgs is not null then
		foreach epsg in array epsgs
		loop
			raise notice 'Creating gist index tile_patches_geometrical_geom_% if not exists', epsg;
			execute format(
				'create index if not exists tile_patches_geometrical_geom_%s on tile_patches_geometrical using gist(geom) where st_srid(geom) = %s', 
				epsg, 
				epsg
			);
			raise notice 'Creating gist index __ben_countries_geom_%s if not exists', epsg;
			execute format(
				'create index if not exists __ben_countries_geom_%s on __ben_countries using gist(st_transform(geom, %s))', 
				epsg, 
				epsg
			);
		end loop;
	end if;
	analyze tile_patches_geometrical;
	analyze __ben_countries;


	-- An EXECUTE with a simple constant command string and some USING parameters, 
	-- as in the first example above, is functionally equivalent to just writing 
	-- the command directly in PL/pgSQL and allowing replacement of PL/pgSQL variables
	-- to happen automatically. The important difference is that EXECUTE will re-plan 
	-- the command on each execution, generating a plan that is specific to the current
	-- parameter values; whereas PL/pgSQL may otherwise create a generic plan and 
	-- cache it for re-use. In situations where the best plan depends strongly on 
	-- the parameter values, it can be helpful to use EXECUTE to positively ensure 
	-- that a generic plan is not selected.
	if epsgs is not null then
		foreach epsg in array epsgs
		loop
			raise notice 'Calculating the nearest neighbor for %s', epsg;
			return query execute format('
				with tile_patches_geometrical_subset as (
					select *
					from tile_patches_geometrical
					where st_srid(geom) = $1
				)
				select p.patch_id, country.name as country, dist
					from tile_patches_geometrical_subset p
				cross join lateral (
					select bc.name, p.geom <-> st_transform(bc.geom, $1) as dist
					from __ben_countries bc
					order by dist
					limit 1
				) country;
			') using epsg;
		end loop;
	end if;
	raise notice 'Finished calculating the neighbors, about to transfer results';
	return ;
end
$$;

comment on function tile_patches_geometries_distance_to_closest_ben_country is 'Returns the
distance from each patch from tile_patches_geometry to the closest BigEarthNet country, defined
by the shape of the country table `ne_admin_0_countries` filtered by the country names in `bigearthnet_countries`.
Requires that the table `ne_admin_0_countries` was previously uploaded to the DB!
After calling the function the output should be filtered/aligned to the relevant/valid patches
and ordered! Executing the function should only take about 30sek for the entire area!
';

--- Until here

-- should get the data via the parquet file
-- in the future it should depent on the user provided table!
create table bigearthnet_patches_alignment (
	patch_name text primary key,
	geom geometry check (st_area(geom) = (1200 * 1200) and st_srid(geom) <> 0), --hardcoded for now
	sensing_time timestamptz,
	season text generated always as (timestamptz_to_meteorological_season(sensing_time)) stored
);

create function add_bigearthnet_patches_alignment_indexes ()
	returns void
	language plpgsql
as $$
declare
		epsgs int[];
		epsg int;
begin
	-- future: Adapt the function to use allow for 'user' table
	create index if not exists bigearthnet_patches_alignment_srid on bigearthnet_patches_alignment
		using btree((st_srid(geom)));
	create index if not exists bigearthnet_patches_alignment_season on bigearthnet_patches_alignment
		using btree(season);
	-- The most important use of this table is to 'filter' the patches from the S2 rasters
	-- by these geometries with the same CRS and season.
	-- The by far best performance is achieved with a 'global' gist
	create index if not exists bigearthnet_patches_alignment_geom on bigearthnet_patches_alignment
		using gist(geom);
	-- the following partial indexes are could be removed but they might help with
	-- self-'validation' tests, later. So keeping them around for now 
	select array_agg(distinct st_srid(geom)) from bigearthnet_patches_alignment into strict epsgs;
	if epsgs is not null then
		foreach epsg in array epsgs
		loop
			execute format('create index if not exists bigearthnet_patches_alignment_idx_%s on bigearthnet_patches_alignment using gist(geom) where st_srid(geom) = %s', epsg, epsg);
		end loop;
	end if;
	analyze bigearthnet_patches_alignment;
end
$$;

comment on function add_bigearthnet_patches_alignment_indexes is 'Add smart selection of indexes
on add_bigearthnet_patches_alignment table. Should allow for efficient querying of the table!
';

create table tile_patches_validation_and_alignment_result (
	patch_id patch_identifier primary key,
	alignment_reference text
);

comment on table tile_patches_validation_and_alignment_result is 'Table that contains a list of
valid patch identfiers. Here, valid means that it does not contain
any invalid pixels and that the patches are aligned with the bigearthnet_patches_alignment table.
Needs to be filled by calling the fill function.';

create or replace function fill_tile_patches_validation_and_alignment_result ()
	returns void
	language plpgsql
as $$
begin
	drop index if exists tile_patches_validation_and_alignment_result_idx;
	perform add_bigearthnet_patches_alignment_indexes();
	insert into tile_patches_validation_and_alignment_result (patch_id, alignment_reference)
		with invalid_geoms as (
			select l2a_product_id, st_union(geom) as geom
			from sentinel2_invalid_regions
			group by l2a_product_id  -- over entire tile and all (spectral) bands
		), valid_patches as (  -- before: 1035125, after filtering out invalid: 1007612
			select tp.*
			from tile_patches_geometrical tp
				-- need to keep those that are completely 'valid'!
				left join invalid_geoms on
					(patch_id).product_id = invalid_geoms.l2a_product_id
			where
				invalid_geoms.l2a_product_id is null -- if a tile does not contain any invalid components
				or not (
					st_intersects(invalid_geoms.geom, tp.geom)
					and not st_touches(invalid_geoms.geom, tp.geom)
				)
				-- or not st_covers(invalid_geoms.geom, tp.geom) <- Wrong! Single intersection is sufficient!
		)
		select vp.patch_id, bp.patch_name as alignment_reference
			from bigearthnet_patches_alignment bp, valid_patches vp
				where st_covers(vp.geom, bp.geom)
					and st_srid(bp.geom) =  st_srid(vp.geom) 
					and bp.season = vp.season;
	analyze tile_patches_validation_and_alignment_result;
end
$$;

--comment on table tile_patches_validation_and_alignment_result is 'A table that
--returns the bigearthnet patch name called `alignment_reference` and
--the patch_id from the `tile_patches_geometrical` that are aligned with
--(covered by) the corresponding patch name from the same season and are also valid (contain no
--nodata regions).';

-- This could also be a simple view.
create function patches_to_old_bigearthnet_name ()
	returns table(patch_id text, s2v1_name text)
	stable
	language sql
as $$
	-- Here I am also using the dynamic pad-size strategy to double check
	-- if it is working as expected. Even if this makes the query considerably
	-- slower
	with res as (
		select *
		from tile_patches_validation_and_alignment_result
	)
	select patch_identifier_to_text(
			patch_id,
			derive_s2_tile_patches_pad_size(1200) 
			-- hard-coded to 1200 as other values wouldn't make any sense as the patches
			-- would then never overlap
		) as patch_id, alignment_reference as s2v1_name
	from res
$$;

comment on function patches_to_old_bigearthnet_name is 'Function that maps the new patch id to the
associated bigearthnet patch. Derives the data from tile_patches_validation_and_alignment result and format
the new patch id with the correct padding.
';

-- The materialization and ordering would take very long
-- instead, create a simple function that returns the converted tile
-- The DB is smart enough to not materialize the entire result if no
-- ordering is required!
-- TODO: Check if this still works!
create function exportable_aligned_named_patches (
	id sentinel2_l2a_rasters.l2a_product_id%type
)
	returns table(file_name text, bytes_hex text)
	stable
	language sql
as $$
	-- take special care with the function name! Due to the lateral keyword additional columns
	-- that might have the same name as one of the function arguments which leads to the
	-- argument being ignored! This is a super annoying bug and leads to 'all' values being returned!
	with r as (
		select p.l2a_patch_id, p.pretty_patch_name, p.pretty_band_name, p.rast
		from sentinel2_l2a_rasters s,
			lateral sentinel2_l2a_rasters_to_named_patches(s.*, 1200) as p
		where (p.l2a_patch_id).product_id = id
			and p.l2a_patch_id in (select patch_id from tile_patches_validation_and_alignment_result)
	)
	select pretty_patch_name || '_' || pretty_band_name || '.tiff' as file_name,
	encode(
		st_astiff(
			rast,
			-- https://gdal.org/drivers/raster/gtiff.html#raster-gtiff
			-- deflate: 1123
			-- lzw: 1583
			-- none: 15'000
--			array['COMPRESS=deflate', 'PREDICTOR=2', 'NUM_THREADS=4']
			array['COMPRESS=none']
		)
	, 'hex') -- base64 adds line breaks...
	as bytes_hex
	from r
$$;

create table tile_patches_geometrical_u2018_overlay_result  (
	patch_id patch_identifier,
	clc_id text, -- clc
	clc_code_18 clc_code_18,
	geom_id int, -- geom_id with geom or patch_id should be unique!
	geom geometry check (st_area(geom) > 0 and st_isvalid(geom) and st_srid(geom) <> 0),-- intersection with valid geometry
	primary key (patch_id, geom_id)
);

comment on table tile_patches_geometrical_u2018_overlay_result is 'CLC2018 overlay results
on a given Sentinel2-L2A tile. The resulting table should contain the original patch_id,
the clc_id and clc_code_18 (could be dropped but this is a bit easier) and the
geometry with a geom_id. This geom_id should uniquely identify each geometry that is
associated to a given patch_id. The id should be generated by partitioning the data
on the patch_id and then ordering by the clc_id, clc_code_18, and geom to have a stable result.
It should NOT use a global sequence, as this table will be written to in parallel as the
tiles will be processed in parallel!';

-- Drops those that contain NO overlay!
create function tile_patches_geometrical_u2018_overlay(ref_l2a_product_id sentinel2_l2a_rasters.l2a_product_id%type)
	returns void
	language plpgsql
as $$
	declare 
		n_rows int;
		ref_epsg int;
begin
	-- USES tile_patches_validation_and_alignment_result for filtering
	-- USES tile_patches_geometrical for geometries!
	-- USES sentinel2_l2a_rasters to quickly derive st_srid()

	-- making the code below a bit more readable by deriving the epsg of all patches
	-- from the tile once here. Using this as a constant should help with optimizations for
	-- the st_transform functions as well
	-- epsg is derived from the tile which was used to generate the tile_patches_geometrical
	-- these CANNOT differ!
	select st_srid(rast) from sentinel2_l2a_rasters slar 
		where l2a_product_id = ref_l2a_product_id
			and dataset_name like '60m%' into strict ref_epsg;

	create temporary table __sub_clc
	on commit drop
	as (
		-- generating the union is fairly fast and allows for quick filtering in the next step
		with tp_union as (
			select st_union(p.geom) as geom
			from tile_patches_geometrical p
				 where (p.patch_id).product_id = ref_l2a_product_id
					and p.patch_id in (select patch_id from tile_patches_validation_and_alignment_result)
		), exp_u as (
			-- dumping is important to minimize the bounding box size of multi-polygons!
			select u.id, u.code_18, st_transform((st_dump(u.geom)).geom, ref_epsg) as geom
			from U2018_CLC2018_V2020_20u1 u
		)
		select exp_u.*
		from exp_u, tp_union
			where st_intersects(exp_u.geom, tp_union.geom)
	);
	get diagnostics n_rows = ROW_COUNT;
	if n_rows = 0 then
		raise info 'No overlaps between the CLC2018 geometries and the patches with the given tile'
			using hint = ('Make sure that you have correctly creates the patches table and that the tile name % is correct!', ref_l2a_product_id);
		return;
	end if;
	raise info 'Number of CLC2018 geometries that intersect with the patches with the given reference tile: %', n_rows;
	
	create index __sub_clc_gist on __sub_clc using gist(geom);
	analyze __sub_clc;
	

	raise notice 'about to create temporary table with overlay results';

	create temporary table __overlay_result
	on commit drop
	as (
		select clc.id, clc.code_18, p.patch_id, st_intersection(clc.geom, p.geom) as geom
		from __sub_clc clc
			inner join tile_patches_geometrical p
			on st_intersects(p.geom, clc.geom) -- <- THIS MUST EXIST! otherwise, it takes forever, the simple && is not sufficient!
				and not st_touches(p.geom, clc.geom)
		where (p.patch_id).product_id = ref_l2a_product_id
			and patch_id in (select patch_id from tile_patches_validation_and_alignment_result)
		--			and st_relate(tp.geom, u.geom, '2********')
	);

	raise notice 'about to add final results to table';

	-- TODO: Think about the patches that are dropped because they do not contain ANY
	-- label information. Do I want to drop them fully?
	-- but it should also be noted that the result of the rasterization itself also produces
	-- patches that ONLY contain UNLABELED areas even if there was a geometry that had a label in the patch
	-- from my current tests, it looks like 4 patches are unlabeled.
	insert into tile_patches_geometrical_u2018_overlay_result (patch_id, clc_id, clc_code_18, geom_id, geom)
	with clc_u_geoms as (
		select patch_id, st_union(geom) as geom
		from __overlay_result
		group by patch_id
	), diff as (
		select tpg.patch_id, st_difference(tpg.geom, clc.geom) as geom
		from tile_patches_geometrical tpg
			join clc_u_geoms clc on clc.patch_id = tpg.patch_id
	), patch_areas_with_unavailable_clc_data as (
		select patch_id, geom
		from diff
		where st_area(geom) > 0
	), unordered_result as (
		select id as clc_id, code_18 as clc_code_18, patch_id, (st_dump(geom)).geom as geom
		from __overlay_result
		union
		select null as clc_id, '999' as clc_code_18, patch_id, (st_dump(geom)).geom as geom
		from patch_areas_with_unavailable_clc_data
	) select
		patch_id, clc_id, clc_code_18, row_number() over (partition by patch_id order by clc_id, clc_code_18, geom) as geom_id, geom
		from unordered_result
		order by patch_id, clc_id, clc_code_18, geom_id;
end
$$;

-- TODO: Think about if this should be a view or not!
-- The view could drag along the tile name to the output
-- which would allow for simple filtering of the resulting query afterwards
-- and, hopefully, optimized by the engine
-- Decided against it. In the current version, I still need access to the
-- parameterized square_side_length_in_m call
create function tile_patches_geometrical_u2018_overlay_rasterize(
	ref_l2a_product_id sentinel2_l2a_rasters.l2a_product_id%type,
	square_side_length_in_m integer default 1200	
)
	returns table(patch_id patch_identifier, rast raster)
--	immutable
	stable
	strict
--	parallel safe
	language sql
as $$
	with s as (
		select *
		-- Do not forget we have multiple rasters per patch-id!
		-- the reference patches should have 10m resolution, could be configurable by user
		-- it is just important that those pixel align with the segmentation maps
		from sentinel2_l2a_rasters s
			where s.l2a_product_id = ref_l2a_product_id
				and dataset_name like '10m%' 
	), rasters as (
		select (sentinel2_l2a_product_to_patches(s.*, square_side_length_in_m)).*
		from s
	), res as (
	-- You generally pass this reference raster by 
	-- joining the table containing the geometry with the table containing the reference raster.
	-- The optional touched parameter defaults to false and maps to the GDAL ALL_TOUCHED 
	-- rasterization option, which determines if pixels touched by lines or polygons will be burned.
	-- Not just those on the line render path, or whose center point is within the polygon.
	-- https://gdal.org/programs/gdal_rasterize.html
		select st_asraster(
				p.geom,
				r.rast,
	--				'8BUI',
	--				value => clc_code_18_to_8bit_value(code_18)::double precision,
				'16BUI',
				value => p.clc_code_18::double precision,
				touched => false
			) as rast,
			p.patch_id
		from rasters r
	--		where r.patch_id = p_id
		inner join tile_patches_geometrical_u2018_overlay_result p
			on p.patch_id = r.patch_id
	), merged_res as (
		select patch_id,
			st_union(rast) as rast
		from res
		group by patch_id
	), aligned_res as (
		-- I cannot, for the life of me, figure out why the rasterization logic
		-- 'creates' additional columns/rows. The rasters are aligned and I assume
		-- it has something to do with precision errors near the border that lead to additional
		-- pixels to become rastered. Looking at a few samples, it looks like clipping should solve
		-- the problem. So that is what I am doing as a last resort.
		select m.patch_id, st_clip(m.rast, st_envelope(r.rast), true) as rast
		from merged_res m
			inner join rasters r
				on m.patch_id = r.patch_id
	) select patch_id, rast
	from aligned_res;
$$;

create function exportable_rasterized_clc_overlayed_patches (
	id sentinel2_l2a_rasters.l2a_product_id%type,
	square_side_length_in_m integer default 1200
)
	returns table(file_name text, bytes_hex text)
	stable
	language sql
as $$
	with raster_res as (
		select *
		from tile_patches_geometrical_u2018_overlay_rasterize(id, square_side_length_in_m)
	)
	select patch_identifier_to_text(
		patch_id, 
		derive_s2_tile_patches_pad_size(square_side_length_in_m)
	) || '_segmentation.tiff' as file_name,
		encode(st_asgdalraster(
				rast,
				-- st_union(rast),
				'GTiff',
				-- https://gdal.org/drivers/raster/gtiff.html#raster-gtiff
				-- especially for my label data using a compression option
				-- with predictor should have a large effect!
				array['COMPRESS=deflate', 'PREDICTOR=2', 'NUM_THREADS=4']
			)
		, 'hex') -- base64 adds line breaks...
		as bytes_hex
	from raster_res;
$$;

create table tile_patches_u2018_overlay_rasterization_geoms (
	patch_id patch_identifier,
	clc_code_18 clc_code_18,
	-- maybe add id back in the future if necessary for further analysis
	-- but for now, the idea is to use the 'real' geometrical data
	-- and not the rasterized output for further analysis. This is only required
	-- to understand what values have been exported in the segmentation maps
	-- no, I should already use it to ensure that I am not duplicating data!
	geom_id int,
	geom geometry check (st_area(geom) > 0 and st_isvalid(geom) and st_srid(geom) <> 0),
	primary key(patch_id, geom_id)
);
-- would it make sense to add the indexes here before ingesting any data?
-- or would that considerably slow down the (parallel) data ingestion?
-- 

-- TODO: Do not forget! After the segmentation there are only 121 tiles available!
-- See the output of:
-- select distinct( (patch_id).product_id )
-- from tile_patches_geometrical_u2018_overlay_result
-- BUT this assumes that each of the previous steps has finished!
-- -> It is fine to call this function even with the 'empty' tiles as they simply add 0 rows.
-- With 16 workers it took around 10min to write the table from all workers.
create or replace function fill_tile_patches_u2018_overlay_rasterization_geoms(
	id sentinel2_l2a_rasters.l2a_product_id%type
)
	returns void
	language sql
as $$
	insert into tile_patches_u2018_overlay_rasterization_geoms (patch_id, clc_code_18, geom_id, geom)
		with raster_result as (
			select patch_id, (st_dumpaspolygons(rast, exclude_nodata_value => false)).*
			from tile_patches_geometrical_u2018_overlay_rasterize(id)
		), unordered_result as (
			select patch_id, val::int::text::clc_code_18 as clc_code_18, geom
			from raster_result
		) select patch_id, clc_code_18,
			row_number() over (partition by patch_id order by clc_code_18, geom) as geom_id, 
			geom
		from unordered_result;
$$;

-- Could also be cross-linked to the tile_patches_u2018_overlay_rasterization_geoms
-- but that would require an intermediate table... Same for lbl_19.
create table ben19_patch_label_table (
	patch_id patch_identifier,
	lbl_19 text,
	primary key (patch_id, lbl_19)
);

comment on table ben19_patch_label_table is 'Table that contains the mapping
from the `patch_id` to the 19-class nomenclature label.
This table is only filled with samples that are ABOVE the minimal
label-area threshold and will be part of the classification dataset!
';

create function update_ben19_patch_label_table ()
	returns void
	language plpgsql
as $$
begin
	truncate ben19_patch_label_table;
	-- NEAR-FUTURE: Either encode the threshold directly
	-- within the table, or pass it as a value and truncate
	insert into ben19_patch_label_table (patch_id, lbl_19)
		with ben_labels as (
			select o.patch_id, c.ben19_name, o.geom
			from tile_patches_u2018_overlay_rasterization_geoms o
				join clc_code_18_to_ben19_name c
				on o.clc_code_18 = c.clc_code_18
			where (o.patch_id).product_id not in (
				select l2a_product_id as product_id from sentinel2_product_ids_failing_quality_indicators
			)
		), rel_unlabeled as (
			select patch_id,
				coalesce(
					sum(st_area(geom)) filter (where ben19_name = 'UNLABELED')
				,0) / sum(st_area(geom)) as relative_unlabeled_area
			from ben_labels
			group by patch_id
		), filtered as (
			select b.*
			from ben_labels b
				join rel_unlabeled u
				on b.patch_id = u.patch_id
				where u.relative_unlabeled_area < 0.25 -- invert and ) select count(*), st_simplify(st_union(st_transform(geom, 3035)), 10) -- quick visualization of dropped areas from filtered
		), agg_lbls as (
			select patch_id, array_agg(distinct ben19_name) as lbls_19
			from filtered
			where ben19_name <> 'UNLABELED'
			group by patch_id
		) select patch_id, unnest(lbls_19) as lbl_19
		from agg_lbls
		order by patch_id, lbl_19;
end
$$;


-- -> This was waaaay toooo sloooow. Moved to a client-side call...
-- Could think again about generating the initial raster results via plpgsql as this
-- shouldn't take too much memory... I think
-- but keeping it as a psql client-side code for now!

-- this should be possible as a SQL function because I just need to join
-- based on the patch_id and that could be row-wise filtering
-- The main reason why we do NOT want to use plgpsql
-- is that it would materialize ALL of the raster data in memory/on-disk
-- before sending it to the client. So the function MUST be a pure SQL
-- function such that the output can be streamed from psql
-- This also means that the dynamic table name is causing issues.
-- One solution would be to dynamically create functions (messy), use
-- cursor function (complicated), or to externalize some logic to the client
-- Here, I keep most of the logic in the DB for the rasterization although
-- the filtering is worse than a 'correct' join-operation.
-- The final rasterization & exporting logic needs to be handled by the client
-- as it requires the name of the table! So some logic had to be externalized...
-- in the previous version I used to convert the clc values down to an 8BUI version
-- to further compress the image. But this just makes it more complicated and the user
-- should ideally convert the data to a more optimized format either way.
-- I will keep the compression but move back to uint16

--create function raster_tile_patch_overlay (p_id patch_identifier, code_18 clc_code_18, geom geometry)
--	returns raster
--	stable
--	parallel safe
--	language sql
--as $$
--	with s as (select *
--		-- Do not forget we have multiple rasters per patch-id!
--		-- the reference patches should have 10m resolution, could be configurable by user
--		-- it is just important that those pixel align with the segmentation maps
--		from sentinel2_l2a_rasters s
--			where dataset_name like '10m%' 
--	), rasters as (
--		select (sentinel2_l2a_product_to_patches(s.*, 1200)).*
--		from s
--	)
--	-- You generally pass this reference raster by 
--	-- joining the table containing the geometry with the table containing the reference raster.
--	-- The optional touched parameter defaults to false and maps to the GDAL ALL_TOUCHED 
--	-- rasterization option, which determines if pixels touched by lines or polygons will be burned.
--	-- Not just those on the line render path, or whose center point is within the polygon.
--	-- https://gdal.org/programs/gdal_rasterize.html
--		select st_asraster(
--				geom,
--				r.rast,
----				'8BUI',
----				value => clc_code_18_to_8bit_value(code_18)::double precision,
--				'16BUI',
--				value => code_18::double precision,
--				touched => false
--			) as rast
--		from rasters r
--		where r.patch_id = p_id
----		inner join overlayed_patches p
----			on p.patch_id = r.patch_id
--$$;


