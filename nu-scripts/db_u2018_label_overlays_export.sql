-- psql script that exports the rastered segementation maps
-- of the clc-overlayed patches.
-- the output contains <patch_name>_segmentation.tiff, <HEX_CODED_RASTER>

set min_parallel_table_scan_size to 0;
set min_parallel_index_scan_size to 0;
set parallel_setup_cost to 0.0;
set parallel_tuple_cost to 0.0;

create temp view __overlay_rasters as 
	with s as (
		select *
		-- Do not forget we have multiple rasters per patch-id!
		-- the reference patches should have 10m resolution, could be configurable by user
		-- it is just important that those pixel align with the segmentation maps
		from sentinel2_l2a_rasters s
			where dataset_name like '10m%' 
	), rasters as (
		select (sentinel2_l2a_product_to_patches(s.*, 1200)).*
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
				value => p.code_18::double precision,
				touched => false
			) as rast,
			p.patch_id
		from rasters r
	--		where r.patch_id = p_id
		inner join :"overlay_table" p
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
	) select patch_identifier_to_text(patch_id, 2) || '_segmentation.tiff' as file_name,
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
	from aligned_res;
	-- limit 3;

-- no header to allow line streaming-based parsing of output
\copy (select * from __overlay_rasters) to pstdout with delimiter '|';

set min_parallel_table_scan_size to default;
set min_parallel_index_scan_size to default;
set parallel_setup_cost to default;
set parallel_tuple_cost to default;
