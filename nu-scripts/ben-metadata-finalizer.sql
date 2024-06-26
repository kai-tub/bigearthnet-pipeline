-- This script takes the generated metadata of the data-generator step
-- and aligns and processes it, while ensuring that the input data has the correct format
-- and that the results are non-zero.
-- It requires that the metadata (csv) files are in a findable path for the duckdb process and
-- that the following names are used, where 
-- the search path can be defined via the environment variable `BEN_UNALIGNED_METADATA_DIR`).
-- 
-- + `patch_id_label_mapping.csv`
-- + `patch_id_s2v1_mapping.csv`
-- + `old_s1s2_mapping`
-- + `patch_id_split_mapping.csv`
-- + `patch_id_country_mapping.csv`
-- + `patches_with_cloud_and_shadow.csv`
-- + `patches_with_seasonal_snow.csv`
-- 
-- The output will be two parquet files that are written to the CWD
--
-- To understand the syntax & tricks check the following links:
-- https://duckdb.org/docs/sql/query_syntax/from.html#from-first-syntax
-- https://duckdb.org/2024/03/01/sql-gymnastics.html#creating-version-2-of-the-macro
-- about why the csv files cannot be set dynamically see: https://github.com/duckdb/duckdb/issues/12399
.bail on

create macro datadir () as coalesce(getenv('BEN_UNALIGNED_METADATA_DIR').nullif(''), '.') || '/';
set file_search_path = datadir();
select current_setting('file_search_path');

create or replace macro non_zero_cte_check (error_msg) as table (
	from any_cte
	select case
		when count(*) = 0 then
			error(error_msg)
		else
			'non-zero test passed'
	end as result
);

-- with any_cte as (
-- 	select 1 limit 0
-- )
-- select *
-- from non_zero_cte_check('select 1 failed!');

create temporary table patch_id_label_mapping (patch_id varchar, label varchar, primary key (patch_id, label));
insert into patch_id_label_mapping by name (select * from read_csv('patch_id_label_mapping.csv'));
with any_cte as (
	from patch_id_label_mapping
)
select *
from non_zero_cte_check('patch_id_label_mapping is empty!');

drop table patch_id_s2v1_mapping ;
create temporary table patch_id_s2v1_mapping (patch_id varchar primary key, s2v1_name varchar not null unique);
-- could've also used copy instead of insert into by name
insert into patch_id_s2v1_mapping by name (select * from read_csv('patch_id_s2v1_mapping.csv'));
with any_cte as (
	from patch_id_s2v1_mapping
)
select *
from non_zero_cte_check('patch_id_s2v1_mapping is empty!');

create temporary table old_s1s2_mapping (s2v1_name varchar primary key, s1_name varchar not null unique);
insert into old_s1s2_mapping (
	select *
	from read_csv(
		'old_s1s2_mapping.csv',
		header=false,
		names=['s2v1_name', 's1_name']
	)
);
with any_cte as (
	from old_s1s2_mapping 
)
select *
from non_zero_cte_check('old_s1s2_mapping is empty!');

create temporary table patch_id_split_mapping (patch_id varchar primary key, split varchar check (split in ('train', 'validation', 'test')));
insert into patch_id_split_mapping by name (select * from read_csv('patch_id_split_mapping.csv'));
with any_cte as (
	from patch_id_split_mapping
)
select *
from non_zero_cte_check('patch_id_split_mapping is empty!');

create temporary table patch_id_country_mapping (patch_id varchar primary key, country varchar);
insert into patch_id_country_mapping by name (select * from read_csv('patch_id_country_mapping.csv'));
with any_cte as (
	from patch_id_country_mapping
)
select *
from non_zero_cte_check('patch_id_country_mapping is empty!');

create temporary table s2v1_patches_with_cloud_and_shadow (s2v1_name varchar primary key);
insert into s2v1_patches_with_cloud_and_shadow (select * from read_csv('patches_with_cloud_and_shadow.csv', header=false, names=['s2v1_name']));
with any_cte as (
	from s2v1_patches_with_cloud_and_shadow 
)
select *
from non_zero_cte_check('s2v1_patches_with_cloud_and_shadow is empty!');

create temporary table s2v1_patches_with_seasonal_snow (s2v1_name varchar primary key);
insert into s2v1_patches_with_seasonal_snow (select * from read_csv('patches_with_seasonal_snow.csv', header=false, names=['s2v1_name']));
with any_cte as (
	from s2v1_patches_with_seasonal_snow
)
select *
from non_zero_cte_check('s2v1_patches_with_seasonal_snow is empty!');

create temporary table labeled_patch_ids (patch_id varchar primary key);
insert into labeled_patch_ids by name (
	select distinct(patch_id) as patch_id
	from patch_id_label_mapping
);
with any_cte as (
	from labeled_patch_ids 
)
select *
from non_zero_cte_check('labeled_patch_ids is empty!');

create temporary table result (
	patch_id varchar primary key,
	labels varchar[] not null,
	split varchar not null,
	country varchar not null,
	s1_name varchar not null,
	s2v1_name varchar not null,
	contains_seasonal_snow boolean not null,
	contains_cloud_or_shadow boolean not null,
);

insert into result by name (
	with seasonal_snow_patch_ids as (
		select p.patch_id
		from s2v1_patches_with_seasonal_snow s
		join patch_id_s2v1_mapping p
		on s.s2v1_name = p.s2v1_name
	), cloud_and_shadow_patch_ids as (
		select p.patch_id
		from s2v1_patches_with_cloud_and_shadow c
		join patch_id_s2v1_mapping p
		on c.s2v1_name = p.s2v1_name
	), grouped_labels as (
		select patch_id, list(label order by label) as labels
		from patch_id_label_mapping
		group by patch_id
	) select *,
		patch_id in (select patch_id from seasonal_snow_patch_ids) as contains_seasonal_snow,
		patch_id in (select patch_id from cloud_and_shadow_patch_ids) as contains_cloud_or_shadow
	from grouped_labels
	join patch_id_split_mapping
		using (patch_id)
	join patch_id_s2v1_mapping
		using (patch_id)
	join patch_id_country_mapping
		using (patch_id)
	join old_s1s2_mapping
		using (s2v1_name)
);

with any_cte as (
	from result
) select *
from non_zero_cte_check('result is empty!');

select case
	when (
		select count(*) from result
	) <> (
		select count(distinct patch_id) from patch_id_label_mapping
	) then
		error('The result does not have the same length as the agg. label patch-ids!')
	else
		'result length test passed'
	end as result;

copy (
	select *
	from result
	where
		(not contains_seasonal_snow) and (not contains_cloud_or_shadow)
	order by patch_id
) to 'metadata.parquet';

copy (
	select *
	from result
	where
		contains_seasonal_snow or contains_cloud_or_shadow
	order by patch_id
) to 'metadata_for_patches_with_snow_cloud_or_shadow.parquet';

