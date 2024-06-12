begin;

select plan(6);

select results_eq(
    $$
    select count(*)
	from sentinel2_l2a_metadata
	where jsonb_path_exists(metadata, 'strict $.**.SENSOR_QUALITY ? (@ == "PASSED")')
    $$, 
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'Sensor quality always PASSED'
);

select results_eq(
    $$
    select count(*)
	from sentinel2_l2a_metadata
	where jsonb_path_exists(metadata, 'strict $.**.GENERAL_QUALITY ? (@ == "PASSED")')
    $$, 
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'General quality always PASSED'
);

select results_eq(
    $$
    select count(*)
	from sentinel2_l2a_metadata
	where jsonb_path_exists(metadata, 'strict $.**.GEOMETRIC_QUALITY ? (@ == "PASSED")')
    $$, 
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'Geometric quality always PASSED'
);

select results_eq(
    $$
    select count(*)
	from sentinel2_l2a_metadata
	where jsonb_path_exists(metadata, 'strict $.**.FORMAT_CORRECTNESS ? (@ == "PASSED")')
    $$, 
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'Format correctness always PASSED'
);

select results_eq(
    $$
    select count(*)
	from sentinel2_l2a_metadata
	where jsonb_path_exists(metadata, 'strict $.**.RADIOMETRIC_QUALITY ? (@ == "PASSED")')
    $$, 
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'Radiometric quality always PASSED'
);

select results_eq(
    $$
    select count(*)
    from sentinel2_l2a_metadata
    where jsonb_path_exists(metadata, 'strict $.**.DEGRADED_MSI_DATA_PERCENTAGE ? (@ == "0")')
    $$,
    $$
    select count(*)
	from sentinel2_l2a_metadata
    $$,
    'Degraded MSI Data percentage always 0'
);

select * from finish();

rollback;
