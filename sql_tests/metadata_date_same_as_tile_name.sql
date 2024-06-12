begin;

select plan(1);

-- Ensure that datetime from name is IDENTICAL to data from PRODUCT_START_TIME!
-- The date is just the truncated version of the seconds!
select results_eq(
    $$
	select count(*) 
	from sentinel2_l2a_rasters where
		abs(
			extract(
				epoch from (
					to_timestamp(
					metadata -> 'metadata' -> '' ->> 'PRODUCT_START_TIME', 
					'YYYY-MM-DD"T"HH:MI:SS.MS"Z"'
					) at time zone 'UTC'
					-
					split_part(l2a_product_id, '_', 3)::timestamptz at time zone 'UTC'
				)
			)
		) > 1;
    $$,
    'VALUES (0::bigint)',
    'Datetime from name is IDENTICAL to data from PRODUCT_START_TIME!'
);

select * from finish();

rollback;
