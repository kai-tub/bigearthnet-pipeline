begin;

select plan(9);

prepare parsed_patch_name as select * 
	from text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_59_21');

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_59_21')).mission_id, 
	'S2B', 
	'mission_id!'
);

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_59_21')).product_level, 
	'MSIL2A', 
	'product_level'
);

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_09_21')).x, 
	9, 
	'xpos leading 0'
);

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_9_21')).x, 
	9, 
	'xpos single digit'
);

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_09_09')).y, 
	9, 
	'ypos leading 0'
);

select IS(
	(text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095409_9_9')).y, 
	9,
	'ypos single digit'
);

select results_eq(
	$$select
		extract(year from sensing_time),
		extract(month from sensing_time),
		extract(day from sensing_time),
		extract(hour from sensing_time),
		extract(minute from sensing_time),
		extract(second from sensing_time)
	from text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T095408_9_9')$$, 
	$$VALUES (
		2017::numeric, 
		12::numeric, 
		19::numeric,
		9::numeric,
		54::numeric,
		8::numeric) $$,
	'Date parsing'
);

select results_eq(
	$$select
		extract(hour from sensing_time),
		extract(minute from sensing_time),
		extract(second from sensing_time)
	from text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T95408_9_9')$$, 
	$$VALUES (
		9::numeric,
		54::numeric,
		8::numeric) $$,
	'Date parsing missing hour prefix'
);

select results_eq(
	$$select
		extract(second from sensing_time)
	from text_to_bigearthnet_patch_name('S2B_MSIL2A_20171219T95461_9_9')$$, 
	$$VALUES (
		59::numeric) $$,
	'Limit second overflow'
);

select * from finish();

rollback;
