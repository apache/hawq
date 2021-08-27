------------------------------------------------------------------
-- pg_proc
------------------------------------------------------------------
set gen_new_oid_value to 321;
insert into pg_proc values ('json_in',11,10,12,'f','f','t','f','i',1,193,'f','2275',null,null,null,'json_in','-',null,'n');
set gen_new_oid_value to 322;
insert into pg_proc values ('json_out',11,10,12,'f','f','t','f','i',1,2275,'f','193',null,null,null,'json_out','-',null,'n');
set gen_new_oid_value to 323;
insert into pg_proc values ('json_recv',11,10,12,'f','f','t','f','i',1,193,'f','2281',null,null,null,'json_recv','-',null,'n');
set gen_new_oid_value to 324;
insert into pg_proc values ('json_send',11,10,12,'f','f','t','f','i',1,17,'f','193',null,null,null,'json_send','-',null,'n');
set gen_new_oid_value to 3153;
insert into pg_proc values ('array_to_json',11,10,12,'f','f','t','f','s',1,193,'f','2277',null,null,null,'array_to_json','-',null,'n');
set gen_new_oid_value to 3154;
insert into pg_proc values ('array_to_json',11,10,12,'f','f','t','f','s',2,193,'f','2277 16',null,null,null,'array_to_json_pretty','-',null,'n');
set gen_new_oid_value to 3155;
insert into pg_proc values ('row_to_json',11,10,12,'f','f','t','f','s',1,193,'f','2249',null,null,null,'row_to_json','-',null,'n');
set gen_new_oid_value to 3156;
insert into pg_proc values ('row_to_json',11,10,12,'f','f','t','f','s',2,193,'f','2249 16',null,null,null,'row_to_json_pretty','-',null,'n');
set gen_new_oid_value to 3173;
insert into pg_proc values ('json_agg_transfn',11,10,12,'f','f','f','f','i',2,2281,'f','2281 2283',null,null,null,'json_agg_transfn','-',null,'n');
set gen_new_oid_value to 3174;
insert into pg_proc values ('json_agg_finalfn',11,10,12,'f','f','f','f','i',1,193,'f','2281',null,null,null,'json_agg_finalfn','-',null,'n');
set gen_new_oid_value to 3175;
insert into pg_proc values ('json_agg',11,10,12,'t','f','f','f','i',1,193,'f','2283',null,null,null,'aggregate_dummy','-',null,'n');
set gen_new_oid_value to 3176;
insert into pg_proc values ('to_json',11,10,12,'f','f','t','f','s',1,193,'f','2283',null,null,null,'to_json','-',null,'n');

set gen_new_oid_value to 3947;
insert into pg_proc values ('json_object_field',11,10,12,'f','f','t','f','s',2,193,'f','193 25',null,null,'{from_json,field_name}','json_object_field','-',null,'n');
set gen_new_oid_value to 3948;
insert into pg_proc values ('json_object_field_text',11,10,12,'f','f','t','f','s',2,25,'f','193 25',null,null,'{from_json,field_name}','json_object_field_text','-',null,'n');
set gen_new_oid_value to 3949;
insert into pg_proc values ('json_array_element',11,10,12,'f','f','t','f','s',2,193,'f','193 23',null,null,'{from_json,element_index}','json_array_element','-',null,'n');
set gen_new_oid_value to 3950;
insert into pg_proc values ('json_array_element_text',11,10,12,'f','f','t','f','s',2,25,'f','193 23',null,null,'{from_json,element_index}','json_array_element_text','-',null,'n');
set gen_new_oid_value to 3951;
insert into pg_proc values ('json_extract_path',11,10,12,'f','f','t','f','s',2,193,'f','193 1009','{193,1009}','{i,v}','{from_json,path_elems}','json_extract_path','-',null,'n');
set gen_new_oid_value to 3952;
insert into pg_proc values ('json_extract_path_op',11,10,12,'f','f','t','f','s',2,193,'f','193 1009',null,null,'{from_json,path_elems}','json_extract_path','-',null,'n');
set gen_new_oid_value to 3953;
insert into pg_proc values ('json_extract_path_text',11,10,12,'f','f','t','f','s',2,25,'f','193 1009','{193,1009}','{i,v}','{from_json,path_elems}','json_extract_path_text','-',null,'n');
set gen_new_oid_value to 3954;
insert into pg_proc values ('json_extract_path_text_op',11,10,12,'f','f','t','f','s',2,25,'f','193 1009',null,null,'{from_json,path_elems}','json_extract_path_text','-',null,'n');
set gen_new_oid_value to 3955;
insert into pg_proc values ('json_array_elements',11,10,12,'f','f','t','t','s',1,193,'f','193','{193,193}','{i,o}','{from_json,value}','json_array_elements','-',null,'n');
set gen_new_oid_value to 3956;
insert into pg_proc values ('json_array_length',11,10,12,'f','f','t','f','s',1,23,'f','193',null,null,null,'json_array_length','-',null,'n');
set gen_new_oid_value to 3957;
insert into pg_proc values ('json_object_keys',11,10,12,'f','f','t','t','s',1,25,'f','193',null,null,null,'json_object_keys','-',null,'n');
set gen_new_oid_value to 3958;
insert into pg_proc values ('json_each',11,10,12,'f','f','t','t','s',1,2249,'f','193','{193,25,193}','{i,o,o}','{from_json,key,value}','json_each','-',null,'n');
set gen_new_oid_value to 3959;
insert into pg_proc values ('json_each_text',11,10,12,'f','f','t','t','s',1,2249,'f','193','{193,25,25}','{i,o,o}','{from_json,key,value}','json_each_text','-',null,'n');
set gen_new_oid_value to 3960;
insert into pg_proc values ('json_populate_record',11,10,12,'f','f','f','f','s',3,2283,'f','2283 193 16',null,null,null,'json_populate_record','-',null,'n');
set gen_new_oid_value to 3961;
insert into pg_proc values ('json_populate_recordset',11,10,12,'f','f','f','t','s',3,2283,'f','2283 193 16',null,null,null,'json_populate_recordset','-',null,'n');
set gen_new_oid_value to 3968;
insert into pg_proc values ('json_typeof',11,10,12,'f','f','t','f','i',1,25,'f','193',null,null,null,'json_typeof','-',null,'n');
set gen_new_oid_value to 3180;
insert into pg_proc values ('json_object_agg_transfn',11,10,12,'f','f','f','f','i',3,2281,'f','2281 2276 2276',null,null,null,'json_object_agg_transfn','-',null,'n');
set gen_new_oid_value to 3196;
insert into pg_proc values ('json_object_agg_finalfn',11,10,12,'f','f','f','f','i',1,193,'f','2281',null,null,null,'json_object_agg_finalfn','-',null,'n');
set gen_new_oid_value to 3197;
insert into pg_proc values ('json_object_agg',11,10,12,'t','f','f','f','i',2,193,'f','2276 2276',null,null,null,'aggregate_dummy','-',null,'n');
set gen_new_oid_value to 3198;
insert into pg_proc values ('json_build_array',11,10,12,'f','f','f','f','i',1,193,'f','2276','{2276}','{v}',null,'json_build_array','-',null,'n');
set gen_new_oid_value to 3199;
insert into pg_proc values ('json_build_array',11,10,12,'f','f','f','f','i',0,193,'f','',null,null,null,'json_build_array_noargs','-',null,'n');
set gen_new_oid_value to 3500;
insert into pg_proc values ('json_build_object',11,10,12,'f','f','f','f','i',1,193,'f','2276','{2276}','{v}',null,'json_build_object','-',null,'n');
set gen_new_oid_value to 3501;
insert into pg_proc values ('json_build_object',11,10,12,'f','f','f','f','i',0,193,'f','',null,null,null,'json_build_object_noargs','-',null,'n');
set gen_new_oid_value to 3502;
insert into pg_proc values ('json_object',11,10,12,'f','f','t','f','s',1,193,'f','1009',null,null,null,'json_object','-',null,'n');
set gen_new_oid_value to 3503;
insert into pg_proc values ('json_object',11,10,12,'f','f','t','f','s',2,193,'f','1009 1009',null,null,null,'json_object_two_arg','-',null,'n');
set gen_new_oid_value to 3504;
insert into pg_proc values ('json_to_record',11,10,12,'f','f','f','f','s',2,2249,'f','193 16',null,null,null,'json_to_record','-',null,'n');
set gen_new_oid_value to 3505;
insert into pg_proc values ('json_to_recordset',11,10,12,'f','f','f','t','s',2,2249,'f','193 16',null,null,null,'json_to_recordset','-',null,'n');
set gen_new_oid_value to 3969;
insert into pg_proc values ('json_array_elements_text',11,10,12,'f','f','t','t','i',1,25,'f','193','{193,25}','{i,o}','{from_json,value}','json_array_elements_text','-',null,'n');
set gen_new_oid_value to 3506;
insert into pg_proc values ('json_strip_nulls',11,10,12,'f','f','t','f','i',1,193,'f','193',null,null,null,'json_strip_nulls','-',null,'n');
-- jsonb
set gen_new_oid_value to 3806;
insert into pg_proc values ('jsonb_in',11,10,12,'f','f','t','f','i',1,3802,'f','2275',null,null,null,'jsonb_in','-',null,'n');
set gen_new_oid_value to 3805;
insert into pg_proc values ('jsonb_recv',11,10,12,'f','f','t','f','i',1,3802,'f','2281',null,null,null,'jsonb_recv','-',null,'n');
set gen_new_oid_value to 3804;
insert into pg_proc values ('jsonb_out',11,10,12,'f','f','t','f','i',1,2275,'f','3802',null,null,null,'jsonb_out','-',null,'n');
set gen_new_oid_value to 3803;
insert into pg_proc values ('jsonb_send',11,10,12,'f','f','t','f','i',1,17,'f','3802',null,null,null,'jsonb_send','-',null,'n');
set gen_new_oid_value to 3478;
insert into pg_proc values ('jsonb_object_field',11,10,12,'f','f','t','f','i',2,3802,'f','3802 25',null,null,'{from_json,field_name}','jsonb_object_field','-',null,'n');
set gen_new_oid_value to 5509;
insert into pg_proc values ('jsonb_object_field_text',11,10,12,'f','f','t','f','i',2,25,'f','3802 25',null,null,'{from_json,field_name}','jsonb_object_field_text','-',null,'n');
set gen_new_oid_value to 5510;
insert into pg_proc values ('jsonb_array_element',11,10,12,'f','f','t','f','i',2,3802,'f','3802 23',null,null,'{from_json,element_index}','jsonb_array_element','-',null,'n');
set gen_new_oid_value to 5511;
insert into pg_proc values ('jsonb_array_element_text',11,10,12,'f','f','t','f','i',2,25,'f','3802 23',null,null,'{from_json,element_index}','jsonb_array_element_text','-',null,'n');
set gen_new_oid_value to 5512;
insert into pg_proc values ('jsonb_extract_path',11,10,12,'f','f','t','f','i',2,3802,'f','3802 1009','{3802,1009}','{i,v}','{from_json,path_elems}','jsonb_extract_path','-',null,'n');
set gen_new_oid_value to 3939;
insert into pg_proc values ('jsonb_extract_path_op',11,10,12,'f','f','t','f','i',2,3802,'f','3802 1009',null,null,'{from_json,path_elems}','jsonb_extract_path','-',null,'n');
set gen_new_oid_value to 3940;
insert into pg_proc values ('jsonb_extract_path_text',11,10,12,'f','f','t','f','i',2,25,'f','3802 1009','{3802,1009}','{i,v}','{from_json,path_elems}','jsonb_extract_path_text','-',null,'n');
set gen_new_oid_value to 5513;
insert into pg_proc values ('jsonb_extract_path_text_op',11,10,12,'f','f','t','f','i',2,25,'f','3802 1009',null,null,'{from_json,path_elems}','jsonb_extract_path_text','-',null,'n');
set gen_new_oid_value to 5514;
insert into pg_proc values ('jsonb_array_elements',11,10,12,'f','f','t','t','i',1,3802,'f','3802','{3802,3802}','{i,o}','{from_json,value}','jsonb_array_elements','-',null,'n');
set gen_new_oid_value to 3465;
insert into pg_proc values ('jsonb_array_elements_text',11,10,12,'f','f','t','t','i',1,25,'f','3802','{3802,25}','{i,o}','{from_json,value}','jsonb_array_elements_text','-',null,'n');
set gen_new_oid_value to 5502;
insert into pg_proc values ('jsonb_array_length',11,10,12,'f','f','t','f','i',1,23,'f','3802',null,null,null,'jsonb_array_length','-',null,'n');
set gen_new_oid_value to 3931;
insert into pg_proc values ('jsonb_object_keys',11,10,12,'f','f','t','t','i',1,25,'f','3802',null,null,null,'jsonb_object_keys','-',null,'n');
set gen_new_oid_value to 5503;
insert into pg_proc values ('jsonb_each',11,10,12,'f','f','t','t','i',1,2249,'f','3802','{3802,25,3802}','{i,o,o}','{from_json,key,value}','jsonb_each','-',null,'n');
set gen_new_oid_value to 3932;
insert into pg_proc values ('jsonb_each_text',11,10,12,'f','f','t','t','i',1,2249,'f','3802','{3802,25,25}','{i,o,o}','{from_json,key,value}','jsonb_each_text','-',null,'n');
set gen_new_oid_value to 5504;
insert into pg_proc values ('jsonb_populate_record',11,10,12,'f','f','f','f','s',3,2283,'f','2283 3802 16',null,null,null,'jsonb_populate_record','-',null,'n');
set gen_new_oid_value to 3475;
insert into pg_proc values ('jsonb_populate_recordset',11,10,12,'f','f','f','t','s',3,2283,'f','2283 3802 16',null,null,null,'jsonb_populate_recordset','-',null,'n');
set gen_new_oid_value to 5505;
insert into pg_proc values ('jsonb_typeof',11,10,12,'f','f','t','f','i',1,25,'f','3802',null,null,null,'jsonb_typeof','-',null,'n');
set gen_new_oid_value to 4038;
insert into pg_proc values ('jsonb_ne',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_ne','-',null,'n');
set gen_new_oid_value to 4039;
insert into pg_proc values ('jsonb_lt',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_lt','-',null,'n');
set gen_new_oid_value to 4040;
insert into pg_proc values ('jsonb_gt',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_gt','-',null,'n');
set gen_new_oid_value to 4041;
insert into pg_proc values ('jsonb_le',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_le','-',null,'n');
set gen_new_oid_value to 4042;
insert into pg_proc values ('jsonb_ge',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_ge','-',null,'n');
set gen_new_oid_value to 4043;
insert into pg_proc values ('jsonb_eq',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_eq','-',null,'n');
set gen_new_oid_value to 4044;
insert into pg_proc values ('jsonb_cmp',11,10,12,'f','f','t','f','i',2,23,'f','3802 3802',null,null,null,'jsonb_cmp','-',null,'n');
set gen_new_oid_value to 4045;
insert into pg_proc values ('jsonb_hash',11,10,12,'f','f','t','f','i',1,23,'f','3802',null,null,null,'jsonb_hash','-',null,'n');
set gen_new_oid_value to 4046;
insert into pg_proc values ('jsonb_contains',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_contains','-',null,'n');
set gen_new_oid_value to 4047;
insert into pg_proc values ('jsonb_exists',11,10,12,'f','f','t','f','i',2,16,'f','3802 25',null,null,null,'jsonb_exists','-',null,'n');
set gen_new_oid_value to 4048;
insert into pg_proc values ('jsonb_exists_any',11,10,12,'f','f','t','f','i',2,16,'f','3802 1009',null,null,null,'jsonb_exists_any','-',null,'n');
set gen_new_oid_value to 4049;
insert into pg_proc values ('jsonb_exists_all',11,10,12,'f','f','t','f','i',2,16,'f','3802 1009',null,null,null,'jsonb_exists_all','-',null,'n');
set gen_new_oid_value to 4050;
insert into pg_proc values ('jsonb_contained',11,10,12,'f','f','t','f','i',2,16,'f','3802 3802',null,null,null,'jsonb_contained','-',null,'n');
set gen_new_oid_value to 3480;
insert into pg_proc values ('gin_compare_jsonb',11,10,12,'f','f','t','f','i',2,23,'f','25 25',null,null,null,'gin_compare_jsonb','-',null,'n');
set gen_new_oid_value to 3482;
insert into pg_proc values ('gin_extract_jsonb',11,10,12,'f','f','t','f','i',3,2281,'f','2281 2281 2281',null,null,null,'gin_extract_jsonb','-',null,'n');
set gen_new_oid_value to 3483;
insert into pg_proc values ('gin_extract_jsonb_query',11,10,12,'f','f','t','f','i',7,2281,'f','2277 2281 21 2281 2281 2281 2281',null,null,null,'gin_extract_jsonb_query','-',null,'n');
set gen_new_oid_value to 3484;
insert into pg_proc values ('gin_consistent_jsonb',11,10,12,'f','f','t','f','i',8,16,'f','2281 21 2277 23 2281 2281 2281 2281',null,null,null,'gin_consistent_jsonb','-',null,'n');
set gen_new_oid_value to 3488;
insert into pg_proc values ('gin_triconsistent_jsonb',11,10,12,'f','f','t','f','i',7,16,'f','2281 21 2277 23 2281 2281 2281',null,null,null,'gin_triconsistent_jsonb','-',null,'n');
set gen_new_oid_value to 5516;
insert into pg_proc values ('jsonb_object',11,10,12,'f','f','t','f','i',1,3802,'f','1009',null,null,null,'jsonb_object','-',null,'n');
set gen_new_oid_value to 5517;
insert into pg_proc values ('jsonb_object',11,10,12,'f','f','t','f','i',2,3802,'f','1009 1009',null,null,null,'jsonb_object_two_arg','-',null,'n');
set gen_new_oid_value to 5518;
insert into pg_proc values ('to_jsonb',11,10,12,'f','f','t','f','s',1,3802,'f','2283',null,null,null,'to_jsonb','-',null,'n');
set gen_new_oid_value to 5519;
insert into pg_proc values ('jsonb_agg_transfn',11,10,12,'f','f','f','f','s',2,2281,'f','2281 2283',null,null,null,'jsonb_agg_transfn','-',null,'n');
set gen_new_oid_value to 5520;
insert into pg_proc values ('jsonb_agg_finalfn',11,10,12,'f','f','f','f','s',1,3802,'f','2281',null,null,null,'jsonb_agg_finalfn','-',null,'n');
set gen_new_oid_value to 5521;
insert into pg_proc values ('jsonb_agg',11,10,12,'t','f','f','f','s',1,3802,'f','2283',null,null,null,'aggregate_dummy','-',null,'n');
set gen_new_oid_value to 5522;
insert into pg_proc values ('jsonb_object_agg_transfn',11,10,12,'f','f','f','f','s',3,2281,'f','2281 2276 2276',null,null,null,'jsonb_object_agg_transfn','-',null,'n');
set gen_new_oid_value to 5523;
insert into pg_proc values ('jsonb_object_agg_finalfn',11,10,12,'f','f','f','f','s',1,3802,'f','2281',null,null,null,'jsonb_object_agg_finalfn','-',null,'n');
set gen_new_oid_value to 5524;
insert into pg_proc values ('jsonb_object_agg',11,10,12,'t','f','f','f','i',2,3802,'f','2276 2276',null,null,null,'aggregate_dummy','-',null,'n');
set gen_new_oid_value to 5525;
insert into pg_proc values ('jsonb_build_array',11,10,12,'f','f','f','f','s',1,3802,'f','2276','{2276}','{v}',null,'jsonb_build_array','-',null,'n');
set gen_new_oid_value to 5526;
insert into pg_proc values ('jsonb_build_array',11,10,12,'f','f','f','f','s',0,3802,'f','',null,null,null,'jsonb_build_array_noargs','-',null,'n');
set gen_new_oid_value to 5527;
insert into pg_proc values ('jsonb_build_object',11,10,12,'f','f','f','f','s',1,3802,'f','2276','{2276}','{v}',null,'jsonb_build_object','-',null,'n');
set gen_new_oid_value to 5528;
insert into pg_proc values ('jsonb_build_object',11,10,12,'f','f','f','f','s',0,3802,'f','',null,null,null,'jsonb_build_object_noargs','-',null,'n');
set gen_new_oid_value to 5529;
insert into pg_proc values ('jsonb_strip_nulls',11,10,12,'f','f','t','f','i',1,3802,'f','3802',null,null,null,'jsonb_strip_nulls','-',null,'n');
set gen_new_oid_value to 5530;
insert into pg_proc values ('jsonb_to_record',11,10,12,'f','f','t','f','s',1,2249,'f','3802',null,null,null,'jsonb_to_record','-',null,'n');
set gen_new_oid_value to 5531;
insert into pg_proc values ('jsonb_to_recordset',11,10,12,'f','f','f','t','s',1,2249,'f','3802',null,null,null,'jsonb_to_recordset','-',null,'n');
set gen_new_oid_value to 5532;
insert into pg_proc values ('gin_extract_jsonb_path',11,10,12,'f','f','t','f','i',3,2281,'f','2281 2281 2281',null,null,null,'gin_extract_jsonb_path','-',null,'n');
set gen_new_oid_value to 5533;
insert into pg_proc values ('gin_extract_jsonb_query_path',11,10,12,'f','f','t','f','i',7,2281,'f','2277 2281 21 2281 2281 2281 2281',null,null,null,'gin_extract_jsonb_query_path','-',null,'n');
set gen_new_oid_value to 5534;
insert into pg_proc values ('gin_consistent_jsonb_path',11,10,12,'f','f','t','f','i',8,16,'f','2281 21 2277 23 2281 2281 2281 2281',null,null,null,'gin_consistent_jsonb_path','-',null,'n');
set gen_new_oid_value to 5535;
insert into pg_proc values ('gin_triconsistent_jsonb_path',11,10,12,'f','f','t','f','i',7,18,'f','2281 21 2277 23 2281 2281 2281',null,null,null,'gin_triconsistent_jsonb_path','-',null,'n');
set gen_new_oid_value to 5536;
insert into pg_proc values ('jsonb_concat',11,10,12,'f','f','t','f','i',2,3802,'f','3802 3802',null,null,null,'jsonb_concat','-',null,'n');
set gen_new_oid_value to 5537;
insert into pg_proc values ('jsonb_delete',11,10,12,'f','f','t','f','i',2,3802,'f','3802 25',null,null,null,'jsonb_delete','-',null,'n');
set gen_new_oid_value to 5538;
insert into pg_proc values ('jsonb_delete',11,10,12,'f','f','t','f','i',2,3802,'f','3802 23',null,null,null,'jsonb_delete_idx','-',null,'n');
set gen_new_oid_value to 5539;
insert into pg_proc values ('jsonb_delete_path',11,10,12,'f','f','t','f','i',2,3802,'f','3802 1009',null,null,null,'jsonb_delete_path','-',null,'n');
set gen_new_oid_value to 5540;
insert into pg_proc values ('jsonb_set',11,10,12,'f','f','t','f','i',4,3802,'f','3802 1009 3802 16',null,null,null,'jsonb_set','-',null,'n');
set gen_new_oid_value to 5541;
insert into pg_proc values ('jsonb_pretty',11,10,12,'f','f','t','f','i',1,25,'f','3802',null,null,null,'jsonb_pretty','-',null,'n');
reset gen_new_oid_value;

------------------------------------------------------------------
-- pg_type
------------------------------------------------------------------
set gen_new_oid_value to 193;
insert into pg_type values ('json',11,10,-1,'f','b','t',',',0,0,'json_in','json_out','json_recv','json_send','-','i','x','f',0,-1,0,null,null);
set gen_new_oid_value to 199;
insert into pg_type values ('_json',11 ,10,-1,'f','b','t',',',0,193,'array_in','array_out','array_recv','array_send','-','i','x','f',0,-1,0,null,null);                
set gen_new_oid_value to 3802;
insert into pg_type values ('jsonb',11,10,-1,'f','b','t',',',0,0,'jsonb_in','jsonb_out','jsonb_recv','jsonb_send','-','i','x','f',0,-1,0,null,null);
set gen_new_oid_value to 3807;
insert into pg_type values ('_jsonb',11,10,-1,'f','b','t',',',0,3802,'array_in','array_out','array_recv','array_send','-','i','x','f',0,-1,0,null,null);
reset gen_new_oid_value;

------------------------------------------------------------------
-- pg_aggregate
------------------------------------------------------------------
-- json
insert into pg_aggregate values (3175,'json_agg_transfn','-','-','-','json_agg_finalfn',0,2281,null,'f');
insert into pg_aggregate values (3197,'json_object_agg_transfn','-','-','-','json_object_agg_finalfn',0,2281,null,'f');

-- jsonb
insert into pg_aggregate values (5521,'jsonb_agg_transfn','-','-','-','jsonb_agg_finalfn',0,2281,null,'f');
insert into pg_aggregate values (5524,'jsonb_object_agg_transfn','-','-','-','jsonb_object_agg_finalfn',0,2281,null,'f');


------------------------------------------------------------------
-- pg_amop
------------------------------------------------------------------
-- btree jsonb_ops
insert into pg_amop values (4033,0,1,'f',3242);
insert into pg_amop values (4033,0,2,'f',3244);
insert into pg_amop values (4033,0,3,'f',5515);
insert into pg_amop values (4033,0,4,'f',3245);
insert into pg_amop values (4033,0,5,'f',3243);

-- hash jsonb ops
insert into pg_amop values (4034,0,1,'f',5515);


-- GIN jsonb ops
insert into pg_amop values (4036,0,7,'f',3246);
insert into pg_amop values (4036,25,9,'f',3247);
insert into pg_amop values (4036,1009,10,'f',3248);
insert into pg_amop values (4036,1009,11,'f',3249);

-- GIN jsonb hash ops
insert into pg_amop values (4037,0,7,'f',3246);


------------------------------------------------------------------
-- pg_amproc
------------------------------------------------------------------
insert into pg_amproc values (4033,0,1,4044);
insert into pg_amproc values (4034,0,1,4045);
insert into pg_amproc values (4036,0,1,3480);
insert into pg_amproc values (4036,0,2,3482);
insert into pg_amproc values (4036,0,3,3483);
insert into pg_amproc values (4036,0,4,3484);
insert into pg_amproc values (4036,0,6,3488);
insert into pg_amproc values (4037,0,1,351);
insert into pg_amproc values (4037,0,2,3485);
insert into pg_amproc values (4037,0,3,3486);
insert into pg_amproc values (4037,0,4,3487);
insert into pg_amproc values (4037,0,6,3489);

------------------------------------------------------------------
-- pg_cast
------------------------------------------------------------------
insert into pg_cast values (193,3802,0,'e');
insert into pg_cast values (3802,193,0,'e');

------------------------------------------------------------------
-- pg_opclass
------------------------------------------------------------------
set gen_new_oid_value to 4033;
insert into pg_opclass values (403,'jsonb_ops',11,10,3802,'t',0);
set gen_new_oid_value to 4034;
insert into pg_opclass values (405,'jsonb_ops',11,10,3802,'t',0); 
set gen_new_oid_value to 4036;
insert into pg_opclass values (2742,'jsonb_ops',11,10,3802,'t',25);
set gen_new_oid_value to 4037;
insert into pg_opclass values (2742,'jsonb_hash_ops',11,10,3802,'f',23);
reset gen_new_oid_value;

------------------------------------------------------------------
-- pg_operator
------------------------------------------------------------------
set gen_new_oid_value to 3962;
insert into pg_operator values ('->' ,11,10,'b','f',193 ,25  ,193 ,0   ,0   ,0,0,0,0,'json_object_field','-','-' );
set gen_new_oid_value to 3963;
insert into pg_operator values ('->>',11,10,'b','f',193 ,25  ,25  ,0   ,0   ,0,0,0,0,'json_object_field_text','-','-' );
set gen_new_oid_value to 3964;
insert into pg_operator values ('->' ,11,10,'b','f',193 ,23  ,193 ,0   ,0   ,0,0,0,0,'json_array_element','-','-' );
set gen_new_oid_value to 3965;
insert into pg_operator values ('->>',11,10,'b','f',193 ,23  ,25  ,0   ,0   ,0,0,0,0,'json_array_element_text','-','-' );
set gen_new_oid_value to 3966;
insert into pg_operator values ('#>' ,11,10,'b','f',193 ,1009,193 ,0   ,0   ,0,0,0,0,'json_extract_path_op','-','-' );
set gen_new_oid_value to 3967;
insert into pg_operator values ('#>>',11,10,'b','f',193 ,1009,25  ,0   ,0   ,0,0,0,0,'json_extract_path_text_op','-','-' );
set gen_new_oid_value to 5506;
insert into pg_operator values ('->' ,11,10,'b','f',3802,25  ,3802,0   ,0   ,0,0,0,0,'jsonb_object_field','-','-' );
set gen_new_oid_value to 3477;
insert into pg_operator values ('->>',11,10,'b','f',3802,25  ,25  ,0   ,0   ,0,0,0,0,'jsonb_object_field_text','-','-' );
set gen_new_oid_value to 5507;
insert into pg_operator values ('->' ,11,10,'b','f',3802,23  ,3802,0   ,0   ,0,0,0,0,'jsonb_array_element','-','-' );
set gen_new_oid_value to 3481;
insert into pg_operator values ('->>',11,10,'b','f',3802,23  ,25  ,0   ,0   ,0,0,0,0,'jsonb_array_element_text','-','-' );
set gen_new_oid_value to 5508;
insert into pg_operator values ('#>' ,11,10,'b','f',3802,1009,3802,0   ,0   ,0,0,0,0,'jsonb_extract_path_op','-','-' );
set gen_new_oid_value to 5501;
insert into pg_operator values ('#>>',11,10,'b','f',3802,1009,25  ,0   ,0   ,0,0,0,0,'jsonb_extract_path_text_op','-','-' );
set gen_new_oid_value to 5515;
insert into pg_operator values ('='  ,11,10,'b','t',3802,3802,16  ,5515,3241,0,0,0,0,'jsonb_eq','eqsel','eqjoinsel');
set gen_new_oid_value to 3241;
insert into pg_operator values ('<>' ,11,10,'b','f',3802,3802,16  ,3241,5515,0,0,0,0,'jsonb_ne','neqsel','neqjoinsel');
set gen_new_oid_value to 3242;
insert into pg_operator values ('<'  ,11,10,'b','f',3802,3802,16  ,3243,3245,0,0,0,0,'jsonb_lt','scalarltsel','scalarltjoinsel');
set gen_new_oid_value to 3243;
insert into pg_operator values ('>'  ,11,10,'b','f',3802,3802,16  ,3242,3244,0,0,0,0,'jsonb_gt','scalargtsel','scalargtjoinsel');
set gen_new_oid_value to 3244;
insert into pg_operator values ('<=' ,11,10,'b','f',3802,3802,16  ,3245,3243,0,0,0,0,'jsonb_le','scalarltsel','scalarltjoinsel');
set gen_new_oid_value to 3245;
insert into pg_operator values ('>=' ,11,10,'b','f',3802,3802,16  ,3244,3242,0,0,0,0,'jsonb_ge','scalargtsel','scalargtjoinsel');
set gen_new_oid_value to 3246;
insert into pg_operator values ('@>' ,11,10,'b','f',3802,3802,16  ,0   ,3250,0,0,0,0,'jsonb_contains','contsel','contjoinsel');
set gen_new_oid_value to 3247;
insert into pg_operator values ('?'  ,11,10,'b','f',3802,25  ,16  ,0   ,0   ,0,0,0,0,'jsonb_exists','contsel','contjoinsel');
set gen_new_oid_value to 3248;
insert into pg_operator values ('?|' ,11,10,'b','f',3802,1009,16  ,0   ,0   ,0,0,0,0,'jsonb_exists_any','contsel','contjoinsel');
set gen_new_oid_value to 3249;
insert into pg_operator values ('?&' ,11,10,'b','f',3802,1009,16  ,0   ,0   ,0,0,0,0,'jsonb_exists_all','contsel','contjoinsel');
set gen_new_oid_value to 3250;
insert into pg_operator values ('<@' ,11,10,'b','f',3802,3802,16  ,0   ,3246,0,0,0,0,'jsonb_contained','contsel','contjoinsel');
set gen_new_oid_value to 3284;
insert into pg_operator values ('||' ,11,10,'b','f',3802,3802,3802,0   ,0   ,0,0,0,0,'jsonb_concat','-','-');
set gen_new_oid_value to 3285;
insert into pg_operator values ('-'  ,11,10,'b','f',3802,25  ,3802,0   ,0   ,0,0,0,0,'5537','-','-' );
set gen_new_oid_value to 3286;
insert into pg_operator values ('-'  ,11,10,'b','f',3802,23  ,3802,0   ,0   ,0,0,0,0,'5538','-','-' );
set gen_new_oid_value to 3287;
insert into pg_operator values ('#-' ,11,10,'b','f',3802,1009,3802,0   ,0   ,0,0,0,0,'jsonb_delete_path','-','-');
reset gen_new_oid_value;
