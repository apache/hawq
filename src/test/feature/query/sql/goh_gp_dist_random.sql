select count(*) > 0 as c from gp_dist_random('pg_class');
select relname from gp_dist_random('pg_class') c
  inner join gp_dist_random('pg_namespace') n
    on c.gp_segment_id = n.gp_segment_id
      and c.relnamespace = n.oid
  where n.nspname = 'nonexistent';
