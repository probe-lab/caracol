create or replace function get_collected_values (
   qid integer,        -- id of query
   lower timestamptz,  -- start time of sequence to return, all returned values will on or after this time
   upper timestamptz   -- end time of collected values, all returned values will be before this time
)
returns table (
	seq integer,
	date timestamptz,
	value float
)
language plpgsql
as $$
declare
-- variable declaration
begin
	return query
	with q as (
	  select id, start, case
	    when interval='hourly' then '1 hour'::interval
	    when interval='daily'  then '1 day'::interval
	    when interval='weekly' then '1 week'::interval
	  end as step
	  from queries where id=qid
	)
	select c.seq, q.start+c.seq*q.step as date, c.value as value
	from q left join collections c on c.query_id = q.id
	where q.start+c.seq*q.step >= lower
	  and q.start+c.seq*q.step < upper
	order by seq;
end; $$ ;

---- create above / drop below ----

drop function if exists get_collected_values;
