drop function get_offers_increasing_visits(date,date,integer,double precision,double precision,double precision);
create or replace function get_offers_increasing_visits(first_date date, 
														last_date date, 
														trans_number int, 
														max_churn_rate float, 
														max_discount_share float, 
														margin_share float)
returns table (customer_id int, 
			start_date date, 
			end_date date, 
			required_transactions_count numeric, 
			group_name varchar, 
			offer_discount_depth numeric)
as $$
begin
	return query
	select c.customer_id,
		$1 as start_date,
		$2 as end_date,
		round((cast($2 as date) - cast($1 as date)) / nullif(c.customer_frequency, 0)) + $3 as required_transactions_count,
		sg.group_name,
		(g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) as offer_discount_depth
from customers c
join groups g on c.customer_id = g.customer_id
join sku_group sg on g.group_id = sg.group_id
where g.group_affinity_index = (select max(g1.group_affinity_index) from groups g1)
	and g.group_churn_rate <= $4
	and g.group_discount_share <= $5
	and (g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) < (($6/100) * g.group_margin);
end
$$
language plpgsql;


select * from get_offers_increasing_visits ('2023-08-01', '2023-09-01', 1, 0.6, 1, 20);

