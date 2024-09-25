DROP FUNCTION get_offers_improving_average_ticket(date,date,double precision,double precision,double precision,double precision);
create or replace function get_offers_improving_average_ticket(period_begin date,
																period_end date,
																improving_k float,
																max_outflow_index float,
																max_discount_volume float,
																margin_volume float
																)
returns table (customer_id int, required_check_measure float, group_name varchar, offer_discount_depth numeric)
as $$
declare fact_start timestamp;
declare fact_end timestamp;
begin
		select min(Transaction_DateTime) INTO fact_start from transactions;
		select max(Transaction_DateTime) INTO fact_end from transactions;
		
		if $1 < fact_start or $2 > fact_end
		then $1 := fact_start;
			$2 := fact_end;
		end if;
		return query
			select g.customer_id,
				(sum(t.transaction_summ)/count(t.transaction_summ)) * $3 as average_ticket,
				sg.group_name,
				(g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) as offer_discount
			from groups g
			join cards c on g.customer_id = c.customer_id 
			join transactions t on c.customer_card_id = t.customer_card_id
			join sku_group sg on g.group_id = sg.group_id
			where (t.transaction_datetime between $1 and $2) 
				and g.group_churn_rate <= $4
				and g.group_discount_share <= $5
				and g.group_affinity_index = (select max(g1.group_affinity_index) 
												from groups g1 )
				and (g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) <= g.group_margin * ($6 / 100)
			group by g.customer_id, sg.group_name, offer_discount;
	end
$$
language plpgsql;

select * from get_offers_improving_average_ticket('2023-08-01', '2023-09-15', 0.5, 1, 1, 20);



-- 2nd method
DROP FUNCTION get_offers_improving_average_ticket(integer,double precision,double precision,double precision,double precision);
create or replace function get_offers_improving_average_ticket(amount int,
																improving_k float,
																max_outflow_index float,
																max_discount_volume float,
																margin_volume float
																)
returns table (customer_id int, required_check_measure float, group_name varchar, offer_discount_depth numeric)
as $$
begin
	return query
	select g.customer_id,
				(sum(t.transaction_summ)/count(t.transaction_summ)) * $2 as average_ticket,
				sg.group_name,
				(g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) as offer_discount
			from groups g
			join cards c on g.customer_id = c.customer_id 
			join transactions t on c.customer_card_id = t.customer_card_id
			join sku_group sg on g.group_id = sg.group_id
			where
				g.group_churn_rate <= $3
				and g.group_discount_share <= $4
				and g.group_affinity_index in (select max(g1.group_affinity_index) 
												from groups g1 )
				and (g.group_minimum_discount - (g.group_minimum_discount % 5) + 5) <= g.group_margin * ($5 / 100)
			group by g.customer_id, sg.group_name, offer_discount, t.transaction_datetime 
			order by t.transaction_datetime desc 
			limit $1;
end
$$
language plpgsql;

select * from get_offers_improving_average_ticket(3, 0.5, 1, 1, 20);
