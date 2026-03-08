create table user_events(
event_id int primary key,	
user_id	int,
event_type char(100),
event_date	date,
product_id	int,
amount	float,
traffic_source char(100)

)
select * from user_events

---1: define sales funnel and the different stages
with funnel_stage as(
select 
count(distinct case when event_type='page_view' then user_id end) as stage_1_views,
count(distinct case when event_type='add_to_cart' then user_id end) as stage_2_cart,
count(distinct case when event_type='checkout_start' then user_id end) as stage_3_checkout,
count(distinct case when event_type='payment_info' then user_id end) as stage_4_payment,
count(distinct case when event_type='purchase' then user_id end) as stage_5_purchase

from user_events
where event_date >= current_date - interval '60 days'
)


select
  stage_1_views,
  stage_2_cart,
  round(stage_2_cart * 100/ stage_1_views) as view_to_cart_rate,

  stage_3_checkout,
  round(stage_3_checkout * 100 / stage_2_cart) as cart_to_checkout_rate,

  stage_4_payment,
  round(stage_4_payment * 100 / stage_3_checkout) as checkout_to_payment_rate,

  stage_5_purchase,
  round(stage_5_purchase * 100 / stage_4_payment) as payment_to_purchase_rate,

  round(stage_5_purchase * 100 / stage_1_views) as orevall_conversion_rate

 from funnel_stage

--- 2: FUNNEL BY SOURCE
with source_funnel as 
(
 select
 traffic_source,
 count(distinct case when event_type='page_view' then user_id end) as views,
 count(distinct case when event_type='add_to_cart' then user_id end) as cart,
 count(distinct case when event_type='purchase' then user_id end) as purchase

 from user_events
 where event_date >= current_date - interval '60 days'
 group by  1                    
)

select 
 traffic_source,
 views,
 cart,
 purchase,

 round(cart * 100 / views) as cart_conversion_rate,
 round(purchase * 100 / cart) as cart_to_purchase_conversion_rate,
 round(purchase * 100 / views) as purchase_conversion_rate
from source_funnel

--- time to conversion analysis

with user_journey as (
select
    user_id,
    min(case when event_type = 'page_view' then event_date end)::timestamp as view_time,
    min(case when event_type = 'add_to_cart' then event_date end)::timestamp as cart_time,
    min(case when event_type = 'purchase' then event_date end)::timestamp as purchase_time
from user_events
where event_date >= current_date - interval '60 days'
group by user_id
having min(case when event_type = 'purchase' then event_date end) is not null
)

select
    count(*) as converted_users,
    avg(extract(epoch from (cart_time - view_time)) / 60) as avg_view_to_cart_min,
    avg(extract(epoch from (purchase_time - cart_time)) / 60) as avg_cart_to_purchase_min,
    avg(extract(epoch from (purchase_time - view_time)) / 60) as avg_view_to_purchase_min
from user_journey;

--- revenue funnel analysis
with funnel_revenue as 
(
 select
 count(distinct case when event_type='page_view' then user_id end) as total_views,
 count(distinct case when event_type='purchase' then user_id end) as total_buyers,
 sum(case when event_type='purchase' then amount end) as total_revenue,
 count(case when event_type='purchase' then 1 end) as total_orders
 from user_events
 where event_date >= current_date - interval '60 days'
                   
)

select
 total_views,
 total_buyers,
 total_revenue,
 total_orders,
 total_revenue/total_orders as avg_order_value,
 total_revenue/total_buyers as reveue_buyers,
 total_revenue/total_views as revenue_per_view

from funnel_revenue