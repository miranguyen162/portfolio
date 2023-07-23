select order_id, date, status, fulfilment, sales_channel_, ship_service_level, category, ship_state,
    qty, amount, amount*0.012 as amount_usd
from sales.amazon
where ship_country = 'IN'
  and date >= '2022-04-01' and date <= '2022-06-30'
order by date
