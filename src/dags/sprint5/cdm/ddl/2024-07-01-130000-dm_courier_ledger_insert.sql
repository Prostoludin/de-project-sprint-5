INSERT INTO cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
    )
WITH del AS (SELECT cour.courier_id,
					cour.courier_name,
					dt."year",
					dt."month",
					ord.order_key,
					fod.order_sum,
					fod.rate,
					fod.order_tips
			FROM dds_project.fct_order_delivery fod
			JOIN dds_project.dm_orders ord ON ord.id = fod.order_id
			JOIN dds_project.dm_couriers cour ON cour.id = ord.courier_id
			JOIN dds_project.dm_order_ts dt ON dt.id = ord.order_ts_id)
SELECT 	courier_id,
		courier_name,
		year AS settlement_year,
		month AS settlement_month,
		COUNT(order_key) AS orders_count,
		SUM(order_sum) AS orders_total_sum,
		AVG(rate)::NUMERIC(14,2) AS rate_avg,
		SUM(order_sum)*0.25 AS order_processing_fee,
		CASE
			WHEN AVG(rate)::NUMERIC(14,2) < 4 THEN SUM(GREATEST(100, 0.05*order_sum))
			WHEN AVG(rate)::NUMERIC(14,2) >= 4 AND AVG(rate)::NUMERIC(14,2) < 4.5 THEN SUM(GREATEST(150, 0.07*order_sum))
			WHEN AVG(rate)::NUMERIC(14,2) >= 4.5 AND AVG(rate)::NUMERIC(14,2) < 4.9 THEN SUM(GREATEST(175, 0.08*order_sum))
			ELSE SUM(GREATEST(200, 0.1*order_sum))
		END AS courier_order_sum,
		SUM(order_tips) AS courier_tips_sum,
		CASE
			WHEN AVG(rate)::NUMERIC(14,2) < 4 THEN SUM(GREATEST(100, 0.05*order_sum)) + SUM(order_tips)*0.95
			WHEN AVG(rate)::NUMERIC(14,2) >= 4 AND AVG(rate)::NUMERIC(14,2) < 4.5 THEN SUM(GREATEST(150, 0.07*order_sum)) + SUM(order_tips)*0.95
			WHEN AVG(rate)::NUMERIC(14,2) >= 4.5 AND AVG(rate)::NUMERIC(14,2) < 4.9 THEN SUM(GREATEST(175, 0.08*order_sum)) + SUM(order_tips)*0.95
			ELSE SUM(GREATEST(200, 0.1*order_sum)) + SUM(order_tips)*0.95
		END AS courier_reward_sum
FROM del
GROUP BY courier_id, courier_name, year, month
ORDER BY courier_id, courier_name, year, month ASC
ON CONFLICT (courier_id, settlement_year, courier_name, settlement_month)
DO UPDATE SET orders_count= EXCLUDED.orders_count,
			  orders_total_sum= EXCLUDED.orders_total_sum,
			  rate_avg= EXCLUDED.rate_avg,
			  order_processing_fee= EXCLUDED.order_processing_fee,
			  courier_order_sum= EXCLUDED.courier_order_sum,
			  courier_tips_sum= EXCLUDED.courier_tips_sum,
			  courier_reward_sum= EXCLUDED.courier_reward_sum;