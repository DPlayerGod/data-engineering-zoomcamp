SELECT COUNT(*) FROM `kestra-486406.raw.fct_monthly_zone_revenue`;

SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS revenue 
FROM `kestra-486406.raw.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM DATE(revenue_month)) = 2020
GROUP BY pickup_zone
ORDER BY revenue DESC
LIMIT 1;


SELECT
    SUM(total_monthly_trips) AS total
FROM `kestra-486406.raw.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM DATE(revenue_month)) = 2019
  AND EXTRACT(MONTH FROM DATE(revenue_month)) = 10;