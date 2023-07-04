-- Dashboard #1
WITH chart AS (
    SELECT * FROM {{ ref("src_country_weekly_chart") }}
)
SELECT *
FROM chart
WHERE chart.chart_date >= DATEADD(year, -1, CURRENT_DATE())
