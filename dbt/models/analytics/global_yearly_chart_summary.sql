-- Dashboard #1
WITH chart AS (
    SELECT * FROM {{ ref("src_global_yearly_chart") }}
)
SELECT *
FROM chart
WHERE chart.chart_date >= DATEADD(year, -6, CURRENT_DATE())