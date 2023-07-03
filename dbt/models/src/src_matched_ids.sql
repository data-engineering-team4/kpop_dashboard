SELECT chart.track_id
FROM {{ ref('src_track_ids') }} kpop
JOIN {{ ref('src_chart_ids') }} chart ON kpop.id = chart.track_id