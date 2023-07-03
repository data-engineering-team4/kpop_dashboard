SELECT chart.track_id
FROM src_track_ids kpop
JOIN src_chart_ids chart ON kpop.id = chart.track_id