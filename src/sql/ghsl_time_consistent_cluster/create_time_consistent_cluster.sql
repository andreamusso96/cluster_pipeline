DROP TABLE {{ params.time_consistent_cluster_year }};

CREATE TABLE {{ params.time_consistent_cluster_year }} AS
WITH zonal_stats AS (
    SELECT cluster_uid, (St_SummaryStats(St_Union(ST_Clip(rast, 1, geom, true)))).*
    FROM {{ params.time_consistent_cluster_geometry_pre_geocoding_table }}, {{ params.pop_table }}
    WHERE St_Intersects(rast,geom)
    GROUP BY cluster_uid
)
SELECT cluster_uid, sum AS population
FROM zonal_stats;
