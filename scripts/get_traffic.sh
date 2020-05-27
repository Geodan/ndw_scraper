#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIROUTPUT="/var/data/html/saturnus/saturnus.geodan.nl/traffic" 
ogr2ogr -f GeoJSON "$DIR/traffic.json.new" "PG:host=mimas.geodan.nl port=5432 dbname=research user=postgres password=" -sql "

WITH traffic AS (
	SELECT location, date, speed_avg, flow_avg
	FROM ndw.trafficspeed 
	WHERE date > (NOW() - INTERVAL '15 minutes')
),
osm_traffic AS (
	SELECT osm.osm_id, osm.geom, date, t1.speed_avg speed, t1.flow_avg flow
	FROM ndw.osm_mapping osm LEFT JOIN traffic t1
	ON t1.location = osm.mst_id --AND t1.speed_avg > 0 AND t1.flow_avg > 0
	WHERE NOT EXISTS (
		SELECT 1 FROM traffic t2
		WHERE t2.location = osm.mst_id AND t1.date < t2.date
	)
	AND t1.speed_avg > 0 AND t1.flow_avg > 0
)
SELECT DISTINCT ON (geom) * FROM osm_traffic
"
/bin/mv "$DIR/traffic.json.new" "$DIROUTPUT/traffic.json"
