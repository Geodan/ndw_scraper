#!/usr/bin/env bash

curl -sL https://opendata.ndw.nu/trafficspeed.xml.gz | \
	gunzip | \
	xml-json siteMeasurements | \
	jq -r -f ./trafficspeed_reader.jq > speed.tsv


awk 'BEGIN { FS = "\t"; OFS = "\t" }
     {
         if (NR == FNR)
             values[$1] = $2
         else
             print $1, values[$2], $3, $4
     }' meas.csv speed.tsv > speed2.tsv

