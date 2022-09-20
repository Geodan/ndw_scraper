#!/usr/bin/env bash

curl -s https://opendata.ndw.nu/measurement.xml.gz |\
	gunzip |\
	xml-json measurementSiteRecord | \
	jq -f measurement_reader.jq
