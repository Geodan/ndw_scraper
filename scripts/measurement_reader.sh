#!/usr/bin/env bash

curl -sL https://opendata.ndw.nu/measurement.xml.gz |\
	gunzip |\
	xml-json measurementSiteRecord | \
	jq -r -f measurement_reader.jq
