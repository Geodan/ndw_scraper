def intersects($b): any(.[]; . as $x | any( $b[]; . == $x) ) ;

select([.measurementSiteReference.id] | intersects(["GEO0K_K_RWSTI360404","RWS01_MONIBAS_0020vwc1168ra","RWS01_MONIBAS_0020vwc1189ra"])) |
{
	id: .measurementSiteReference.id,
	time: .measurementTimeDefault, 
	values: .measuredValue[] | { 
	  index: .index,
	  value: ( .measuredValue | 
		if .basicData["xsi:type"] == "TrafficFlow" 
		then .basicData.vehicleFlow.vehicleFlowRate 
		else .basicData.averageVehicleSpeed.speed 
		end
	  )
	}
} | [.time, .id + "_" + .values.index, (.values.value | tonumber)] | @tsv

