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

