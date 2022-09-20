select(
	.measurementEquipmentTypeUsed.values.value._ == "lus" #no fcd data
	and (.id | startswith("RWS01_MONICA") | not) #no monica data
	and .computationMethod != "movingAverageOfSamples" #no traveltim
	and (.measurementSpecificCharacteristics | type == "array") #workaround for fcd data hiding as lus
) 
|
{
	id: .id,
	name: .measurementSiteName.values.value._,
	time: .measurementSiteRecordVersionTime,
	numlanes: .measurementSiteNumberOfLanes,
	side: .measurementSide,
	num_entities: (.measurementSpecificCharacteristics | length ),
	entities: 
		.measurementSpecificCharacteristics[] 
		|{index: .index,
			entity: .measurementSpecificCharacteristics |  
			(.specificLane + "_" +
			.specificMeasurementValueType + "_" +
			(.specificVehicleCharacteristics | (.vehicleType // (.lengthCharacteristic | 
				if 
				  type=="array" then .[0].vehicleLength + "-" + .[1].vehicleLength
				  else "> " + .vehicleLength 
				end) )
			) 
			)
		}
	
} |
[.id + "_" + .entities.index, .entities.entity] | @tsv
