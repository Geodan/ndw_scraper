var fs        = require('fs')
  , zlib          = require('zlib')
  , path      = require('path')
  , XmlStream = require('xml-stream')
  , XmlJson = require('xml-json')
  , pg 		  = require('/usr/local/lib/node_modules/pg')
  , request   = require('/usr/local/lib/node_modules/request');
var ldj = require('ldjson-stream');

var _db;
//Prepare PG
pg.on('error', function (err) {
  console.log('Database error!', err);
});
var connstring = "tcp://postgres@localhost:5433/research";
var writecount = 0;

var req = pg.connect(connstring, function(err, client) {
		if (err){
              console.log('meeh',err);
              reject(err);
        }
        _db = client;
        //Remove old data
        _db.query('DELETE FROM ndw.mst_points;');
        _db.query('DELETE FROM ndw.mst_lines;');
        
        function writeout(item){
        	
        	//return;//
        	if (item.location && item.location.longitude){
        		console.log(JSON.stringify(item));
				query = "INSERT INTO ndw.mst_points (mst_id,name, location, alertcdirection, alertclocation, alertcoffset, carriageway, direction, distance, method, equipment, lanes, characteristics, geom) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9,$10, $11, $12, $13, ST_SetSrid(ST_MakePoint("+item.location.longitude+","+item.location.latitude+"),4326))";
				var vars = [
				   item.mst_id
				  ,item.name
				  //,item.locationi
				  ,0
				  ,item.alertcdirection
				  ,item.alertclocation
				  ,item.alertcoffset
				  ,item.carriageway
				  ,item.direction
				  ,item.distance
				  ,item.method
				  ,item.equipment
				  ,item.lanes
				  ,JSON.stringify(item.characteristics)
				];
				writecount = writecount + 1;
				_db.query(query, vars, function(err, result){
					if (err){
						console.warn(err, query);
					}
				});
			}
			//console.log('---------------------------------');

        }
        console.log('Opening stream');
        
        
    var converter = XmlJson('measurementSiteRecord', {})    
	var stream = request.get('http://opendata.ndw.nu/measurement_current.xml.gz')
		.pipe(zlib.createGunzip())
        .pipe(converter).pipe(ldj.serialize())
        .on('data',function(data){
        	var node = JSON.parse(data);
    		var item = {};
			item.time = node.measurementSiteRecordVersionTime;
			
			item.name = node.measurementSiteName.values.value._;
			if (node.measurementEquipmentTypeUsed){
				item.equipment = node.measurementEquipmentTypeUsed.values.value._;
			}
			else item.equipment = 'unknown';
			item.mst_id = node.id;
			item.lanes = node.measurementSiteNumberOfLanes;
			item.characteristics = node.measurementSpecificCharacteristics;
			item.method = node.computationMethod;
			//If only 1 location
			if (node.measurementSiteLocation['xsi:type'] == 'Point'){
				
				item.startpoint = {latitude: 1,longitude: 1};
				item.endpoint = {latitude: 0,longitude: 0};
				item.location = node.measurementSiteLocation.locationForDisplay;
				var alertc = node.measurementSiteLocation.alertCPoint;
				item.alertcdirection = alertc.alertCDirection.alertCDirectionCoded;
				
				item.alertclocation = alertc.alertCMethod4PrimaryPointLocation.alertCLocation.specificLocation;
				item.alertcoffset = alertc.alertCMethod4PrimaryPointLocation.offsetDistance.offsetDistance;
				writeout(item);
			}
			//If multiple locations
			
			else if (node.measurementSiteLocation['xsi:type'] =="ItineraryByIndexedLocations") {
				var l = node.measurementSiteLocation
					.locationContainedInItinerary;
				let locations = Array.isArray(l)?l:[l];
				locations.forEach(location=>{
					item.index = location.index;
					if (location['xsi:type'] == 'Linear'){
						var linenode = location
							.linearExtension.linearByCoordinatesExtension;
						item.startpoint = linenode.linearCoordinatesStartPoint.pointCoordinates;
						item.endpoint = linenode.linearCoordinatesEndPoint.pointCoordinates;
						var alertc = location.alertCLinear;
						item.alertclocation1 = alertc.alertCMethod4PrimaryPointLocation.alertCLocation.specificLocation;
						item.alertclocation2 = alertc.alertCMethod4SecondaryPointLocation.alertCLocation.specificLocation;
					}
					else { 
						//this doesn't exist
					}
					item.location = location
						.locationForDisplay;
					writeout(item);
				});
			}
			else {
				console.log('Not included: ' ,id);
			}
			
	});
	
});
