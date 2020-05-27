var fs        = require('fs')
  , zlib          = require('zlib')
  , path      = require('path')
  , XmlStream = require('xml-stream')
  , XmlJson = require('xml-json')
  , { Pool }  = require('pg')
  , request   = require('request');
var ldj = require('ldjson-stream');

const DEBUG = false;
const DATA_URL = 'http://opendata.ndw.nu/measurement_current.xml.gz';

function log(message){
	if(DEBUG) 
		console.log(message);
}
function logerror(message){
	console.error(message);
}

var filedate = '';
function readXML() {
	return new Promise(function(resolve, reject) {
		var nodes = [];
		var counter = 0;
		var stream = request.get(DATA_URL)
				.pipe(zlib.createGunzip());
		var xml = new XmlStream(stream, 'utf8');
		xml.collect('values');
		xml.on('updateElement: publicationTime', function(datetime) {
			filedate = datetime.$text;
		});
		xml.on('updateElement: measurementSiteRecord', function(node) {
			counter++;
			if (DEBUG && counter % 100 == 0) {
				process.stdout.write('reading record ' + counter + "\r");
			}
			var item = {};
			item.time = node.measurementSiteRecordVersionTime;
			item.name = node.measurementSiteName.values[0].value.$text;
			if (node.measurementEquipmentTypeUsed){
				item.equipment = node.measurementEquipmentTypeUsed.values[0].value.$text;
			}
			else item.equipment = 'unknown';
			item.mst_id = node.$.id;
			item.lanes = node.measurementSiteNumberOfLanes;
			item.characteristics = node.measurementSpecificCharacteristics;
			item.method = node.computationMethod;
			//If only 1 location
			if (node.measurementSiteLocation.$['xsi:type'] == 'Point'){
				item.startpoint = {latitude: 1,longitude: 1};
				item.endpoint = {latitude: 0,longitude: 0};
				item.location = node.measurementSiteLocation.locationForDisplay;
				var alertc = node.measurementSiteLocation.alertCPoint;
				item.alertcdirection = alertc.alertCDirection.alertCDirectionCoded;
				
				item.alertclocation = alertc.alertCMethod4PrimaryPointLocation.alertCLocation.specificLocation;
				item.alertcoffset = alertc.alertCMethod4PrimaryPointLocation.offsetDistance.offsetDistance;
				nodes.push(item);
			}
			//If multiple locations		
			else if (node.measurementSiteLocation.$['xsi:type'] == 'ItineraryByIndexedLocations') {
				var l = node.measurementSiteLocation.locationContainedInItinerary;
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
					item.location = location.locationForDisplay;
					nodes.push(item);
				});
			}
			else {
				log('Not included: ' ,id);
			}
		});
		xml.on('error', function(message) {
			reject('XML parsing failed: ' + message);
		});
		xml.on('end', function(){
			log('XML closed');
			resolve(nodes);
		});
	});
}

const pool = new Pool({
	host: 'mimas.geodan.nl',
	port: 5432,
	user: 'postgres',
	password: '',
	database: 'research'
});

(async () => {
	// note: we don't try/catch this because if connecting throws an exception
	// we don't need to dispose of the client (it will be undefined)
	const client = await pool.connect();

	try{
		var items = await readXML();
		log('read ' + items.length + ' items');
		log('beginning transaction');
		await client.query('BEGIN');
		await client.query('DELETE FROM ndw.mst_points_all WHERE filedate = $1;', [filedate]);
		//await client.query('DELETE FROM ndw.mst_lines_test WHERE filedate = $1;', [filedate]);

		var counter = 0;

		for(var i=0; i<items.length; i++) {
			var item = items[i];
			if (item.location && item.location.longitude){
				//log(JSON.stringify(item));
				query = "INSERT INTO ndw.mst_points_all (mst_id, filedate, name, location, alertcdirection, alertclocation, alertcoffset, carriageway, direction, distance, method, equipment, lanes, characteristics, geom) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9,$10, $11, $12, $13, $14, ST_SetSrid(ST_MakePoint("+item.location.longitude+","+item.location.latitude+"),4326))";
				var vars = [
				   item.mst_id
				  ,filedate
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
				if (DEBUG && counter % 100 == 0) {
					process.stdout.write('writing record ' + counter + "\r");
				}
				await client.query(query, vars);
				counter++;
			}
		};
		if (DEBUG)
			process.stdout.write("                                        \r");
		log('wrote ' + counter + ' records');
		log('committing transaction');
		await client.query('COMMIT');
	} catch (e) {
		logerror('an error occured, rolling back transaction');
		await client.query('ROLLBACK');
		throw e;
	} finally {
		await client.release();
		log('done, please wait until process exits');
	}
})().catch(e => logerror(e.stack));

