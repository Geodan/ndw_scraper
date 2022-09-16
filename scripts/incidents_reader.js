var fs		= require('fs')
  , zlib		  = require('zlib')
  , path	  = require('path')
  , XmlStream = require('xml-stream')
  , { Pool }  = require('pg')
  , copy	  = require('pg-copy-streams')
  , request   = require('request')
  , SQL	      = require('sql-template-strings');

const DATA_URL = 'http://opendata.ndw.nu/incidents.xml.gz';
const DEBUG = false;

const incidentClassTypes = {
	'AbnormalTraffic': 'abnormalTrafficType',
	'Accident': 'accidentType',
	'AuthorityOperation': 'authorityOperationType',
	'DisturbanceActivity': 'disturbanceActivityType',
	'PublicEvent': 'publicEventType',
	'PoorEnvironmentConditions': 'poorEnvironmentType',
	'NonWeatherRelatedRoadConditions': 'nonWeatherRelatedRoadConditionType',
	'WeatherRelatedRoadConditions': 'weatherRelatedRoadConditionType',
	'EquipmentOrSystemFault': 'faultyEquipmentOrSystemType',
	'AnimalPresenceObstruction': 'animalPresenceType',
	'EnvironmentalObstruction': 'environmentalObstructionType',
	'GeneralObstruction': 'obstructionType',
	'InfrastructureDamageObstruction': 'infrastructureDamageType',
	'VehicleObstruction': 'vehicleObstructionType'
}

function log(message){
	if(DEBUG) 
		console.log(message);
}
function logerror(message){
	console.error(message);
}

function readXML() {
	return new Promise(function(resolve, reject) {
		var nodes = [];
		var stream = request.get(DATA_URL)
				.pipe(zlib.createGunzip());
		var xml = new XmlStream(stream, 'utf8');
		xml.collect('values');
		xml.on('updateElement: situation', function(node) {
			nodes.push(node);
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

//Prepare PG
const pool = new Pool({
	host: 'mimas3.geodan.nl',
	port: 5432,
	user: 'postgres',
	password: '',
	database: 'research'
});

(async () => {
	// note: we don't try/catch this because if connecting throws an exception
	// we don't need to dispose of the client (it will be undefined)
	const client = await pool.connect();

	try {
		var nodes = await readXML();
		log('read ' + nodes.length + ' nodes');
		log('beginning transaction');
		await client.query('BEGIN');
		await client.query('UPDATE ndw.incidents SET active = 0 WHERE active = 1');

		var counter = 0;
		
		for(var i=0; i<nodes.length; i++) {
			const node = nodes[i];
			//log(node);
			var observationtime = node.situationRecord.situationRecordObservationTime;
			var probabilityofoccurrence = node.situationRecord.probabilityOfOccurrence;
			var id = node.$.id;
			var version = node.$.version;
			var source = node.situationRecord.source.sourceName.values[0].value.$text;
			var location = node.situationRecord.groupOfLocations.locationForDisplay;
			var incidentclass = node.situationRecord.$['xsi:type'];
			var incidenttype = 'Unknown';
			if (incidentClassTypes.hasOwnProperty(incidentclass)) {
				var typeField = incidentClassTypes[incidentclass];
				incidenttype = node.situationRecord[typeField];
			}
			
			//log(observationtime, id, version, probabilityofoccurrence, source, accidenttype,location);

			//log(counter++,'---------------------------------');

			//Stream to db
			var querystring = SQL`
				INSERT INTO ndw.incidents
				(id, version, active, observationtime, probabilityofoccurrence, source, 
					incidentclass, incidenttype, latitude, longitude, geom)
				VALUES (
					${id},
					${version},
					1,
					${observationtime},
					${probabilityofoccurrence},
					${source},
					${incidentclass},
					${incidenttype},
					${location.latitude},
					${location.longitude},
					ST_SetSRID(ST_MakePoint(${location.longitude}, ${location.latitude}), 4326)
				)
				ON CONFLICT ON CONSTRAINT incidents_pkey DO UPDATE SET active = 1;
			`;
			await client.query(querystring);
			counter++;
		};
		log('wrote ' + counter + ' records');
		log('committing transaction');
		await client.query('COMMIT');
	} catch (e) {
		logerror('an error occured, rolling back transaction');
		await client.query('ROLLBACK');
		throw e;
	} finally {
		client.release();
		log('done, please wait until process exits');
	}
})().catch(e => logerror(e.stack))

