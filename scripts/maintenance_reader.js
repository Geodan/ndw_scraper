var fs		= require('fs')
  , zlib		  = require('zlib')
  , path	  = require('path')
  , XmlStream = require('xml-stream')
  , { Pool }  = require('/usr/local/lib/node_modules/pg')
  , copy	  = require('/usr/local/lib/node_modules/pg-copy-streams')
  , request   = require('/usr/local/lib/node_modules/request')
  , SQL	      = require('/usr/local/lib/node_modules/sql-template-strings');

const DATA_URL = 'http://opendata.ndw.nu/wegwerkzaamheden.xml.gz';
const DEBUG = true;

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
		var counter = 0;
		var stream = request.get(DATA_URL)
				.pipe(zlib.createGunzip());
		var xml = new XmlStream(stream, 'utf8');
		xml.collect('values');
		xml.collect('locationContainedInItinerary');
		xml.collect('generalPublicComment');
		xml.on('updateElement: situationRecord', function(node) {
			if (node.$['xsi:type'] != 'MaintenanceWorks') return;
			counter++;
			if (DEBUG && counter % 100 == 0) {
				process.stdout.write('reading record ' + counter + "\r");
			}
			nodes.push(node);
		});
		xml.on('error', function(message) {
			reject('XML parsing failed: ' + message);
		});
		xml.on('end', function(){
			//log('XML closed');
			resolve(nodes);
		});
	});
}

//Prepare PG
const pool = new Pool({
	host: 'localhost',
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
		await client.query('UPDATE ndw.maintenance SET active = 0 WHERE active = 1');

		var counter = 0;
		
		for(var i=0; i<nodes.length; i++) {
			const node = nodes[i];
			//log(node);
			var id = node.$.id;
			var version = node.$.version;
			var versiontime = node.situationRecordVersionTime;
			var starttime = node.validity.validityTimeSpecification.overallStartTime;
			var endtime = node.validity.validityTimeSpecification.overallEndTime;
			var probabilityofoccurrence = node.probabilityOfOccurrence;
			var source = node.source.sourceName.values[0].value.$text;
			var cause = node.cause ? node.cause.causeDescription.values[0].value.$text : '';
			var comments = [];
			if (node.generalPublicComment) {
				comments = node.generalPublicComment.map((e) => {
					return e.comment.values[0].value.$text;
				});
			}
			var location = undefined;
			if (node.groupOfLocations.$['xsi:type'] == 'ItineraryByIndexedLocations') {
				let f = node.groupOfLocations.locationContainedInItinerary.find((e) => {
					return e.location.$['xsi:type'] == 'Linear';
				});
				if (f) {
					location = f.location.locationForDisplay;
				} else {
					f = node.groupOfLocations.locationContainedInItinerary.find((e) => {
						return e.location.$['xsi:type'] == 'Point';
					});
					if (f && f.location.locationForDisplay) {
						location = f.location.locationForDisplay;
					} else if (f && f.location.pointByCoordinates) {
						location = f.location.pointByCoordinates.pointCoordinates;
					}
				}
			} else if (node.groupOfLocations.$['xsi:type'] == 'Point') {
				location = node.groupOfLocations.locationForDisplay;
			}
			if (!location) {
				log('skipped: ' + id);
				continue;
			}
			var maintenancetype = node.roadMaintenanceType;
			
			var querystring = SQL`
				INSERT INTO ndw.maintenance
				(id, version, active, versiontime, starttime, endtime, probabilityofoccurrence, 
					source, maintenancetype, cause, comments, latitude, longitude, geom)
				VALUES (
					${id},
					${version},
					1,
					${versiontime},
					${starttime},
					${endtime},
					${probabilityofoccurrence},
					${source},
					${maintenancetype},
					${cause},
					${comments},
					${location.latitude},
					${location.longitude},
					ST_SetSRID(ST_MakePoint(${location.longitude}, ${location.latitude}), 4326)
				)
				ON CONFLICT ON CONSTRAINT maintenance_pkey DO UPDATE SET active = 1;
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

