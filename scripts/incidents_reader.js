var fs        = require('fs')
  , zlib          = require('zlib')
  , path      = require('path')
  , XmlStream = require('/usr/local/lib/node_modules/xml-stream')
  , pg            = require('/usr/local/lib/node_modules/pg')
  , copy      = require('/usr/local/lib/node_modules/pg-copy-streams')
  , request   = require('/usr/local/lib/node_modules/request');




var _db;
//Prepare PG
pg.on('error', function (err) {
  console.log('Database error!', err);
});
var connstring = "tcp://postgres@localhost/research";



pg.connect(connstring, function(err, client) {
                if (err){
              console.log('Connecting error: ',err);
        }
        var querystring = copy.from('COPY ndw.incidents FROM STDIN');
        var pgstream = client.query(querystring);
        pgstream.on('error',function(e){
                        console.log('Stream error: ',e)
        });
        pgstream.on('end', function() {
                        console.log('Stream closed');
        });

        var counter = 0;
                //var stream = fs.createReadStream('trafficspeed.xml');
                var stream = request.get('http://opendata.ndw.nu/incidents.xml.gz')
                        .pipe(zlib.createGunzip());

                var xml = new XmlStream(stream, 'utf8');
                xml.collect('values');
                xml.on('updateElement: situation', function(node) {
                        //console.log(node);
                        var observationtime = node.situationRecord.situationRecordObservationTime;
                        var probabilityofoccurrence = node.situationRecord.probabilityOfOccurrence;
                        var id = node.$.id;
                        var version = node.$.version;
                        var source = node.situationRecord.source.sourceName.values[0].value.$text;
                        var location = node.situationRecord.groupOfLocations.locationForDisplay;
                        var accidenttype = node.situationRecord.accidentType;
                        //console.log(observationtime, id, version, probabilityofoccurrence, source, accidenttype,location);

                        //console.log(counter++,'---------------------------------');

                        //Stream to db
                        var writestring = id + '\t' +
                                version + '\t' +
                                observationtime + '\t' +
                                probabilityofoccurrence + '\t' +
                                source + '\t' +
                                accidenttype + '\t' +
                                location.latitude + '\t' +
                                location.longitude + '\n';
                        //console.log('Copying:', writestring);
                        pgstream.write(writestring);
                });
                xml.on('error', function(message) {
                        console.log('Parsing failed: ' + message);
                });
                xml.on('end',function(){
                        console.log('XML closed');
                        pgstream.end();
                        client.end();
                });
});
