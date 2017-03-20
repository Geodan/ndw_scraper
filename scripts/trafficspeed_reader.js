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
var connstring = "tcp://postgres@localhost:5432/research";



pg.connect(connstring, function(err, client) {
                if (err){
              console.log('Connecting error: ',err);
        }
        var querystring = copy.from('COPY ndw.trafficspeed FROM STDIN');
        var pgstream = client.query(querystring);
        pgstream.on('error',function(e){
                        console.log('Stream error: ',e)
        });
        pgstream.on('end', function() {
                        console.log('Stream closed');
        });

        var counter = 0;
                //var stream = fs.createReadStream('trafficspeed.xml');
                var stream = request.get('http://opendata.ndw.nu/trafficspeed.xml.gz')
                        .pipe(zlib.createGunzip());

                var xml = new XmlStream(stream, 'utf8');
                xml.collect('measuredValue');
                xml.on('updateElement: siteMeasurements', function(node) {
                        var time = node.measurementTimeDefault;
                        var ref = node.measurementSiteReference;
                        var values = node.measuredValue;
                        var flowArray = [];
                        var speedArray = [];
                        //console.log(time,ref.$.id);
                        values.forEach(function(v){
                                var basicData = v.measuredValue[0].basicData;
                                if (basicData.vehicleFlow){
                                         flowArray.push(basicData.vehicleFlow.vehicleFlowRate);
                                        //console.log('Flow' + basicData.vehicleFlow.vehicleFlowRate);
                                }
                                else if (basicData.averageVehicleSpeed){
                                        speedArray.push(basicData.averageVehicleSpeed.speed);
                                        //console.log('Speed:' + basicData.averageVehicleSpeed.speed);
                                }
                        });
                        //console.log(counter++,'---------------------------------');

                        //Stream to db
                        var writestring = ref.$.id + '\t' +
                                time + '\t' +
                                '{' + flowArray.toString() + '}' + '\t' +
                                '{' + speedArray.toString() + '}' + '\n';
                        //console.log('Copying:', writestring);
                        pgstream.write(writestring);
                });
                xml.on('error', function(message) {
                        console.log('Parsing failed: ' + message);
                });
                xml.on('end',function(){
                        //console.log('XML closed');
                        pgstream.end();
                        client.end();
                });
});
