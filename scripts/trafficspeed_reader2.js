var fs        = require('fs')
  , zlib 	  = require('zlib')
  , path      = require('path')
  , XmlStream = require('/usr/local/lib/node_modules/xml-stream')
  , copy      = require('/usr/local/lib/node_modules/pg-copy-streams')
  , request   = require('/usr/local/lib/node_modules/request')  
  , querystring = require('querystring')
  , http = require('http');


  
         var post_options = {
      		host: 'gost.geodan.nl',
      		port: '80',
      		path: '/v1.0/Datastreams(13)',
      		method: 'POST',
      		headers: {
	          'Content-Typie': 'application/json',
		  'Accept': 'application/json'
      		}
  	};
        var counter = 0;
		//var stream = fs.createReadStream('trafficspeed.xml');
	var stream = request.get('http://opendata.ndw.nu/trafficspeed.xml.gz')  
		.pipe(zlib.createGunzip());

	var xml = new XmlStream(stream, 'utf8');
	console.log('Opening stream');
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
		//pgstream.write(writestring);

		if (ref.$.id == 'RWS01_MONIBAS_0101hrl0205ra'){
		    var post_data = {
			  "result" : flowArray[0]
		   };
		console.log('posting ', post_data);
		   var post_req = http.request(post_options, function(res) {
		      res.setEncoding('utf8');
		      res.on('data', function (chunk) {
		          console.log('Response: ' + chunk);
		      });
		   });

		  // post the data
		  post_req.write(post_data);
		  post_req.end();

		}

	});
	xml.on('error', function(message) {
		console.log('Parsing failed: ' + message);
	});
	xml.on('end',function(){
		//console.log('XML closed');
	});
