const fs        = require('fs')
  , zlib          = require('zlib')
  , path      = require('path')
  , XmlStream = require('xml-stream')
  , XmlJson = require('xml-json')
  , copy      = require('/usr/local/lib/node_modules/pg-copy-streams')
  , request   = require('/usr/local/lib/node_modules/request');
const ldj = require('ldjson-stream');

let logger = fs.createWriteStream('log.txt');

const DEBUG = false;
function log(message){
	if(DEBUG) 
		console.log(message);
}
function logerror(message){
	console.error(message);
}

//Prepare PG

const { Pool } = require('/usr/local/lib/node_modules/pg')
const pool = new Pool({
	          host: 'localhost',
	          user: 'postgres',
	          database: 'research',
	          port: 5433,
	          max: 20,
	          idleTimeoutMillis: 30000,
	          connectionTimeoutMillis: 2000,
})
pool.on('error', function (err) {
	  logerror('Database error!', err);
});

pool.connect((err, client,done) => {
        if (err){
              logerror('Connecting error: ',err);
        }

        const querystring = copy.from('COPY ndw.trafficspeed_2 FROM STDIN');
        const pgstream = client.query(querystring);
        pgstream.on('error',function(e){
                logerror('Stream error: ',e)
        });
        pgstream.on('end', function() {
                log('Stream closed');
		client.end();
		done();
        });

        let counter = 0;
	let converter = XmlJson('siteMeasurements', {})
	let stream = request.get('http://opendata.ndw.nu/trafficspeed.xml.gz')
		.pipe(zlib.createGunzip())
		.pipe(converter).pipe(ldj.serialize())
	        .on('data',function(d){
			//log(JSON.parse(node));
			node = JSON.parse(d);
		
	
			const time = node.measurementTimeDefault;
			const ref = node.measurementSiteReference;
			const values = node.measuredValue;
			let dataArray = [];
			let countArray = [];
			let speedArray = [];
			let flowArray = [];
			let sumflow = 0;
			let avgflow = -1;
			let nflow = 0;
			let minspeed = 0;
			let maxspeed = 0;
			let avgspeed = 0;
			let nspeed = 0;
			//log(time,ref.$.id);
			values.forEach(function(v,i){
				const basicData = v.measuredValue.basicData;
				if (basicData.vehicleFlow){
					const flow = parseInt(basicData.vehicleFlow.vehicleFlowRate);
					dataArray.push(flow);
					countArray.push(basicData.vehicleFlow.numberOfInputValuesUsed||0);
					if ((values[i+1]&&values[i+1].measuredValue.basicData.averageVehicleSpeed) || (!values[i+1])){
						if (flow>0) flowArray.push(flow);
						/*
						sumflow = flow>0?sumflow+flow:sumflow;
						nflow = flow>0? nflow++:nflow;
						avgflow = flow>0?
							avgflow>0?
								avgflow * (nflow-1)/nflow + flow/nflow
								:flow
							:avgflow;
						*/
					}
					
				}
				else if (basicData.averageVehicleSpeed){
					const speed = parseInt(basicData.averageVehicleSpeed.speed);
					dataArray.push(speed);
					countArray.push(basicData.averageVehicleSpeed.numberOfInputValuesUsed||0);
					if ((values[i+1]&&values[i+1].measuredValue.basicData.vehicleFlow) || (!values[i+1])){
						if (speed > -1) speedArray.push(speed);
						/*
						minspeed = speed>-1&&speed<minspeed?speed:minspeed;
						maxspeed = speed>maxspeed?speed:maxspeed;
						nspeed = speed>-1? nspeed++:nspeed;
						avgspeed = speed>-1?
							avgspeed>-1?
								avgspeed * (nspeed-1)/nspeed + speed/nspeed
								:speed
							:avgspeed;
						*/
					}

				}
			});
			//avgspeed =  isNaN(avgspeed)?0:avgspeed;
			//avgflow = isNaN(avgflow)?0:avgflow;
			if (flowArray.length > 0){
			sumflow = flowArray.reduce(function(sum, value) {
				  return sum + value;
			}, 0);
			avgflow = Math.round(sumflow/flowArray.length);
			//avgflow = isNaN(avgflow)?0:avgflow;
			}
			if (speedArray.length > 0){
			minspeed = speedArray.reduce(function(a, b) {
				    return Math.min(a, b);
			});
			maxspeed = speedArray.reduce(function(a, b) {
				    return Math.max(a, b);
			});
			sumspeed = speedArray.reduce(function(sum, value) {
				  return sum + value;
			});
			
			avgspeed = Math.round(sumspeed/speedArray.length);
			//avgspeed =  isNaN(avgspeed)?0:avgspeed;
			}


			//Stream to db
			var writestring = ref.id + '\t' +
				time + '\t' +
				'{' + dataArray.toString() + '}' + '\t' +
				minspeed + '\t' +
				maxspeed + '\t' +
				avgspeed + '\t' +
				avgflow + '\t' +
				sumflow + '\t' +
				'{' + countArray.toString() + '}' + '\n';
			//logger.write(writestring);
			pgstream.write(writestring);
		})
		.on('end',function(){
			pgstream.end();
			log('Finished reading stream');
		});
	
});
