var pg = require ('pg');
var http = require('http');
var rp = require('/usr/local/lib/node_modules/request-promise');
http.globalAgent.keepAlive = true

pg.connect("tcp://postgres@localhost:5432/research", function(err, client) {
    if(err) {
        console.log(err);
    }
    console.log('Listening for inserts in trafficspeed');
    client.on('notification', function(msg) {
	process.stderr.write('.')
        if (msg.name === 'notification' && msg.channel === 'observation_insert') {
            var pl = JSON.parse(msg.payload);
			var options = {
				method: 'POST',
				uri: 'http://gost.geodan.nl/v1.0/Datastreams('+pl.id+')/Observations',
				body: {
					result: pl.result
				},
				json: true // Automatically stringifies the body to JSON 
			};
			rp(options)
				.then(function (parsedBody) {
					//console.log(parsedBody);
				})
				.catch(function(err){
					console.log('Problem with' ,err.options.uri, JSON.stringify(err.options.body));
				});
        
        }
        
    });
    client.query("LISTEN observation_insert");
});
