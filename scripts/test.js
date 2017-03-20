var  http = require('http');

 var post_options = {
                host: 'gost.geodan.nl',
                port: '80',
                path: '/v1.0/Datastreams(13)/Observations',
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                  'Accept': 'application/json'
                }
        };
	var post_data = JSON.stringify({
              "result" : 999
        });
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

