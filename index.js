// Generated on 2016-06-21 using generator-web-data-connector 2.0.0

var express = require('express'),
    request = require('request'),
    app = express(),
    bodyParser = require('body-parser'),
    retry = require('retry'),
    port = process.env.PORT || 9001;

// Serve files as if this were a static file server.
app.use(express.static('./'));

// Proxy the index.html file.
app.get('/', function (req, res) {
  res.sendFile('./index.html');
});


app.use(bodyParser.json()); //for parsing application/json

// Create a proxy endpoint.
app.post('/proxy', function (req, res) {
  // Note that the "buildApiFrom(path)" helper in main.js sends the API endpoint
  // as a query parameter to our proxy. We read that in here and build the real
  // endpoint we want to hit.
  setTimeout(function(){
    var opts = {
      retries: 2,
      factor: 2,
      minTimeout: 1 * 1000,
      maxTimeout: 2 * 1000
    };

    var operation = retry.operation(opts);

    console.log("Query Offset: " + JSON.stringify(req.body.offset));
    // console.log("req.query: " + JSON.stringify(req.query));
    // console.log("req.query.endpoint: " + req.query.endpoint);
    // console.log("query="+JSON.stringify(req.body));
    //console.log("auth:" + req.header('authorization'));

    var realPath = req.query.endpoint,
        options = {
          url: realPath,
          body: "query=" + JSON.stringify(req.body),
          method: "POST",
          headers: {
            Accept: 'application/json',
            // Client-side code sends us exactly what we need for authentication.
            Authorization: req.header('authorization'),
            'User-Agent': 'brightedge-wdc/0.0.0'
          }
        };

    // Make an HTTP request using the above specified options.
    console.log('Attempting to proxy request to ' + options.url);

    

    //make the retry attempt
    operation.attempt(function(currentAttempt){

      request(options, function (error, response, body) {
        var header;

        console.log("status code returned: " + response.statusCode);

        if (!error && response.statusCode === 200) {
          // Proxy all response headers.
          for (header in response.headers) {
            if (response.headers.hasOwnProperty(header)) {
              res.set(header, response.headers[header]);
            }
          }

          // Send the response body.
          res.send(body);
       }
        else {
          error = error || response.statusMessage || response.statusCode;
          console.log('Error fulfilling request: "' + error.toString() + '"');
          res.sendStatus(response.statusCode);
        }
      });
    });
  }, req.body.offset / 8)

});

var server = app.listen(port, function () {
  var port = server.address().port;
  console.log('Express server listening on port ' + port);
});

module.exports = app;
