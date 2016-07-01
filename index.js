// Generated on 2016-06-21 using generator-web-data-connector 2.0.0

var express = require('express'),
    limit = require('simple-rate-limiter'),
    request = limit(require('request')).to(1).per(900),
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


app.post('/proxy', function (req, res) {
  // Note that the "buildApiFrom(path)" helper in main.js sends the API endpoint
  // as a query parameter to our proxy. We read that in here and build the real
  // endpoint we want to hit.

  var opts = {
    retries: 5,
    factor: 2,
    minTimeout: 1 * 1000,
    maxTimeout: 2 * 1000
  };

  var operation = retry.operation(opts);

  //console.log("req: " + JSON.stringify(req));
  console.log("Query Offset: " + JSON.stringify(req.body.offset));
  // console.log("req.query: " + JSON.stringify(req.query));
  // console.log("req.query.endpoint: " + req.query.endpoint);
  //console.log("body: "+JSON.stringify(req.body));
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
  
    //make the retry attempt
    operation.attempt(function(currentAttempt){

      // Make an HTTP request using the above specified options.
      console.log('Attempting to proxy request to ' + options.url);

      request(options, function (error, response, body) {
        var header;

        //console.log("status code returned: " + response.statusCode);

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
});




// Create a proxy endpoint for a GET request
app.get('/proxy', function (req, res) {
  // Note that the "buildDateApiFrom(path)" helper in main.js sends the API endpoint
  // as a query parameter to our proxy. We read that in here and build the real
  // endpoint we want to hit.

  var opts = {
    retries: 5,
    factor: 2,
    minTimeout: 1 * 1000,
    maxTimeout: 2 * 1000
  };

  var operation = retry.operation(opts);

  //console.log("date req.query: " + JSON.stringify(req.query));

  var realPath = req.query.endpoint,
      options = {
        url: realPath,
        method: "GET",
        headers: {
          Accept: 'application/json',
          // Client-side code sends us exactly what we need for authentication.
          Authorization: req.header('authorization'),
          'User-Agent': 'brightedge-wdc/0.0.0'
        }
      };



  //make the retry attempt
  operation.attempt(function(currentAttempt){
  
    // Make an HTTP request using the above specified options.
    console.log('Attempting to proxy date request to ' + options.url);

    request(options, function (error, response, body) {
      var header;

      console.log("status code returned: " + response.statusCode);
      // console.log("response: " + JSON.stringify(response));
      // console.log("response body: " + JSON.stringify(body));

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
        console.log('Error fulfilling GET request: "' + error.toString() + '"');
        res.sendStatus(response.statusCode);
      }
    });
  });
});

var server = app.listen(port, function () {
  var port = server.address().port;
  console.log('Express server listening on port ' + port);
});

module.exports = app;
