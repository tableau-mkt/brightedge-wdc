var module = module || {},
    window = window || {},
    jQuery = jQuery || {},
    Q = Q || {},
    tableau = tableau || {};

module.exports = (function($, Q, tableau) {
  var retriesAttempted = 0,
      maxRetries = 5,
      defaultItemsPerPage = 1000,
      config = {},
      connector,
      wrapper;

  config.name = 'BrightEdge WDC';

  /**
   * Run during initialization of the web data connector.
   *
   * @param {string} phase
   *   The initialization phase. This can be one of:
   *   - tableau.phaseEnum.interactivePhase: Indicates when the connector is
   *     being initialized with a user interface suitable for an end-user to
   *     enter connection configuration details.
   *   - tableau.phaseEnum.gatherDataPhase: Indicates when the connector is
   *     being initialized in the background for the sole purpose of collecting
   *     data.
   *   - tableau.phaseEnum.authPhase: Indicates when the connector is being
   *     accessed in a stripped down context for the sole purpose of refreshing
   *     an OAuth authentication token.
   * @param {function} setUpComplete
   *   A callback function that you must call when all setup tasks have been
   *   performed.
   */
  config.setup = function setup(phase) {

    // You may need to perform set up or other initialization tasks at various
    // points in the data connector flow. You can do so here.
    switch (phase) {
      case tableau.phaseEnum.interactivePhase:
        console.log("inside interactive phase");
        // Perform actual interactive phase stuff.
        break;

      case tableau.phaseEnum.gatherDataPhase:
        console.log("inside gatherDataPhase");
        // Perform set up tasks that should happen when Tableau is attempting to
        // retrieve data from your connector (the user is not prompted for any
        // information in this phase.
        break;

      case tableau.phaseEnum.authPhase:
        console.log("inside authPhase");
        // Perform set up tasks that should happen when Tableau is attempting to
        // refresh OAuth authentication tokens.
        break;
    }

    // Always register when initialization tasks are complete by calling this.
    // This can be especially useful when initialization tasks are asynchronous
    // in nature.
    return Promise.resolve();
  };


  /**
   * Run when the web data connector is being unloaded. Useful if you need
   * custom logic to clean up resources or perform other shutdown tasks.
   *
   * @param {function} tearDownComplete
   *   A callback function that you must call when all shutdown tasks have been
   *   performed.
   */
  config.teardown = function teardown() {
    // Once shutdown tasks are complete, call this. Particularly useful if your
    // clean-up tasks are asynchronous in nature.
    return Promise.resolve();
  };

  /**
   * Primary method called when Tableau is asking for the column headers that
   * this web data connector provides. Takes a single callable argument that you
   * should call with the headers you've retrieved.
   *
   * @param {function(Array<{name, type, incrementalRefresh}>)} registerHeaders
   *   A callback function that takes an array of objects as its sole argument.
   *   For example, you might call the callback in the following way:
   *   registerHeaders([
   *     {name: 'Boolean Column', type: 'bool'},
   *     {name: 'Date Column', type: 'date'},
   *     {name: 'DateTime Column', type: 'datetime'},
   *     {name: 'Float Column', type: 'float'},
   *     {name: 'Integer Column', type: 'int'},
   *     {name: 'String Column', type: 'string'}
   *   ]);
   *
   *   Note: to enable support for incremental extract refreshing, add a third
   *   key (incrementalRefresh) to the header object. Candidate columns for
   *   incremental refreshes must be of type datetime or integer. During an
   *   incremental refresh attempt, the most recent value for the given column
   *   will be passed as "lastRecord" to the tableData method. For example:
   *   registerHeaders([
   *     {name: 'DateTime Column', type: 'datetime', incrementalRefresh: true}
   *   ]);
   */
  config.schema = function defineSchema() {
    return Promise.all([
      Q($.getJSON('/src/schema/keywords.json'))
    ]);
  };

  config.tables = {
    keyword: {
      /**
       * Primary method called when Tableau is asking for your web data connector's
       * data. Takes a callable argument that you should call with all of the
       * data you've retrieved. You may optionally pass a token as a second argument
       * to support paged/chunked data retrieval.
       *
       * @param {function(Array<{object}>, {string})} registerData
       *   A callback function that takes an array of objects as its sole argument.
       *   Each object should be a simple key/value map of column name to column
       *   value. For example, you might call the callback in the following way:
       *   registerData([
       *     {'String Column': 'String Column Value', 'Integer Column': 123}
       *   ]});
       *
       *   It's possible that the API you're interacting with supports some mechanism
       *   for paging or filtering. To simplify the process of making several paged
       *   calls to your API, you may optionally pass a second argument in your call
       *   to the registerData callback. This argument should be a string token that
       *   represents the last record you retrieved.
       *
       *   If provided, your implementation of the tableData method will be called
       *   again, this time with the token you provide here. Once all data has been
       *   retrieved, pass null, false, 0, or an empty string.
       *
       * @param {string} lastRecord
       *   Optional. If you indicate in the call to registerData that more data is
       *   available (by passing a token representing the last record retrieved),
       *   then the lastRecord argument will be populated with the token that you
       *   provided. Use this to update/modify the API call you make to handle
       *   pagination or filtering.
       *
       *   If you indicated a column in wdcw.columnHeaders suitable for use during
       *   an incremental extract refresh, the last value of the given column will
       *   be passed as the value of lastRecord when an incremental refresh is
       *   triggered.
       */
      getData: function getKeywordData(lastRecord) {
        connector = this;
        console.log("entered getKeywordData()");

        //console.log(tableau);

        // console.log("username: " + tableau.username);
        // console.log("password: " + tableau.password);

          var settings = {
                "async": true,
                "crossDomain": true,
                "url": "https://api.brightedge.com/3.0/query/35547",
                "method": "POST",
                "headers": {
                  "authorization": 'Basic ' + btoa(tableau.username + ':' + tableau.password),
                  "content-type": "application/json"
                },
                "data": "query={ \"dataset\":\"keyword\",\r\"dimension\":[\"keyword\", \"time\", \"search_engine\", \"page_url\"]," +
                "\"measures\":[\"blended_rank\"], \"dimensionOptions\":{\"time\":\"weekly\"}, \"filter\":[[\"time\",\"ge\",\"201601\"]],"+
                " \"count\":\"1000\", \"offset\":\"0\"\r}"

            }

            //query=

        console.log(settings);

        return new Promise(function (resolve, reject) {
          console.log("entered getData Promise");

          console.log("inside getData promise -- before initial call");    

          // If a value is passed in for lastRecord, stash it. It means that Tableau
          // is attempting an incremental refresh. We'll use the stashed value as a
          // bound for API requests.
          // if (lastRecord) {
          //   lastKeyword = Number(lastRecord);
          // }

          // Do an initial request to get at the total number of records, then begin to
          // go through all requests.
          getData(buildApiFrom(settings, 0), function initialCall(data) {
            var total = Number(data.total),
                processedData = [];

            Promise.all(prefetchApiUrls(settings, total)).then(function (values) { 
              //console.log("values: " + JSON.stringify(values));
              values.forEach(function (value) {
                processedData = processedData.concat(value.values);
              });
              console.log("length of values: " + values.length);
              resolve(processedData);
            }, function reject(reason) {
              tableau.abortWithError('Unable to fetch data: ' + reason);
              resolve([]);
            });
          });
        })
      },
      /**
       * Transform keyword data into the format expected for the keywords table.
       *
       * @param Object data
       *   Raw data returned from the keywords.getData method.
       *
       * @returns {Promise.<Array<any>>}
       */
      postProcess: function postProcessKeywordData(rawData) {
        console.log("entering postProcess");
        console.log("rawData: " + JSON.stringify(rawData));
        console.log("exiting postProcess");
        return Promise.resolve(rawData);
      }
    }
  };

  // You can write private methods for use above like this:

  /**
   * Helper function to build an API endpoint.
   *
   * @param {object} settings
   *   API Options from which we build a URL and pass a query payload
   *
   * @param {Integer} offsetCount
   *   # to inform paging.
   */
   buildApiFrom = function buildApiFrom(settings, offsetCount) {
    console.log("entering buildApiFrom");
    proxy = '/proxy?endpoint=';

    var updatedSettings = {
      url: proxy + "https://api.brightedge.com/3.0/query/35547",
      method: "POST",
      headers: {
        "authorization": 'Basic ' + btoa(tableau.username + ':' + tableau.password),
        "content-type": "application/json"
      },
      data: "{ \"dataset\":\"keyword\",\r\"dimension\":[\"keyword\", \"time\", \"search_engine\", \"page_url\"]," +
       "\"measures\":[\"blended_rank\"], \"dimensionOptions\":{\"time\":\"weekly\"}, \"filter\":[[\"time\",\"ge\",\"201601\"]]," +
        "\"count\":\"1000\", \"offset\":\"" + offsetCount.toString() + "\"\r}"
    }

    //query=
    //console.log(updatedSettings);
    console.log("leaving buildApiFrom");
    return updatedSettings;
  };

  /**
   * Helper function to return an array of promises
   *
   * @param {object} settings
   *   The BrightEdge POST Payload
   * @param {int} total
   *   The total # of records to retrieve
   *
   * @returns {[]}
   *   An array of promise objects, set to resolve or reject after attempting to
   *   retrieve API data.
   */
  function prefetchApiUrls(settings, total) {
    var urlPromises = [],
        itemsPerPage = 1000,
        maxPromises,
        offsetCount = 0,
        urlPromise;

     console.log("entering prefetchApiURLS");   

    // calculate max promises to return.
    maxPromises = Math.ceil(total / itemsPerPage);


    // Generate query batches.
    while (offsetCount <= total && urlPromises.length < maxPromises) {
      urlPromise = new Promise(function urlPromise(resolve, reject) {
        getData(buildApiFrom(settings, offsetCount), function gotData(data) {
          resolve(data);
        }, function couldNotGetData(reason) {
          reject(reason);
        });
      });

      urlPromises.push(urlPromise);
      offsetCount += itemsPerPage;
    }
    console.log("length of urlPromises array: " + urlPromises.length);
    // console.log("exiting prefetchApiURLS");
    return urlPromises;
  }

  /**
   * AJAX call to our API.
   *
   * @param {Object} settings
   *   The url used for our API payload.
   * @param {function(data)} successCallback
   *   A callback function which takes one argument:
   *     data: result set from the API call.
   * @param {function(reason)} failCallback
   *   A callback which takes one argument:
   *     reason: A string describing why data collection failed.
   */
  function getData(settings, successCallback, failCallback) {
    console.log("getData settings: " + settings);

     $.ajax({
      url: settings.url,
      headers: settings.headers,
      data: settings.data,
      method: "POST",
      success: function dataRetrieved(response) {
        // console.log("getData's success callback has fired!");
        // console.log("dataRetrieved: " + response);
        successCallback(response);
      },
      error: function retrievalFailed(xhr, status, error) {
        if (retriesAttempted <= maxRetries) {
          console.log("getData's failure callback has fired!");
          retriesAttempted++;
          getData(settings, successCallback, failCallback);
        }
        else {
          failCallback('JSON fetch failed too many times for ' + settings.url + '.');
        }
      }
    });
  }


    // Polyfill for btoa() in older browsers.
  // @see https://raw.githubusercontent.com/davidchambers/Base64.js/master/base64.js
  /* jshint ignore:start */
  if (typeof btoa === 'undefined') {
    btoa = function btoa(input) {
      var object = typeof exports != 'undefined' ? exports : this, // #8: web workers
          chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=',
          str = String(input);

      function InvalidCharacterError(message) {
        this.message = message;
      }
      InvalidCharacterError.prototype = new Error;
      InvalidCharacterError.prototype.name = 'InvalidCharacterError';

      for (
        // initialize result and counter
        var block, charCode, idx = 0, map = chars, output = '';
        // if the next str index does not exist:
        //   change the mapping table to "="
        //   check if d has no fractional digits
        str.charAt(idx | 0) || (map = '=', idx % 1);
        // "8 - idx % 1 * 8" generates the sequence 2, 4, 6, 8
        output += map.charAt(63 & block >> 8 - idx % 1 * 8)
      ) {
        charCode = str.charCodeAt(idx += 3 / 4);
        if (charCode > 0xFF) {
          throw new InvalidCharacterError("'btoa' failed: The string to be encoded contains characters outside of the Latin1 range.");
        }
        block = block << 8 | charCode;
      }
      return output;
    };
  }
  /* jshint ignore:end */



  // Instantiate our web data connector.
  wrapper = wdcw(config);
  //console.log(config);
  return config;
  //return wrapper;
})(jQuery, Q, tableau);
