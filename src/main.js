var module = module || {},
    window = window || {},
    jQuery = jQuery || {},
    Q = Q || {},
    tableau = tableau || {};

module.exports = (function($, Q, tableau) {
  var retriesAttempted = 0,
      maxRetries = 10,
      dateRetriesAttempted = 0,
      maxDateRetries = 5,
      defaultItemsPerPage = 1000,
      config = {},
      wrapper,
      acctID,
      startDate,
      endDate,
      dates;


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
        // Perform actual interactive phase stuff.
        setUpInteractivePhase();

        break;

      case tableau.phaseEnum.gatherDataPhase:
        // Perform set up tasks that should happen when Tableau is attempting to
        // retrieve data from your connector (the user is not prompted for any
        // information in this phase.

        //At the very start of the gatherDataPhase, convert the user-inputted date fields, and assign
        //the acctID variable to the user-submitted account id # to be used in the API queries
        try {
          if (!dates && !acctID && phase === tableau.phaseEnum.gatherDataPhase){
            startDate = JSON.parse(tableau.connectionData).startDate;
            endDate = JSON.parse(tableau.connectionData).endDate;
            acctID = JSON.parse(tableau.connectionData).id;

            return new Promise(function(resolve, reject){
              //convert dates from YYYYMMDD to YYYYWW
              getConvertedDates(startDate, endDate)
                .then(function(dateObj){
                  dates = dateObj;
                  resolve(Promise.resolve()); 
                }); 
            }, function(err){
              console.error(err);
            });

          } else {
            return Promise.resolve();
          }
        } catch (e) {
          console.error(e);
          return Promise.resolve();
        }

        break;

      case tableau.phaseEnum.authPhase:
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
      Q($.getJSON('/src/schema/keywords.json')),
      Q($.getJSON('/src/schema/keyword_volume_trending.json'))
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

        var settings = {
              "async": true,
              "crossDomain": true,
              "url": "https://api.brightedge.com/3.0/query/" + acctID,
              "method": "POST",
              "headers": {
                "authorization": 'Basic ' + btoa(tableau.username + ':' + tableau.password),
                "content-type": "application/json"
              },
              "data": "{\"dataset\":\"keyword\",\r\"dimension\":[\"keyword\", \"time\", \"search_engine\", \"page_url\"],"+
              " \"measures\":[\"blended_rank\"], \"dimensionOptions\":{\"time\":\"weekly\"}, \"filter\":[[\"time\",\"ge\",\""+dates.startWeek+"\"],"+
              " [\"time\",\"le\",\""+dates.endWeek+"\"]],\"count\":\"1000\", \"offset\":\""
          }

          return new Promise(function (resolve, reject) {

            //@TODO: Implement Incremental Refresh Functionality
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

                values.forEach(function (value) {
                  processedData = processedData.concat(value.values);
                });

                resolve(processedData);
              }, function reject(reason) {
                tableau.abortWithError('Unable to fetch data: ' + reason);
                resolve([]);
              });
            });
          });
      },
      /**
       * Transform keyword data into the format expected for the keyword table.
       *
       * @param {Object} rawData
       *   Raw data returned from the keyword.getData method.
       *
       * @returns {Promise.<Array<any>>}
       */
      postProcess: function postProcessKeywordData(rawData) {
        return Promise.resolve(rawData);
      }
    },
    keyword_volume_trending: {
       /**
       * Primary method called when Tableau is asking for your web data connector's
       * data. Takes a callable argument that you should call with all of the
       * data you've retrieved. You may optionally pass a token as a second argument
       * to support paged/chunked data retrieval.
       *
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
      getData: function getKeywordVolumeTrendingData(lastRecord){

        var settings = {
              "async": true,
              "crossDomain": true,
              "url": "https://api.brightedge.com/3.0/query/" + acctID,
              "method": "POST",
              "headers": {
                "authorization": 'Basic ' + btoa(tableau.username + ':' + tableau.password),
                "content-type": "application/json"
              },
              "data": "{\"dataset\":\"keyword_volume_trending\",\r\"dimension\":[\"keyword\", \"time\", \"search_engine\"],"+
              " \"measures\":[\"avg_volume\", \"search_volume\"], \"dimensionOptions\":{\"time\":\"monthly\"}, \"filter\":[[\"time\",\"ge\",\""+dates.startMonth+"\"],"+
              " [\"time\",\"le\",\""+dates.endMonth+"\"], [\"search_engine\", [[\"-1\", \"34\"], [\"-1\", \"44\"], [\"-1\", \"102\"], [\"-1\", \"268\"],"+
              " [\"-1\", \"43\"]]]], \"count\":\"1000\", \"offset\":\""
          }

        return new Promise(function (resolve, reject) {

          // If a value is passed in for lastRecord, stash it. It means that Tableau
          // is attempting an incremental refresh. We'll use the stashed value as a
          // bound for API requests.
          //@TODO: Implement Incremental Refresh
          // if (lastRecord) {
          //   lastKeyword = Number(lastRecord);
          // }

          // Do an initial request to get at the total number of records, then begin to
          // go through all requests.
          getData(buildApiFrom(settings, 0), function initialCall(data) {
            var total = Number(data.total),
                processedData = [];

            Promise.all(prefetchApiUrls(settings, total)).then(function (values) { 

              values.forEach(function (value) {
                processedData = processedData.concat(value.values);
              });

              resolve(processedData);
            }, function reject(reason) {
              tableau.abortWithError('Unable to fetch data: ' + reason);
              resolve([]);
            });
          });
        })
      },  
      /**
       * Transform keyword data into the format expected for the keyword_volume_trending table.
       *
       * @param Object data
       *   Raw data returned from the keyword_volume_trending.getData method.
       *
       * @returns {Promise.<Array<any>>}
       */
      postProcess: function postProcessKeywordVolumeTrendingData(rawData){
        return Promise.resolve(rawData);
      }
    }
  };

  // You can write private methods for use above like this:

  /**
   * Actual interactive phase setup code. 
   * Validation check to ensure WDC is not fired with invalid data
   */
  function setUpInteractivePhase() {
    var $modal = $('div.modal'),
        $form = $('form'),
        recoverFromError = function recoverFromError() {
          $modal.find('h3').text('Please fill out all fields before submitting.');
          setTimeout(function () {
            $modal.modal('hide');
          }, 2000);
        };


    // Add a handler to detect missing field values
    $form.submit(function (event) {
      $modal.modal('show');
      
    if ($('#id').val().length === 0 || $('#username').val().length === 0 || $('#password').val().length === 0 ||
     $('#startDate').val().length === 0 || $('#endDate').val().length === 0) {      
        // Prevent the WDCW handler from firing.
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation();
        recoverFromError();
      }
    });

    // Reverse submit bindings on the $form element so our handler above is
    // triggered before the main WDCW handler, allowing us to prevent it.
    $._data($form[0], 'events').submit.reverse();
  };


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
    //express.js proxy endpoint
    var proxy = '/proxy?endpoint=';

    var updatedSettings = {
      url: proxy + settings.url,
      method: "POST",
      headers: settings.headers,
      data: settings.data + offsetCount.toString() + "\"\r}"
    }
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

     $.ajax({
      url: settings.url,
      headers: settings.headers,
      data: settings.data,
      method: "POST",
      success: function dataRetrieved(response) {
        successCallback(response);
      },
      error: function retrievalFailed(xhr, status, error) {
        if (retriesAttempted <= maxRetries) {
          retriesAttempted++;
          getData(settings, successCallback, failCallback);
        }
        else {
          failCallback('JSON fetch failed too many times for ' + settings.url + '.');
        }
      }
    });
  }


  /**
   * Helper function to build an API endpoint for our date conversions
   *
   * @param {Integer} rawDate
   *   YYYYMMDD date to be converted to YYYYWW date
   *
   * @param {String} dateType (e.g: "weekly" or "monthly")
   *   to inform which type of date to return
   */
   buildDateApiFrom = function buildDateApiFrom(rawDate, dateType) {
    var proxy = '/proxy?endpoint=',
      path = 'https://api.brightedge.com/3.0/objects/time/' + acctID +'/';

    var dateConversionSettings = {
      url: proxy + path + dateType + "/" + rawDate.toString(),
      async: true,
      crossDomain: true,
      method: "GET",
      headers: {
        "authorization": 'Basic ' + btoa(tableau.username + ':' + tableau.password),
        "content-type": "application/json"
      },
    }
    return dateConversionSettings;
  };


  /**
   * AJAX GET Request to our API to grab YearWeek formatted dates
   *  from BrightEdge's API
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
  function getYearWeekDate(settings, successCallback, failCallback){
    $.ajax({
        url: settings.url,
        async: true,
        crossDomain: true,
        method: "GET",
        headers: settings.headers,
        success: function dataRetrieved(response) {
          successCallback(response);
        },
        error: function retrievalFailed(xhr, status, error) {
          if (dateRetriesAttempted <= maxDateRetries) {
            dateRetriesAttempted++;
            getYearWeekDate(settings, successCallback, failCallback);
          }
          else {
            failCallback('GET request failed too many times for ' + settings.url + '.');
          }
        }
    }); 
  } 


  /**
   * Function to return convertedDates
   *
   * @param {Integer} startDate
   *   YYYYMMDD date to be converted to YYYYWW date
   *
   * @param {Integer} endDate
   *   YYYYMMDD date to be converted to YYYYWW date
   *
   * @returns {[]}
   *   A Date object, containing the startWeek, StartMonth, endWeek, and endMonth values   
   */
  function getConvertedDates(startDate, endDate){
    var datesObj = {},
      startweek,
      startMonth,
      endWeek,
      endMonth;

    //get start week
    startWeek = new Promise(function(resolve, reject){
      getYearWeekDate(buildDateApiFrom(startDate, "weekly"), function gotDate(data){
        var startWeekObj = {
          "startWeek": data.time_value
        }
        resolve(startWeekObj);
      }, function couldNotGetStartWeek(reason){
        reject(reason);
      });
    });
    
    //get start month
    startMonth = new Promise(function(resolve, reject){
      getYearWeekDate(buildDateApiFrom(startDate, "monthly"), function gotDate(data){
        var startMonthObj = {
          "startMonth": data.time_value
        }
        resolve(startMonthObj);
      }, function couldNotGetStartMonth(reason){
        reject(reason);
      });
    });        

    //get end week
    endWeek = new Promise(function(resolve, reject){
      getYearWeekDate(buildDateApiFrom(endDate, "weekly"), function gotDate(data){
        var endWeekObj = {
          "endWeek": data.time_value
        }
        resolve(endWeekObj);
      }, function couldNotGetEndWeek(reason){
        reject(reason);
      });
    });

    //get end month
    endMonth = new Promise(function(resolve, reject){    
      getYearWeekDate(buildDateApiFrom(endDate, "monthly"), function gotDate(data){
        var endMonthObj = {
          "endMonth": data.time_value
        }
        resolve(endMonthObj);
      }, function couldNotGetEndMonth(reason){
        reject(reason);
      });
    });

    //returns a "datesObj" with all the necessary dates included as key:value pairs
    return new Promise(function(resolve, reject){

      Promise.all([startWeek, startMonth, endWeek, endMonth]).then(function(values){

        values.forEach(function (value) {
          $.extend(datesObj, value);
        });

        resolve(datesObj);
      },
        function reject(reason) {
          tableau.abortWithError('Unable to fetch date conversion data: ' + reason);
          resolve({});
      });
    })
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

  return config;
})(jQuery, Q, tableau);
