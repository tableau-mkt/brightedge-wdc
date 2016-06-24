/**
 * @file
 *   A utility that wraps the Tableau Web Data Connector API into something with
 *   more opinionated, and simple for beginners. Also takes care of very common
 *   setup and event handling boilerplate.
 */

var wdcw = window.wdcw || {};

(function($, tableau, wdcw) {
  var connector = tableau.makeConnector();

  connector.semaphore = {};

  /**
   * Simplifies the connector.init method in several ways:
   * - Makes it so the implementor doesn't have to know to call the
   *   tableau.initCallback method.
   * - Passes the current phase directly to the initializer so that it doesn't
   *   have to know to pull it from the global tableau object.
   * - Handles population of saved data on behalf of the implementor during the
   *   interactive phase.
   * - Unifies the callback-based API of all connector wrapper methods, and
   *   simplifies asynchronous set-up tasks in the process.
   */
  connector.init = function callConnectorInit(initCallback) {
    var data = this.getConnectionData(),
        $input,
        key;

    // Auto-fill any inputs with known data values.
    if (tableau.phase === tableau.phaseEnum.interactivePhase) {
      for (key in data) {
        if (data.hasOwnProperty(key)) {
          $input = $('*[name="' + key + '"]');
          if ($input.length) {
            if ($input.is(':checkbox')) {
              $input.attr('checked', !!data[key]).change();
            }
            else {
              $input.val(data[key]).change();
            }
          }
        }
      }

      // Pre-populate username and password if stored values exist.
      if (tableau.username) {
        $('input[name="username"]').val(tableau.username);
      }
      if (tableau.password) {
        $('input[type="password"]').val(tableau.password);
      }
    }

    // If the provided connector wrapper has a setup property, call it with the
    // current initialization phase.
    if (wdcw.hasOwnProperty('setup')) {
      wdcw.setup.call(this, tableau.phase, function setUpComplete() {
        initCallback();
      });
    }
    else {
      initCallback();
    }
  };

  /**
   * Simplifies the connector.shutDown method in a couple of ways:
   * - Makes it so that the implementor doesn't have to know to call the
   *   tableau.shutdownCallback method.
   * - Mirrors the wrapped init callback for naming simplicity (setup/teardown).
   * - Unifies the callback-based API of all connector wrapper methods, and
   *   simplifies asynchronous tear-down tasks in the process.
   */
  connector.shutDown = function callConnectorShutdown() {
    // If the provided connector wrapper has a teardown property, call it.
    if (wdcw.hasOwnProperty('teardown')) {
      wdcw.teardown.call(this, function shutDownComplete() {
        tableau.shutdownCallback();
      });
    }
    else {
      tableau.shutdownCallback();
    }
  };

  /**
   * Simplifies the connector.getSchema method in a few ways:
   * - Enables simpler asynchronous handling, expecting a promise in return.
   */
  connector.getSchema = function callConnectorGetSchema(schemaCallback) {
    wdcw.schema.call(this)
      .then(schemaCallback, connector.promiseErrorWrapper)
      .catch(connector.promiseErrorWrapper);
  };

  /**
   * Simplifies (and limits) the connector.getTableData method in a couple ways:
   * - Enables simpler asynchronous handling by providing a callback.
   * - Simplifies chunked/paged data handling by limiting the arguments that the
   *   implementor needs to be aware of to just 1) the data retrieved and 2) if
   *   paging functionality is needed, a token for the last record.
   * - Makes it so the implementor doesn't have to know to call the
   *   tableau.dataCallback method.
   *
   * @param {string} lastRecordToken
   *   The token provided by the implementor representing the last record or
   *   page retrieved.
   */
  connector.getData = function callConnectorGetData(table, doneCallback) {
    var connector = this,
        tableId = table.tableInfo.id,
        dependencies = table.tableInfo.dependsOn,
        postProcess,
        dependentPromise;

    // Make sure we've got a data callback to call.
    if (!wdcw.tables.hasOwnProperty(tableId) || typeof wdcw.tables[tableId].getData !== 'function') {
      tableau.abortWithError('Data callback missing for table: ' + tableId);
      return doneCallback();
    }

    // If the implementor provides a post-process callback, give it to them!
    if (wdcw.tables[tableId].hasOwnProperty('postProcess') && typeof wdcw.tables[tableId].postProcess === 'function') {
      postProcess = wdcw.tables[tableId].postProcess;
    }
    // Otherwise, just pass the data through.
    else {
      postProcess = function passThru(data) {return Promise.resolve(data);};
    }

    // If this table depends on others, chain it on their completion.
    if (dependencies) {
      try {
        dependentPromise = Promise.all(dependencies.map(connector.getDataPromise));
      }
      catch (e) {
        tableau.abortWithError('Attempted to gather dependent table data before its requisites. Make sure your table definitions are in order.');
        tableau.abortWithError(e);
        return doneCallback();
      }
    }
    // Otherwise, if this table has no dependencies, proceed immediately.
    else {
      dependentPromise = Promise.resolve();
    }

    dependentPromise
      .then(function (dependentResults) {
        return connector.setDataPromise(
          tableId,
          wdcw.tables[tableId].getData.call(
            connector,
            table.incrementValue,
            dependentResults,
            table.appendRows
          )
        );
      }, connector.promiseErrorWrapper)
      .then(postProcess, connector.promiseErrorWrapper)
      .then(function (results) {
        table.appendRows(results || []);
        doneCallback();
      }, connector.promiseErrorWrapper)
      .catch(connector.promiseErrorWrapper);
  };

  connector.setDataPromise = function setDataPromise(tableId, promise) {
    return connector.semaphore[tableId] = promise;
  };

  connector.getDataPromise = function getDataPromise(tableId) {
    if (!connector.semaphore.hasOwnProperty(tableId)) {
      throw 'Could not find data gathering semaphore for table: ' + tableId;
    }
    return connector.semaphore[tableId];
  };

  /**
   * Extension of the web data connector API that handles complex connection
   * data getting for the implementor.
   *
   * @returns {object}
   *   An object representing connection data. Keys are assumed to be form input
   *   names; values are assumed to be form input values.
   *
   * @see connector.setConnectionData
   */
  connector.getConnectionData = function getConnectionData() {
    return tableau.connectionData ? JSON.parse(tableau.connectionData) : {};
  };

  /**
   * Extension of the web data connector API that handles complex connection
   * data setting for the implementor.
   *
   * @param {object} data
   *   The data that's intended to be set for this connection. Keys are assumed
   *   to be form input names; values are assumed to be form input values.
   *
   * @returns {object}
   *   Returns the data that was set.
   *
   * @see connector.getConnectionData
   */
  connector.setConnectionData = function setConnectionData(data) {
    tableau.connectionData = JSON.stringify(data);
    return data;
  };

  /**
   * Extension of the web data connector API that gets the connection username.
   *
   * @returns {string}
   *   The username associated with this connection.
   */
  connector.getUsername = function getUsername() {
    return tableau.username;
  };

  /**
   * Extension of the web data connector API that sets the connection username.
   *
   * @param {string} username
   *   The username to be associated with this connection.
   *
   * @returns {string}
   *   The username now associated with this connection.
   */
  connector.setUsername = function setUsername(username) {
    tableau.username = username;
    return tableau.username;
  };

  /**
   * Extension of the web data connector API that gets the connection password.
   *
   * @returns {string}
   *   The password associated with this connection.
   */
  connector.getPassword = function getPassword() {
    return tableau.password;
  };

  /**
   * Extension of the web data connector API that sets the connection password.
   *
   * @param {string} password
   *   The password or other sensitive connection information to be associated
   *   with this connection. The value is encrypted and stored by tableau.
   *
   * @returns {string}
   *   The password now associated with this connection.
   */
  connector.setPassword = function setPassword(password) {
    tableau.password = password;
    return tableau.password;
  };

  /**
   * Extension of the web data connector API that gets the incremental extract
   * column.
   */
  connector.getIncrementalExtractColumn = function getIncrementalExtractColumn() {
    return tableau.incrementalExtractColumn;
  };

  /**
   * Extension of the web data connectors API that sets the incremental extract
   * column.
   *
   * @param {string} column
   *   The column from the data source on which incremental extracts should be
   *   based. This can be a column that represents either a DateTime or an
   *   integer.
   */
  connector.setIncrementalExtractColumn = function setIncrementalExtractColumn(column) {
    tableau.incrementalExtractColumn = column;
    return column;
  };

  /**
   * A generic error handler that can be used by implementors for simplicity.
   *
   * @param {object} jqXHR
   * @param {string} textStatus
   * @param {string} errorThrown
   */
  connector.ajaxErrorHandler =  function ajaxErrorHandler(jqXHR, textStatus, errorThrown) {
    var message = 'There was a problem retrieving data: "' +
          textStatus + '" with error thrown: "' +
          errorThrown + '"';

    tableau.abortWithError(message);
  };

  connector.promiseErrorWrapper = function tableauErrorWrapper(e) {
    if (typeof e === 'string') {
      tableau.abortWithError(e);
    }
    else {
      tableau.abortWithError(JSON.stringify(e));
    }
  };

  // Register our connector, which uses logic from the connector wrapper.
  tableau.registerConnector(connector);

  /**
   * Register a submit handler and take care of the following on behalf of the
   * implementor:
   * - Parse and store form data in tableau's connection data property.
   * - Provide the connection name.
   * - Trigger the data collection phase of the web data connector.
   */
  $(document).ready(function connectorDocumentReady() {
    $('form').submit(function connectorFormSubmitHandler(e) {
      var $fields = $('input, select, textarea').not('[type="password"],[type="submit"],[name="username"]'),
          $password = $('input[type="password"]'),
          $username = $('input[name="username"]'),
          data = {};

      e.preventDefault();

      // Format connection data according to assumptions.
      $fields.map(function getValuesFromFields() {
        var $this = $(this);
            name = $this.attr('name');
        if (name) {
          if ($this.is(':checkbox')) {
            data[name] = $this.is(':checked');
          }
          else {
            data[name] = $this.val();
          }
        }
        return this;
      });

      // If nothing was entered, there was a problem. Abort.
      // @todo Automatically add validation handling.
      if ($fields.length && data === {}) {
        return false;
      }

      // Set connection data and connection name.
      connector.setConnectionData(data);
      tableau.connectionName = 'Travis CI';

      // If there was a password, set the password.
      if ($password.length) {
        connector.setPassword($password.val());
      }

      // If there was a username, set the username.
      if ($username.length) {
        connector.setUsername($username.val());
      }

      // Initiate the data retrieval process.
      tableau.submit();
    });
  });

})(jQuery, tableau, wdcw);
