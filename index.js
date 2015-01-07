var exec = require('child_process').exec;
var spawn = require('child_process').spawn;
var temp = require('temp');
var fs = require('fs');
var path = require('path');
var util = require('./lib/util');
var DatastoreError = require('./lib/errors').DatastoreError;
var createJSendClientValidationError = require('./lib/jsend').createJSendClientValidationError;
var createJSendServerError = require('./lib/jsend').createJSendServerError;
var log = require('log4js').getLogger("bodytrack-datastore");

const DATASTORE_EXECUTABLES = ['export', 'gettile', 'import', 'info'];
const VALID_KEY_CHARACTERS_PATTERN = /^[a-zA-Z0-9_\.\-]+$/;

//======================================================================================================================
// CLASS DEFINITION
//======================================================================================================================

/**
 * <p>
 * Creates a BodyTrackDatastore instance using the given config.  The config must contain fields <code>binDir</code> and
 * <code>dataDir</code>. The <code>binDir</code> field should be a path pointing to the directory containing the various
 * BodyTrack Datastore binary executables (e.g. <code>export</code>, <code>gettile</code>, <code>import</code>, etc).
 * The <code>dataDir</code> field should be a path pointing to your BodyTrack Datastore installation's data directory
 * (typically named <code>dev.kvs</code>).
 * </p>
 * <p>
 * Throws an exception if the config argument is <code>undefined</code> or <code>null</code>, or if either or both of
 * the config's expected fields is <code>undefined</code> or <code>null</code>.
 * </p>
 *
 * @param {object} config - the configuration object
 * @constructor
 */
function BodyTrackDatastore(config) {

   if (!util.isDefined(config)) {
      throw new Error("config cannot be undefined or null");
   }
   else if (!util.isDefined(config.binDir)) {
      throw new Error("config.binDir cannot be undefined or null");
   }
   else if (!util.isDefined(config.dataDir)) {
      throw new Error("config.dataDir cannot be undefined or null");
   }
   // PRIVATE METHODS

   // joining with the . ensures there's no trailing slash
   var binDir = path.join(util.isDefined(config) ? config.binDir : '', '.');
   var dataDir = path.join(util.isDefined(config) ? config.dataDir : '', '.');

   var buildCommand = function(command, parameters) {
      // surround the command and data directory with single quotes to deal with paths containing spaces
      var launchCommand = "'" + path.join(binDir, command, '.') + "' '" + dataDir + "'";
      parameters = parameters || [];
      for (var i = 0; i < parameters.length; i++) {
         var param = parameters[i];
         launchCommand += ' ';
         var part = (param == null ? 'null' : param.toString());
         if (part.indexOf(' ') < 0) {
            launchCommand += part;
         }
         else {
            launchCommand += "\"" + part + "\"";
         }
      }

      return launchCommand;
   };

   var executeCommand = function(commandName, parameters, callback) {
      var command = buildCommand(commandName, parameters);
      log.debug("executing command: " + command);
      exec(command, callback);
   };

   var isUserIdValid = function(userId) {
      return util.isInt(userId);
   };

   // PRIVILEGED METHODS

   /**
    * Performs simple validation on the config given to the constructor.  Returns <code>true</code> if the paths exist,
    * if they actually point to directories, and if the <code>binDir</code> contains the expected executables.
    *
    * @returns {boolean}
    */
   this.isConfigValid = function() {

      if (!fs.existsSync(dataDir)) {
         log.error("BodyTrackDatastore.isConfigValid(): the data directory (" + dataDir + ") does not exist");
         return false;
      }

      if (!fs.existsSync(binDir)) {
         log.error("BodyTrackDatastore.isConfigValid(): the bin directory (" + binDir + ") does not exist");
         return false;
      }

      if (!fs.statSync(dataDir).isDirectory()) {
         log.error("BodyTrackDatastore.isConfigValid(): the data directory (" + dataDir + ") is not a directory");
         return false;
      }

      if (!fs.statSync(binDir).isDirectory()) {
         log.error("BodyTrackDatastore.isConfigValid(): the bin directory (" + binDir + ") is not a directory");
         return false;
      }

      // finally, make sure the executables exist and are files
      for (var i = 0; i < DATASTORE_EXECUTABLES.length; i++) {
         var exePath = path.join(binDir, DATASTORE_EXECUTABLES[i]);
         if (!fs.existsSync(exePath)) {
            log.error("BodyTrackDatastore.isConfigValid(): executable (" + exePath + ") does not exist");
            return false;
         }
         var exeStat = fs.statSync(exePath);
         if (!exeStat.isFile()) {
            log.error("BodyTrackDatastore.isConfigValid(): executable (" + exePath + ") is not a file");
            return false;
         }
      }

      return true;
   };

   /**
    * <p>
    * Produces the "channel_specs" JSON either for all devices and channels, for all channels within a single device,
    * or for only the specified device and channel.  Min and/or max time may be optionally specified as well. Produced
    * channel specs are returned to the caller by calling the <code>callback</code> function.
    * </p>
    * <p>
    * To filter the info, the given <code>filter</code> object must at least contain a <code>userId</code> field. The
    * optional fields are:
    * <ul>
    *    <li><code>deviceName</code></li>
    *    <li><code>channelName</code></li>
    *    <li><code>minTime</code></li>
    *    <li><code>maxTime</code></li>
    * </ul>
    * The <code>channelName</code> will only be considered if the <code>deviceName</code> is specified.
    * </p>
    * <p>
    * The callback is called with a <code>DatastoreError</code> if:
    * <ul>
    *    <li>the user ID is not specified</li>
    *    <li>the user ID is invalid</li>
    *    <li>the device name (if specified) is invalid</li>
    *    <li>the channel name (if specified) is invalid</li>
    *    <li>the min time (if specified) is invalid</li>
    *    <li>the max time (if specified) is invalid</li>
    * </ul>
    * The DatastoreError given to the callback will contain a JSend compliant object in the <code>data</code> property
    * with more details about the error.
    * </p>
    *
    * @param {object} filter Object containing the various filter parameters
    * @param {function} callback - callback function with the signature <code>callback(err, info)</code>
    * @throws an <code>Error</code> if the method is called with fewer than 2 arguments
    */
   this.getInfo = function(filter, callback) {
      // make sure they at least specified the userId
      if (filter.hasOwnProperty('userId')) {

         var userId = filter['userId'];

         // make sure the userId is valid
         if (!isUserIdValid(userId)) {
            var msg = "User ID must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
         }

         var parameters = ["-r", userId];

         // see whether deviceName is specified
         if (filter.hasOwnProperty('deviceName') && filter['deviceName'] != null) {
            var deviceName = filter['deviceName'];

            // make sure the deviceName is valid
            if (!BodyTrackDatastore.isValidKey(deviceName)) {
               var msg = "Invalid device name";
               return callback(new DatastoreError(createJSendClientValidationError(msg, { deviceName : msg })));
            }

            // see whether channelName is specified
            if (filter.hasOwnProperty('channelName') && filter['channelName'] != null) {
               var channelName = filter['channelName'];

               // make sure the channelName is valid
               if (!BodyTrackDatastore.isValidKey(channelName)) {
                  var msg = "Invalid channel name";
                  return callback(new DatastoreError(createJSendClientValidationError(msg, { channelName : msg })));
               }

               // Both deviceName and channelName are specified and valid,
               // so do a query using userId, deviceName, and channelName
               parameters.push("--prefix");
               parameters.push(deviceName + "." + channelName);
            }
            else {
               // The deviceName was specified, but not the channelName,
               // so do a query only using userId and deviceName
               parameters.push("--prefix");
               parameters.push(deviceName);
            }
         }

         // Now that device and channel have been handled, check for filtering by min/max time...

         // see whether the caller specified the min time
         if (filter.hasOwnProperty('minTime') && filter['minTime'] != null) {
            var minTime = filter['minTime'];

            // make sure the minTime is valid
            if (!util.isNumber(minTime)) {
               var msg = "Invalid min time";
               return callback(new DatastoreError(createJSendClientValidationError(msg, { minTime : msg })));
            }
            parameters.push("--min-time");
            parameters.push(minTime);
         }

         // see whether the caller specified the max time
         if (filter.hasOwnProperty('maxTime') && filter['maxTime'] != null) {
            var maxTime = filter['maxTime'];

            // make sure the maxTime is valid
            if (!util.isNumber(maxTime)) {
               var msg = "Invalid max time";
               return callback(new DatastoreError(createJSendClientValidationError(msg, { maxTime : msg })));
            }
            parameters.push("--max-time");
            parameters.push(maxTime);
         }

         // FINALLY, execute the command!
         executeCommand("info", parameters,
                        function(err, stdout) {
                           if (err) {
                              return callback(new DatastoreError(createJSendServerError('Failed to call info', err)));
                           }
                           return callback(null, JSON.parse(stdout));
                        });
      }
      else {
         var msg = "User ID is required";
         return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
      }
   };

   /**
    * <p>
    * Exports data from the specified channel(s) for the given device as CSV, optionally filtered by min and max time.
    * Data is returned to the caller as an EventEmitter given to the <code>callback</code> function.
    * </p>
    * <p>
    * To filter the data by time, the given <code>filter</code> object may contain a <code>minTime</code> and/or a
    * <code>maxTime</code>.
    * <ul>
    *    <li><code>minTime</code></li>
    *    <li><code>maxTime</code></li>
    * </ul>
    * </p>
    * <p>
    * The callback is called with a <code>DatastoreError</code> if:
    * <ul>
    *    <li>the user ID is invalid</li>
    *    <li>the device name (if specified) is invalid</li>
    *    <li>the channels array is undefined, null, empty, or otherwise not an array</li>
    *    <li>the min time (if specified) is invalid</li>
    *    <li>the max time (if specified) is invalid</li>
    * </ul>
    * The DatastoreError given to the callback will contain a JSend compliant object in the <code>data</code> property
    * with more details about the error.
    * </p>
    * <p>
    * Other things to note:
    * <ul>
    *    <li>If the min time is greater than the max time, no error is thrown, but no data (other than the header line) will be returned.</li>
    *    <li>Duplicate requests for the same channel are ignored.</li>
    *    <li>
    *       Invalid channel names are ignored.  That is, if a requested channel name has invalid channel name syntax,
    *       it is ignored.  A requested channel name that has valid syntax but simply doesn't exist for the specified
    *       device will not be ignored, but will obviously not have any data.
    *    </li>
    * </ul>
    * </p>
    *
    * @param {int} userId - the user ID
    * @param {string} deviceName - the device name
    * @param {Array} channels - the channel name(s) as an array of strings. Can be null.
    * @param {object} filter Object containing the various filter parameters
    * @param {function} callback - callback function with the signature <code>callback(err, eventEmitter)</code>
    */
   this.exportData = function(userId, deviceName, channels, filter, callback) {

      // make sure the userId is valid
      if (!isUserIdValid(userId)) {
         var msg = "User ID must be an integer";
         return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
      }

      // make sure the deviceName is valid
      if (!BodyTrackDatastore.isValidKey(deviceName)) {
         var msg = "Invalid device name";
         return callback(new DatastoreError(createJSendClientValidationError(msg, { deviceName : msg })));
      }

      // Make sure channels is an array
      if (!Array.isArray(channels)) {
         var msg = "The channels argument must be an array";
         return callback(new DatastoreError(createJSendClientValidationError(msg, { channels : msg })));
      }

      // scrub the channels, removing dupes and invalids, but preserving the requested order of the unique ones
      var alreadyIncludedChannels = {};
      channels = channels.filter(function(channel) {
         if (util.isString(channel)) {
            channel = channel.trim();
            if (BodyTrackDatastore.isValidKey(channel)) {
               var isNew = !(channel in alreadyIncludedChannels);
               alreadyIncludedChannels[channel] = true;
               return isNew;
            }
         }
         return false;
      });
      if (channels.length <= 0) {
         var msg = "Must specify at least one channel to export";
         return callback(new DatastoreError(createJSendClientValidationError(msg, { channels : msg })));
      }

      // always request CSV from the datastore. If the user wants JSON, we have to do the conversion to JSON in Node
      var parameters = ['--csv'];

      // see whether the caller specified the min time
      filter = filter || {};
      if (filter.hasOwnProperty('minTime') && filter['minTime'] != null) {
         var minTime = filter['minTime'];

         // make sure the minTime is valid
         if (!util.isNumber(minTime)) {
            var msg = "Invalid min time";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { minTime : msg })));
         }
         parameters.push("--start");
         parameters.push(minTime);
      }

      // see whether the caller specified the max time
      if (filter.hasOwnProperty('maxTime') && filter['maxTime'] != null) {
         var maxTime = filter['maxTime'];

         // make sure the maxTime is valid
         if (!util.isNumber(maxTime)) {
            var msg = "Invalid max time";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { maxTime : msg })));
         }
         parameters.push("--end");
         parameters.push(maxTime);
      }

      parameters.push(dataDir);
      parameters.push(userId);

      channels.forEach(function(channel) {
         parameters.push(deviceName + "." + channel);
      });

      // FINALLY, spawn the command, and return to the caller.
      var exportExe = path.join(binDir, 'export', '.');
      var command = spawn(exportExe, parameters);
      return callback(null, command);
   };

   /**
    * <p>
    * Returns the tile for the given <code>user</code>, <code>device</code>, <code>channel</code>, <code>level</code>,
    * and <code>offset</code>.  The tile is returned to the caller by calling the <code>callback</code> function.
    * </p>
    * <p>
    * The callback is called with a <code>DatastoreError</code> if:
    * <ul>
    *    <li>the user ID is not an integer</li>
    *    <li>the device name is invalid</li>
    *    <li>the channel name is invalid</li>
    *    <li>the level is not an integer</li>
    *    <li>the offset is not an integer</li>
    * </ul>
    * The DatastoreError given to the callback will contain a JSend compliant object in the <code>data</code> property
    * with more details about the error.
    * </p>
    *
    * @param {int} userId - the user ID
    * @param {string} deviceName - the device name
    * @param {string} channelName - the channel name
    * @param {int} level - the tile level
    * @param {int} offset - the tile offset
    * @param {function} callback - callback function with the signature <code>callback(err, tile)</code>
    */
   this.getTile = function(userId, deviceName, channelName, level, offset, callback) {
      if (typeof callback === 'function') {
         // validate inputs
         if (!isUserIdValid(userId)) {
            var msg = "User ID must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
         }
         if (!util.isInt(level)) {
            var msg = "Level must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { level : msg })));
         }
         if (!util.isInt(offset)) {
            var msg = "Offset must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { offset : msg })));
         }
         if (!BodyTrackDatastore.isValidKey(deviceName)) {
            var msg = "Invalid device name";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { deviceName : msg })));
         }
         if (!BodyTrackDatastore.isValidKey(channelName)) {
            var msg = "Invalid channel name";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { channelName : msg })));
         }

         var parameters = [userId,
                           deviceName + "." + channelName,
                           level,
                           offset];

         executeCommand("gettile", parameters,
                        function(err, stdout) {
                           if (err) {
                              return callback(new DatastoreError(createJSendServerError('Failed to call get tile', err)));
                           }

                           return callback(null, JSON.parse(stdout));
                        });
      }
   };

   /**
    * <p>
    * Returns the tiles for the given <code>userIdDeviceChannelObjects</code>, <code>level</code>, and <code>offset</code>.  The
    * tiles are returned to the caller as an EventEmitter given to the <code>callback</code> function. The
    * <code>userIdDeviceChannelObjects</code> argument must be a non-empty array of objects, where each object contains of the
    * form: <code>{userId: USER_ID, deviceName: "DEVICE_NAME", channelName: "CHANNEL_NAME"}</code>.  The
    * <code>userId</code> field must be an integer, and the <code>deviceName</code> and <code>channelName</code> fields
    * must be strings.
    * </p>
    * <p>
    * The callback is called with a <code>DatastoreError</code> if:
    * <ul>
    *    <li>the userIdDeviceChannelObjects is not an non-empty array of objects</li>
    *    <li>any object in the userIdDeviceChannelObjects array contains invalid fields</li>
    *    <li>the level is not an integer</li>
    *    <li>the offset is not an integer</li>
    * </ul>
    * The DatastoreError given to the callback will contain a JSend compliant object in the <code>data</code> property
    * with more details about the error.
    * </p>
    * <p>
    * NOTE: At this time, only numeric channels will return data.  Non-numeric channels will always return <code>null</code> values.
    * </p>
    *
    * @param {Array} userIdDeviceChannelObjects - a non-empty array of objects. See above for details.
    * @param {int} level - the tile level
    * @param {int} offset - the tile offset
    * @param {function} callback - callback function with the signature <code>callback(err, eventEmitter)</code>
    */
   this.getTiles = function(userIdDeviceChannelObjects, level, offset, callback) {
      if (typeof callback === 'function') {
         // validate inputs
         var userIdDeviceChannels = [];

         if (Array.isArray(userIdDeviceChannelObjects) && userIdDeviceChannelObjects.length > 0) {
            // check each item in the array to make sure they're all strings.
            for (var i = 0; i < userIdDeviceChannelObjects.length; i++) {
               var o = userIdDeviceChannelObjects[i];
               var userId = o.userId;
               var deviceName = o.deviceName;
               var channelName = o.channelName;
               if (util.isObject(o)) {
                  var userIdDeviceChannel = userId + "." + deviceName + "." + channelName;

                  if (!isUserIdValid(userId)) {
                     var msg = "User ID must be an integer in [" + userIdDeviceChannel + "]";
                     return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
                  }
                  if (!BodyTrackDatastore.isValidKey(deviceName)) {
                     var msg = "Invalid device name [" + deviceName + "] in [" + userIdDeviceChannel + "]";
                     return callback(new DatastoreError(createJSendClientValidationError(msg, { deviceName : msg })));
                  }
                  if (!BodyTrackDatastore.isValidKey(channelName)) {
                     var msg = "Invalid channel name [" + channelName + "] in [" + userIdDeviceChannel + "]";
                     return callback(new DatastoreError(createJSendClientValidationError(msg, { channelName : msg })));
                  }

                  // if no error by now, then it's valid, so hang on to it
                  userIdDeviceChannels.push(userIdDeviceChannel);
               }
               else {
                  var msg = "userIdDeviceChannelObjects array must contain only objects";
                  return callback(new DatastoreError(createJSendClientValidationError(msg, { userDeviceChannels : msg })));
               }
            }
         }
         else {
            var msg = "userIdDeviceChannelObjects must be a non-empty array of objects";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { userDeviceChannels : msg })));
         }
         if (!util.isInt(level)) {
            var msg = "Level must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { level : msg })));
         }
         if (!util.isInt(offset)) {
            var msg = "Offset must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { offset : msg })));
         }

         var parameters = [dataDir,
                           '--multi',
                           userIdDeviceChannels.join(','),
                           level,
                           offset];

         // FINALLY, spawn the command, and return to the caller.
         var exe = path.join(binDir, 'gettile', '.');
         var command = spawn(exe, parameters);
         return callback(null, command);
      }
   };

   /**
    * <p>
    * Imports the given JSON data for the given user and associates it with the given device. Upon success, the callback
    * is called with a null first argument and the second argument will be an object of the form:
    * </p>
    * <code>
    * &nbsp;&nbsp;&nbsp;{<br>
    * &nbsp;&nbsp;&nbsp;"successful_records" : &lt;NUMBER&gt;,<br>
    * &nbsp;&nbsp;&nbsp;"failed_records" : &lt;NUMBER&gt;<br>
    * &nbsp;&nbsp;&nbsp;}
    * </code>
    * <p>
    * Upon failure, the callback function is called with a <code>DatastoreError</code> for the first argument.
    * The callback is called with a <code>DatastoreError</code> if:
    * <ul>
    *    <li>the user ID is not an integer</li>
    *    <li>the device name is invalid</li>
    *    <li>the data is null or undefined</li>
    *    <li>data fails to import</li>
    * </ul>
    * The DatastoreError given to the callback will contain a JSend compliant object in the <code>data</code> property
    * with more details about the error.
    * </p>
    *
    * @param {int} userId - the user ID
    * @param {string} deviceName - the device name
    * @param {object} data - the JSON data to import
    * @param {function} callback - callback function with the signature <code>callback(err, importResults)</code>
    */
   this.importJson = function(userId, deviceName, data, callback) {

      if (typeof callback === 'function') {
         if (!isUserIdValid(userId)) {
            var msg = "User ID must be an integer";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { userId : msg })));
         }
         if (!BodyTrackDatastore.isValidKey(deviceName)) {
            var msg = "Invalid device name";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { deviceName : msg })));
         }
         if (!util.isDefined(data)) {
            var msg = "Data cannot be null or undefined";
            return callback(new DatastoreError(createJSendClientValidationError(msg, { data : msg })));
         }

         temp.open('node_bodytrack_datastore_json_data_to_import',
                   function(err, info) {
                      if (err) {
                         return callback(new DatastoreError(createJSendServerError('Failed to open file', err)));
                      }

                      fs.writeFile(info.path,
                                   JSON.stringify(data),
                                   function(err) {
                                      if (err) {
                                         return callback(new DatastoreError(createJSendServerError('Failed to write file', err)));
                                      }

                                      fs.close(info.fd,
                                               function(err) {
                                                  if (err) {
                                                     return callback(new DatastoreError(createJSendServerError('Failed to close file', err)));
                                                  }

                                                  var parameters = [userId,
                                                                    deviceName,
                                                                    "--format",
                                                                    "json",
                                                                    info.path];

                                                  executeCommand("import",
                                                                 parameters,
                                                                 function(err, stdout) {

                                                                    if (err) {
                                                                       return callback(new DatastoreError(createJSendServerError('Failed to execute datastore import command', err)));
                                                                    }

                                                                    var datastoreResponse = null;

                                                                    try {
                                                                       datastoreResponse = JSON.parse(stdout);
                                                                    }
                                                                    catch (e) {
                                                                       datastoreResponse = null;
                                                                    }

                                                                    var wasSuccessful = datastoreResponse != null &&
                                                                                        typeof datastoreResponse['failed_records'] !== 'undefined' &&
                                                                                        datastoreResponse['failed_records'] == 0 &&
                                                                                        typeof datastoreResponse['successful_records'] !== 'undefined' &&
                                                                                        datastoreResponse['successful_records'] > 0;

                                                                    if (wasSuccessful) {
                                                                       return callback(null, datastoreResponse);
                                                                    }

                                                                    return callback(new DatastoreError(createJSendServerError('Failed to parse datastore import response as JSON', datastoreResponse)));

                                                                 });
                                               });
                                   });
                   });
      }
   };
}

// PUBLIC STATIC METHODS

/**
 * Returns <code>true</code> if the given key is a valid key for the datastore.  A valid key is:
 * <ul>
 *    <li>a string</li>
 *    <li>non empty</li>
 *    <li>doesn't start or end with a dot</li>
 *    <li>doesn't contain two or more consecutive dots</li>
 *    <li>consists of only the following characters: a-z, A-Z, 0-9, underscore (_), dot (.), and dash (-)</li>
 * </ul>
 * @param {string} key - the datastore key to be tested
 * @returns {boolean}
 */
BodyTrackDatastore.isValidKey = function(key) {
   return util.isString(key) &&                                // is defined, non-null, and a string
          key.length > 0 &&                                    // is non empty
          key.charAt(0) != '.' &&                              // doesn't start with a dot
          key.slice(-1) != '.' &&                              // doesn't end with a dot
          key.indexOf('..') < 0 &&                             // doesn't have two consecutive dots
          key.match(VALID_KEY_CHARACTERS_PATTERN) != null;     // contains only legal characters
};

module.exports = BodyTrackDatastore;
