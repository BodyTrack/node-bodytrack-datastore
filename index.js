const execFile = require('child_process').execFile;
const spawn = require('child_process').spawn;
const fs = require('fs');
const path = require('path');
const TypeUtils = require('data-type-utils');
const TempFile = require('./lib/TempFile');
const DatastoreError = require('./lib/DatastoreError');
const JSendClientValidationError = require('jsend-utils').JSendClientValidationError;
const JSendServerError = require('jsend-utils').JSendServerError;
const rimraf = require('rimraf');
const log = require('@log4js-node/log4js-api').getLogger("bodytrack-datastore");

const DATASTORE_EXECUTABLES = ['export', 'gettile', 'import', 'info'];
const VALID_KEY_CHARACTERS_PATTERN = /^[a-zA-Z0-9_.\-]+$/;

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

   if (!TypeUtils.isDefinedAndNotNull(config)) {
      throw new Error("config cannot be undefined or null");
   }
   else if (!TypeUtils.isDefinedAndNotNull(config.binDir)) {
      throw new Error("config.binDir cannot be undefined or null");
   }
   else if (!TypeUtils.isDefinedAndNotNull(config.dataDir)) {
      throw new Error("config.dataDir cannot be undefined or null");
   }
   // PRIVATE METHODS

   // joining with the . ensures there's no trailing slash
   const binDir = path.join(TypeUtils.isDefinedAndNotNull(config) ? config.binDir : '', '.');
   const dataDir = path.join(TypeUtils.isDefinedAndNotNull(config) ? config.dataDir : '', '.');

   const convertNullToString = function(val) {
      return val === null ? 'null' : val;
   };

   const executeCommand = function(commandName, parameters = [], callback) {
      const executable = path.join(binDir, commandName, '.');

      parameters.unshift(dataDir);
      const cleanParams = parameters.map(convertNullToString);

      execFile(executable, cleanParams, callback);
   };

   const isUserIdValid = function(userId) {
      return TypeUtils.isPositiveInt(userId);
   };

   const validateUserIdDeviceChannelObjects = function(userIdDeviceChannelObjects, callback) {
      const userIdDeviceChannelsMap = {};
      const userIdDeviceChannels = [];

      if (Array.isArray(userIdDeviceChannelObjects) && userIdDeviceChannelObjects.length > 0) {
         // check each item in the array to make sure they're all strings.
         for (let i = 0; i < userIdDeviceChannelObjects.length; i++) {
            const o = userIdDeviceChannelObjects[i];
            if (TypeUtils.isDefinedAndNotNull(o.userId) &&
                TypeUtils.isDefinedAndNotNull(o.deviceName) &&
                TypeUtils.isDefinedAndNotNull(o.channelNames)) {

               if (!isUserIdValid(o.userId)) {
                  const msg = "User ID [" + o.userId + "] must be an integer";
                  return callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
               }

               if (!BodyTrackDatastore.isValidKey(o.deviceName)) {
                  const msg = "Invalid device name [" + o.deviceName + "]";
                  return callback(new DatastoreError(new JSendClientValidationError(msg, { deviceName : msg })));
               }

               if (Array.isArray(o.channelNames) && o.channelNames.length > 0) {
                  for (let j = 0; j < o.channelNames.length; j++) {
                     const channelName = o.channelNames[j];
                     const userIdDeviceChannel = parseInt(o.userId) + "." + o.deviceName + "." + channelName;

                     if (!BodyTrackDatastore.isValidKey(channelName)) {
                        const msg = "Invalid channel name [" + channelName + "] in [" + userIdDeviceChannel + "]";
                        return callback(new DatastoreError(new JSendClientValidationError(msg, { channelNames : msg })));
                     }

                     // if no error by now, then it's valid, so hang on to it, but only if we haven't seen this one before
                     if (!(userIdDeviceChannel in userIdDeviceChannelsMap)) {
                        userIdDeviceChannelsMap[userIdDeviceChannel] = true;
                        userIdDeviceChannels.push(userIdDeviceChannel);
                     }
                  }
               }
               else {
                  const msg = "channelNames must be a non-empty array of strings";
                  return callback(new DatastoreError(new JSendClientValidationError(msg, { channelNames : msg })));
               }
            }
            else {
               const msg = "userIdDeviceChannelObjects array must contain only objects";
               return callback(new DatastoreError(new JSendClientValidationError(msg, { userIdDeviceChannelObjects : msg })));
            }
         }
      }
      else {
         const msg = "userIdDeviceChannelObjects must be a non-empty array of objects";
         return callback(new DatastoreError(new JSendClientValidationError(msg, { userIdDeviceChannelObjects : msg })));
      }

      return callback(null, userIdDeviceChannels);
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
      for (let i = 0; i < DATASTORE_EXECUTABLES.length; i++) {
         const exePath = path.join(binDir, DATASTORE_EXECUTABLES[i]);
         if (!fs.existsSync(exePath)) {
            log.error("BodyTrackDatastore.isConfigValid(): executable (" + exePath + ") does not exist");
            return false;
         }
         const exeStat = fs.statSync(exePath);
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
    * or for only the specified device and channel.  Min and/or max time may be optionally specified as well. Will
    * attempt to compute the most recent data sample if <code>willFindMostRecentSample</code> is <code>true</code>
    * and the time range is not filtered with <code>minTime</code> and/or <code>maxTime</code>. Produced channel specs
    * are returned to the caller by calling the <code>callback</code> function.
    * </p>
    * <p>
    * The given <code>options</code> object must at least contain a <code>userId</code> field. The optional fields are:
    * <ul>
    *    <li><code>deviceName</code></li>
    *    <li><code>channelName</code></li>
    *    <li><code>minTime</code></li>
    *    <li><code>maxTime</code></li>
    *    <li><code>willFindMostRecentSample</code></li>
    * </ul>
    * Notes:
    * <ul>
    *    <li>The <code>channelName</code> will only be considered if the <code>deviceName</code> is specified.</li>
    *    <li>The <code>willFindMostRecentSample</code> option defaults to <code>false</code></li>
    *    <li>
    *       The <code>willFindMostRecentSample</code> will be ignored (and treated as <code>false</code>) if
    *       the time is filtered with <code>minTime</code> and/or <code>maxTime</code>.
    *    </li>
    * </ul>
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
    * @param {object} options Object containing the various option parameters
    * @param {function} callback - callback function with the signature <code>callback(err, info)</code>
    * @throws an <code>Error</code> if the method is called with fewer than 2 arguments
    */
   this.getInfo = function(options, callback) {
      // make sure they at least specified the userId
      if (options.hasOwnProperty('userId')) {

         // make sure the userId is valid
         if (!isUserIdValid(options['userId'])) {
            const msg = " User ID must be a positive integer";
            return callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
         }

         const parameters = ["-r", parseInt(options['userId'])];

         // see whether the caller specified the flag to find the most recent data sample
         if (options.hasOwnProperty('willFindMostRecentSample')) {
            if (!!options['willFindMostRecentSample']) {
               parameters.push("--find-most-recent");
            }
         }

         // see whether deviceName is specified
         if (options.hasOwnProperty('deviceName') && options['deviceName'] !== null) {
            const deviceName = options['deviceName'];

            // make sure the deviceName is valid
            if (!BodyTrackDatastore.isValidKey(deviceName)) {
               const msg = "Invalid device name";
               return callback(new DatastoreError(new JSendClientValidationError(msg, { deviceName : msg })));
            }

            // see whether channelName is specified
            if (options.hasOwnProperty('channelName') && options['channelName'] !== null) {
               const channelName = options['channelName'];

               // make sure the channelName is valid
               if (!BodyTrackDatastore.isValidKey(channelName)) {
                  const msg = "Invalid channel name";
                  return callback(new DatastoreError(new JSendClientValidationError(msg, { channelName : msg })));
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
         if (options.hasOwnProperty('minTime') && options['minTime'] !== null) {

            // make sure the minTime is valid
            if (!TypeUtils.isNumeric(options['minTime'])) {
               const msg = "Invalid min time";
               return callback(new DatastoreError(new JSendClientValidationError(msg, { minTime : msg })));
            }
            parameters.push("--min-time");
            parameters.push(parseFloat(options['minTime']));
         }

         // see whether the caller specified the max time
         if (options.hasOwnProperty('maxTime') && options['maxTime'] !== null) {

            // make sure the maxTime is valid
            if (!TypeUtils.isNumeric(options['maxTime'])) {
               const msg = "Invalid max time";
               return callback(new DatastoreError(new JSendClientValidationError(msg, { maxTime : msg })));
            }
            parameters.push("--max-time");
            parameters.push(parseFloat(options['maxTime']));
         }

         // FINALLY, execute the command!
         executeCommand("info", parameters,
                        function(err, stdout) {
                           if (err) {
                              return callback(new DatastoreError(new JSendServerError('Failed to call info', err)));
                           }
                           return callback(null, JSON.parse(stdout));
                        });
      }
      else {
         const msg = "User ID is required";
         return callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
      }
   };

   /**
    * <p>
    * Exports data from the specified device+channel(s) as CSV, optionally filtered by min and max time. Data is
    * returned to the caller as an EventEmitter given to the <code>callback</code> function. The devices and channels to
    * export are defined by the <code>userIdDeviceChannelObjects</code> argument. The
    * <code>userIdDeviceChannelObjects</code> argument must be a non-empty array of objects, where each object is
    * of the form: <code>{userId: USER_ID, deviceName: "DEVICE_NAME", channelNames: ["CHANNEL_NAME_1",...]}</code>. The
    * <code>userId</code> field must be an integer, the <code>deviceName</code> a string, and the
    * <code>channelName</code> must be an array of strings.
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
    *    <li>the userIdDeviceChannelObjects is not an non-empty array of objects</li>
    *    <li>any object in the userIdDeviceChannelObjects array contains invalid fields</li>
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
    *    <li>Duplicate requests for the same userId+deviceName+channelName are ignored and not included in the output.</li>
    *    <li>
    *       Although invalid user ids, device names, and channel names will cause an error to be returned, valid but
    *       non-existent user ids, device names, and channel names will not cause an error, but will obviously not have
    *       any data.
    *    </li>
    * </ul>
    * </p>
    *
    * @param {Array} userIdDeviceChannelObjects - a non-empty array of objects. See above for details.
    * @param {object} desiredFilter Object containing the various filter parameters
    * @param {function} callback - callback function with the signature <code>callback(err, eventEmitter)</code>
    */
   this.exportData = function(userIdDeviceChannelObjects, desiredFilter = {}, callback) {
      if (typeof callback === 'function') {

         validateUserIdDeviceChannelObjects(userIdDeviceChannelObjects,
                                            function(err, userIdDeviceChannels) {
                                               if (err) {
                                                  return callback(err);
                                               }

                                               // make sure the filter is non-null
                                               const filter = desiredFilter === null ? {} : desiredFilter;

                                               // default to CSV format if unspecified
                                               let format = TypeUtils.isDefinedAndNotNull(filter) && TypeUtils.isDefinedAndNotNull(filter.format) ? filter.format : 'csv';

                                               // make sure the format is valid
                                               if (TypeUtils.isString(format)) {
                                                  format = format.toLowerCase().trim();
                                                  if (format != 'csv' && format != 'json') {
                                                     return callback(new DatastoreError(new JSendClientValidationError("Invalid format", { format : format })));
                                                  }
                                               }
                                               else {
                                                  return callback(new DatastoreError(new JSendClientValidationError("Invalid format", { format : format })));
                                               }

                                               // specify the format (CSV or JSON)
                                               const parameters = ['--' + format];

                                               // see whether the caller specified the min time
                                               if (filter.hasOwnProperty('minTime') && filter['minTime'] !== null) {

                                                  // make sure the minTime is valid
                                                  if (!TypeUtils.isNumeric(filter['minTime'])) {
                                                     const msg = "Invalid min time";
                                                     return callback(new DatastoreError(new JSendClientValidationError(msg, { minTime : msg })));
                                                  }
                                                  parameters.push("--start");
                                                  parameters.push(parseFloat(filter['minTime']));
                                               }

                                               // see whether the caller specified the max time
                                               if (filter.hasOwnProperty('maxTime') && filter['maxTime'] !== null) {

                                                  // make sure the maxTime is valid
                                                  if (!TypeUtils.isNumeric(filter['maxTime'])) {
                                                     const msg = "Invalid max time";
                                                     return callback(new DatastoreError(new JSendClientValidationError(msg, { maxTime : msg })));
                                                  }
                                                  parameters.push("--end");
                                                  parameters.push(parseFloat(filter['maxTime']));
                                               }

                                               parameters.push(dataDir);
                                               userIdDeviceChannels.forEach(function(item) {
                                                  parameters.push(item);
                                               });

                                               // FINALLY, spawn the command, and return to the caller.
                                               const exportExe = path.join(binDir, 'export', '.');
                                               const command = spawn(exportExe, parameters);
                                               return callback(null, command);
                                            });
      }
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
    * @param {int|string} userId - the user ID
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
            const msg = " User ID must be a positive integer";
            callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
         }
         else if (!TypeUtils.isInt(level)) {
            const msg = "Level must be an integer";
            callback(new DatastoreError(new JSendClientValidationError(msg, { level : msg })));
         }
         else if (!TypeUtils.isInt(offset)) {
            const msg = "Offset must be an integer";
            callback(new DatastoreError(new JSendClientValidationError(msg, { offset : msg })));
         }
         else if (!BodyTrackDatastore.isValidKey(deviceName)) {
            const msg = "Invalid device name";
            callback(new DatastoreError(new JSendClientValidationError(msg, { deviceName : msg })));
         }
         else if (!BodyTrackDatastore.isValidKey(channelName)) {
            const msg = "Invalid channel name";
            callback(new DatastoreError(new JSendClientValidationError(msg, { channelName : msg })));
         }
         else {
            const parameters = [parseInt(userId),
                                deviceName + "." + channelName,
                                level,
                                offset];

            executeCommand("gettile", parameters,
                           function(err, stdout) {
                              if (err) {
                                 callback(new DatastoreError(new JSendServerError('Failed to call get tile', err)));
                              }
                              else {
                                 callback(null, JSON.parse(stdout));
                              }
                           });
         }
      }
      else {
         log.error("Callback is not a function!");
      }
   };

   /**
    * <p>
    * Returns the tiles for the given <code>userIdDeviceChannelObjects</code>, <code>level</code>, and
    * <code>offset</code>.  The tiles are returned to the caller as an EventEmitter given to the <code>callback</code>
    * function. The <code>userIdDeviceChannelObjects</code> argument must be a non-empty array of objects, where each
    * object is of the form: <code>{userId: USER_ID, deviceName: "DEVICE_NAME", channelName: ["CHANNEL_NAME_1",...]}</code>.
    * The <code>userId</code> field must be an integer, the <code>deviceName</code> a string, and the
    * <code>channelName</code> must be an array of strings.
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
    * <p>
    * Other things to note:
    * <ul>
    *    <li>Duplicate requests for the same userId+deviceName+channelName are ignored and not included in the output.</li>
    *    <li>
    *       Although invalid user ids, device names, and channel names will cause an error to be returned, valid but
    *       non-existent user ids, device names, and channel names will not cause an error, but the data will be null.
    *    </li>
    * </ul>
    * </p>
    *
    * @param {Array} userIdDeviceChannelObjects - a non-empty array of objects. See above for details.
    * @param {int} level - the tile level
    * @param {int} offset - the tile offset
    * @param {function} callback - callback function with the signature <code>callback(err, eventEmitter)</code>
    */
   this.getTiles = function(userIdDeviceChannelObjects, level, offset, callback) {
      if (typeof callback === 'function') {
         // validate userId+deviceName+channelName objects
         validateUserIdDeviceChannelObjects(userIdDeviceChannelObjects,
                                            function(err, userIdDeviceChannels) {
                                               if (err) {
                                                  return callback(err);
                                               }

                                               // validate level and offset
                                               if (!TypeUtils.isInt(level)) {
                                                  const msg = "Level must be an integer";
                                                  return callback(new DatastoreError(new JSendClientValidationError(msg, { level : msg })));
                                               }
                                               if (!TypeUtils.isInt(offset)) {
                                                  const msg = "Offset must be an integer";
                                                  return callback(new DatastoreError(new JSendClientValidationError(msg, { offset : msg })));
                                               }

                                               const parameters = [dataDir,
                                                                   '--multi',
                                                                   userIdDeviceChannels.join(','),
                                                                   level,
                                                                   offset];

                                               // FINALLY, spawn the command, and return to the caller.
                                               const exe = path.join(binDir, 'gettile', '.');
                                               const command = spawn(exe, parameters);
                                               return callback(null, command);
                                            });
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
    * @param {int|string} userId - the user ID
    * @param {string} deviceName - the device name
    * @param {object} data - the JSON data to import
    * @param {function} callback - callback function with the signature <code>callback(err, importResults)</code>
    */
   this.importJson = function(userId, deviceName, data, callback) {

      if (typeof callback === 'function') {
         if (!isUserIdValid(userId)) {
            const msg = " User ID must be a positive integer";
            callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
         }
         else if (!BodyTrackDatastore.isValidKey(deviceName)) {
            const msg = "Invalid device name";
            callback(new DatastoreError(new JSendClientValidationError(msg, { deviceName : msg })));
         }
         else if (!TypeUtils.isDefinedAndNotNull(data)) {
            const msg = "Data cannot be null or undefined";
            callback(new DatastoreError(new JSendClientValidationError(msg, { data : msg })));
         }
         else {

            // create a temp file to write the uploaded data so the datastore can import it
            TempFile.create({ prefix : 'node_bodytrack_datastore_json_data_to_import_', suffix : '.json' },
                            function(createTempFileErr, tempFile) {
                               if (createTempFileErr) {
                                  callback(new DatastoreError(new JSendServerError('Upload failed due to an error trying to open the temp file')));
                               }
                               else {
                                  fs.writeFile(tempFile.path,
                                               JSON.stringify(data),
                                               function(writeToTempFileErr) {

                                                  // try closing the file, regardless of whether the write actually succeeded.  We'll
                                                  // verify write success later
                                                  fs.close(tempFile.fd, function(closeTempFileErr) {
                                                     if (closeTempFileErr) {
                                                        log.error("Error trying to close the temp file [" + tempFile.path + "]: " + closeTempFileErr);

                                                        // we couldn't close the file, so it's doubtful that cleaning up will work, but
                                                        // give it a try anyway...
                                                        try {
                                                           tempFile.cleanup();
                                                        }
                                                        catch (e) {
                                                           log.error("Error trying to cleanup the temp file after failing to close it [" + tempFile.path + "]: " + e);
                                                        }

                                                        callback(new DatastoreError(new JSendServerError('Upload failed due to an error trying to close the temp file')));
                                                     }
                                                     else {

                                                        // The file should be closed now, so see whether we actually successfully wrote to
                                                        // the file.  If not, remove it, and abort.
                                                        if (writeToTempFileErr) {
                                                           log.error("Error trying to write to the temp file [" + tempFile.path + "]: " + writeToTempFileErr);

                                                           try {
                                                              tempFile.cleanup();
                                                           }
                                                           catch (e) {
                                                              log.error("Error trying to cleanup the temp file after failing to write to it [" + tempFile.path + "]: " + e);
                                                           }

                                                           callback(new DatastoreError(new JSendServerError('Failed to write to temp file', err)));
                                                        }
                                                        else {

                                                           // we successfully wrote to the file, and successfully closed it, so we're good
                                                           // to go and ready to pass off to the datastore.  Start by creating the params
                                                           // for the import command
                                                           const parameters = [parseInt(userId),
                                                                               deviceName,
                                                                               "--format",
                                                                               "json",
                                                                               tempFile.path];

                                                           executeCommand("import",
                                                                          parameters,
                                                                          function(importError, stdout) {

                                                                             // we're done with the temp file now, so clean it up before doing anything else
                                                                             try {
                                                                                tempFile.cleanup();
                                                                             }
                                                                             catch (e) {
                                                                                log.error("Error trying to cleanup the temp file [" + tempFile.path + "]: " + e);
                                                                             }

                                                                             if (importError) {
                                                                                return callback(new DatastoreError(new JSendServerError('Failed to execute datastore import command', importError)));
                                                                             }

                                                                             let datastoreResponse = null;
                                                                             try {
                                                                                datastoreResponse = JSON.parse(stdout);
                                                                             }
                                                                             catch (e) {
                                                                                log.error("Error parsing datastore response: ", e);
                                                                                datastoreResponse = null;
                                                                             }

                                                                             const wasSuccessful = datastoreResponse !== null &&
                                                                                                   typeof datastoreResponse['failed_records'] !== 'undefined' &&
                                                                                                   parseInt(datastoreResponse['failed_records']) === 0 &&
                                                                                                   typeof datastoreResponse['successful_records'] !== 'undefined' &&
                                                                                                   datastoreResponse['successful_records'] > 0;

                                                                             if (wasSuccessful) {
                                                                                return callback(null, datastoreResponse);
                                                                             }

                                                                             return callback(new DatastoreError(new JSendServerError('Failed to parse datastore import response as JSON', datastoreResponse)));

                                                                          });

                                                        }
                                                     }
                                                  });
                                               });
                               }
                            });
         }
      }
   };

   /**
    * <p>
    *    Deletes the device specified by the given <code>deviceName</code> belonging to the user specified by the given
    *    <code>userId</code>.
    * </p>
    * <p>
    *    Note that there is only at most one argument ever given to the <code>callback</code>.  That argument will be
    *    an <code>Error</code> instance if an error occurred and <code>null</code> otherwise.  A <code>null</code>
    *    error does not necessarily mean that the device was deleted, however, because the <code>userId</code> and/or
    *    <code>deviceName</code> may have been syntactically valid, but not denote any actual entity.
    * </p>
    *
    * @param {int|string} userId - the user ID
    * @param {string} deviceName - the device name
    * @param {function} callback - callback function with the signature <code>callback(err)</code>
    */
   this.deleteDevice = function(userId, deviceName, callback) {
      if (typeof callback === 'function') {
         if (!isUserIdValid(userId)) {
            const msg = " User ID must be a positive integer";
            callback(new DatastoreError(new JSendClientValidationError(msg, { userId : msg })));
         }
         else if (!BodyTrackDatastore.isValidKey(deviceName)) {
            const msg = "Invalid device name";
            callback(new DatastoreError(new JSendClientValidationError(msg, { deviceName : msg })));
         }
         else {
            const deviceDirectoryPath = path.join(dataDir, userId.toString(), deviceName);
            log.debug("Attempting to delete device at path [" + deviceDirectoryPath + "]");

            rimraf(deviceDirectoryPath, { glob : false }, function(err) {
               if (err) {
                  log.error("Failed to delete device directory [" + deviceDirectoryPath + "]");
                  callback(err);
               }
               else {
                  callback(null);
               }
            });
         }
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
   return TypeUtils.isString(key) &&                           // is defined, non-null, and a string
          key.length > 0 &&                                    // is non empty
          key.charAt(0) != '.' &&                              // doesn't start with a dot
          key.slice(-1) != '.' &&                              // doesn't end with a dot
          key.indexOf('..') < 0 &&                             // doesn't have two consecutive dots
          key.match(VALID_KEY_CHARACTERS_PATTERN) !== null;    // contains only legal characters
};

module.exports = BodyTrackDatastore;
