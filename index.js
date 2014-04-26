var exec = require('child_process').exec;
var temp = require('temp');
var fs = require('fs');
var util = require('./lib/util');

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

   var binDir = util.removeTrailingSlash(util.isDefined(config) ? config.binDir : '');
   var dataDir = util.removeTrailingSlash(util.isDefined(config) ? config.dataDir : '');

   var buildCommand = function(command, parameters) {
      // surround the command and data directory with single quotes to deal with paths containing spaces
      var launchCommand = "'" + binDir + "/" + command + "' '" + dataDir + "'";
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
      console.log("bodytrack-datastore: executing command: " + command);
      exec(command, callback);
   };

   var isUidValid = function(uid) {
      return util.isInt(uid);
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
         console.log("ERROR: bodytrack-datastore: the data directory (" + dataDir + ") does not exist");
         return false;
      }

      if (!fs.existsSync(binDir)) {
         console.log("ERROR: bodytrack-datastore: the bin directory (" + binDir + ") does not exist");
         return false;
      }

      if (!fs.statSync(dataDir).isDirectory()) {
         console.log("ERROR: bodytrack-datastore: the data directory (" + dataDir + ") is not a directory");
         return false;
      }

      if (!fs.statSync(binDir).isDirectory()) {
         console.log("ERROR: bodytrack-datastore: the bin directory (" + binDir + ") is not a directory");
         return false;
      }

      // finally, make sure the executables exist and are files
      for (var i = 0; i < DATASTORE_EXECUTABLES.length; i++) {
         var exePath = binDir + "/" + DATASTORE_EXECUTABLES[i];
         if (!fs.existsSync(exePath)) {
            console.log("ERROR: bodytrack-datastore: executable (" + exePath + ") does not exist");
            return false;
         }
         var exeStat = fs.statSync(exePath);
         if (!exeStat.isFile()) {
            console.log("ERROR: bodytrack-datastore: executable (" + exePath + ") is not a file");
            return false;
         }
      }

      return true;
   };

   /**
    * <p>
    * Produces the "channel_specs" JSON either for all devices and channels or for only the specified device and
    * channel. If called with two arguments, returns the channel specs for all devices and channels.  If called with
    * four arguments, it produces the channel specs for only the specified device and channel.  Produced channel specs
    * are returned to the caller by calling the <code>callback</code> function.
    * </p>
    * <p>
    * The callback is called with an <code>Error</code> if:
    * <ul>
    *    <li>the device name is invalid</li>
    *    <li>the channel name is invalid</li>
    *    <li>the user ID is not an integer</li>
    * </ul>
    * </p>
    *
    * @param {int} uid - the user ID
    * @param {string} [deviceName] - the device name
    * @param {string} [channeName] - the channel name
    * @param {function} callback - callback function with the signature <code>callback(err, info)</code>
    * @throws an <code>Error</code> if the method is called with 0, 1, or 3 arguments
    */
   this.getInfo = function(uid) {
      // don't bother doing anything if they didn't provide a callback function!
      if (arguments.length >= 2) {

         var callback = null;
         var parameters = null;

         // if the 2nd argument is a function, then treat it as the callback and get info for all channels
         if (typeof arguments[1] === 'function') {
            callback = arguments[1];
            parameters = ["-r", uid];
         }
         // if the 4th argument is a function, then use the 2nd and 3rd as the device name and
         // channel name, and the 4th as the callback
         else if (arguments.length >= 4 && typeof arguments[3] === 'function') {
            callback = arguments[3];
            var deviceName = arguments[1];
            var channelName = arguments[2];

            // validate the device and channel name
            if (!BodyTrackDatastore.isValidKey(deviceName)) {
               callback(new Error("Invalid device name"));
               return;
            }
            if (!BodyTrackDatastore.isValidKey(channelName)) {
               callback(new Error("Invalid channel name"));
               return;
            }

            var deviceAndChannelName = deviceName + "." + channelName;
            parameters = [uid, "--prefix", deviceAndChannelName];
         }
         else {
            throw new Error("Illegal arguments: expected either 2 or 4 arguments. Usage: getInfo(uid [,deviceName, channelName], callback)");
         }

         if (!isUidValid(uid)) {
            callback(new Error("User ID must be an integer"));
            return;
         }

         if (callback != null) {
            executeCommand("info", parameters,
                           function(err, stdout) {
                              if (err) {
                                 callback(err);
                              }
                              else {
                                 callback(null, JSON.parse(stdout));
                              }
                           });
         }
      }
      else {
         throw new Error("Illegal arguments: expected either 2 or 4 arguments. Usage: getInfo(uid [,deviceName, channelName], callback)");
      }
   };

   /**
    * <p>
    * Returns the tile for the given <code>user</code>, <code>device</code>, <code>channel</code>, <code>level</code>,
    * and <code>offset</code>.  The tile is returned to the caller by calling the <code>callback</code> function.
    * </p>
    * <p>
    * The callback is called with an <code>Error</code> if:
    * <ul>
    *    <li>the user ID is not an integer</li>
    *    <li>the device name is invalid</li>
    *    <li>the channel name is invalid</li>
    *    <li>the level is not an integer</li>
    *    <li>the offset is not an integer</li>
    * </ul>
    * </p>
    *
    * @param {int} uid - the user ID
    * @param {string} deviceName - the device name
    * @param {string} channelName - the channel name
    * @param {int} level - the tile level
    * @param {int} offset - the tile offset
    * @param {function} callback - callback function with the signature <code>callback(err, tile)</code>
    */
   this.getTile = function(uid, deviceName, channelName, level, offset, callback) {
      if (typeof callback === 'function') {
         // validate inputs
         if (!isUidValid(uid)) {
            callback(new Error("User ID must be an integer"));
            return;
         }
         if (!util.isInt(level)) {
            callback(new Error("Level must be an integer"));
            return;
         }
         if (!util.isInt(offset)) {
            callback(new Error("Offset must be an integer"));
            return;
         }
         if (!BodyTrackDatastore.isValidKey(deviceName)) {
            callback(new Error("Invalid device name"));
            return;
         }
         if (!BodyTrackDatastore.isValidKey(channelName)) {
            callback(new Error("Invalid channel name"));
            return;
         }

         var parameters = [uid,
                           deviceName + "." + channelName,
                           level,
                           offset];

         executeCommand("gettile", parameters,
                        function(err, stdout) {
                           if (err) {
                              callback(err);
                           }
                           else {
                              callback(null, JSON.parse(stdout));
                           }
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
    * Upon failure, the callback function is called with an <code>Error</code> for the first argument.
    * The callback is called with an <code>Error</code> if:
    * <ul>
    *    <li>the user ID is not an integer</li>
    *    <li>the device name is invalid</li>
    *    <li>the data is null or undefined</li>
    *    <li>data fails to import</li>
    * </ul>
    * </p>
    *
    * @param {int} uid - the user ID
    * @param {string} deviceName - the device name
    * @param {object} data - the JSON data to import
    * @param {function} callback - callback function with the signature <code>callback(err, importResults)</code>
    */
   this.importJson = function(uid, deviceName, data, callback) {

      if (typeof callback === 'function') {
         if (!isUidValid(uid)) {
            callback(new Error("User ID must be an integer"));
            return;
         }
         if (!BodyTrackDatastore.isValidKey(deviceName)) {
            callback(new Error("Invalid device name"));
            return;
         }
         if (!util.isDefined(data)) {
            callback(new Error("Data cannot be null or undefined"));
            return;
         }

         temp.open('node_bodytrack_datastore_json_data_to_import',
                   function(err, info) {
                      if (err) {
                         callback(new Error("failed to open file: " + err));
                      }
                      else {
                         fs.writeFile(info.path,
                                      JSON.stringify(data),
                                      function(err) {
                                         if (err) {
                                            callback(new Error("failed to write file: " + err));
                                         }
                                         else {
                                            fs.close(info.fd,
                                                     function(err) {
                                                        if (err) {
                                                           callback(new Error("failed to close file: " + err));
                                                        }
                                                        else {
                                                           var parameters = [uid,
                                                                             deviceName,
                                                                             "--format",
                                                                             "json",
                                                                             info.path];

                                                           executeCommand("import",
                                                                          parameters,
                                                                          function(err, stdout) {

                                                                             if (err) {
                                                                                callback(new Error("failed to execute datastore import command: " + err));
                                                                             }
                                                                             else {
                                                                                var datastoreResponse = null;

                                                                                try {
                                                                                   datastoreResponse = JSON.parse(stdout);
                                                                                }
                                                                                catch (e) {
                                                                                   datastoreResponse = null;
                                                                                }

                                                                                var wasSuccessful = datastoreResponse != null &&
                                                                                                    datastoreResponse.failed_records == 0 &&
                                                                                                    datastoreResponse.successful_records > 0;

                                                                                if (wasSuccessful) {
                                                                                   callback(null, {
                                                                                      "successful_records" : datastoreResponse.successful_records,
                                                                                      "failed_records" : datastoreResponse.failed_records
                                                                                   });
                                                                                }
                                                                                else {
                                                                                   callback(new Error("failed to parse datastore import response as JSON: " + datastoreResponse));
                                                                                }

                                                                             }
                                                                          });
                                                        }
                                                     });
                                         }
                                      });
                      }
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
