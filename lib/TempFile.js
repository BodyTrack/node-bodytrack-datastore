const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const log = require('@log4js-node/log4js-api').getLogger("bodytrack-datastore:TempFile");

const TEMP_DIR = os.tmpdir();
const CREATE_FLAGS = fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_RDWR;
const FILE_MODE = 384; // 0600
const TOTAL_TRIES = 10;

/**
 * Random name generator based on crypto.
 * Based on https://github.com/raszi/node-tmp
 *
 * @param {Number} howMany
 * @return {String}
 * @api private
 */
function generateRandomCharacters(howMany) {
   let rnd = null;

   // make sure that we do not fail because we ran out of entropy
   // noinspection UnusedCatchParameterJS
   try {
      rnd = crypto.randomBytes(howMany);
   }
   catch (e) {
      rnd = crypto.pseudoRandomBytes(howMany);
   }

   return rnd.toString('hex');
}

const generateTempFilename = function(options = {}) {
   const hrTime = process.hrtime.bigint();

   // prefix and postfix
   let name = [
      options.prefix || 'tmp',
      process.pid,
      hrTime,
      generateRandomCharacters(10)
   ].join('_');

   name += options.suffix || '.tmp';

   return path.join(TEMP_DIR, name);
};

/**
 * A simple class for creating temporary files.  I stole a lot from node-tmp (https://github.com/raszi/node-tmp), but
 * kept only the bare minimum that I needed and changed some stuff I didn't like.
 *
 * @param fileDescriptor the file descriptor
 * @param filePath the absolute path to the file
 * @constructor
 */
function TempFile(fileDescriptor, filePath) {
   this.fd = fileDescriptor;
   this.path = filePath;
}

/**
 * Unlinks the file, possibly throwing an exception upon failure.  WARNING: assumes you have already closed the file!
 */
TempFile.prototype.cleanup = function() {
   fs.unlinkSync(this.path);
};

TempFile.create = function(options, callback) {
   let numTries = 0;
   let fd = null;
   let path = null;

   do {
      path = generateTempFilename(options || {});
      try {
         fd = fs.openSync(path, CREATE_FLAGS, FILE_MODE);
      }
      catch (e) {
         log.error("Failed to create temp file [" + path + "]: " + e);
         fd = null;
      }
      numTries++;
   }
   while (fd === null && numTries < TOTAL_TRIES);

   if (fd === null) {
      callback(new Error("Failed to create a temp file"));
   }
   else {
      callback(null, new TempFile(fd, path));
   }
};

module.exports = TempFile;