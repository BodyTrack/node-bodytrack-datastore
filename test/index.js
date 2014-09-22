var config = require('./config');
var fs = require('fs');
var expect = require('chai').expect;
var BodyTrackDatastore = require('../index');
var DatastoreError = require('../lib/errors').DatastoreError;

// data files
var emptyData = require('./data/empty_data.json');
var multipleEmptyData = require('./data/multiple_empty_data.json');
var validData = require('./data/valid.json');
var multipleValidData = require('./data/multiple_valid.json');
var validInfo = require('./data/valid_info.json');
var validInfoSpeckParticles = require('./data/valid_info_speck_particles.json');
var tile_10_2639 = require('./data/gettile_10_2639.json');
var tile_2_675957 = require('./data/gettile_2_675957');

const DATA_DIR = "./test/test_datastore";

// create the DATA_DIR if necessary
if (!fs.existsSync(DATA_DIR)) {
   fs.mkdirSync(DATA_DIR);
}

describe('BodyTrackDatastore constructor', function() {
   it("should throw an Error for undefined config", function() {
      expect(function() {
         new BodyTrackDatastore()
      }).to.throw(Error);
   });
   it("should throw an Error for null config", function() {
      expect(function() {
         new BodyTrackDatastore(null)
      }).to.throw(Error);
   });
   it("should throw an Error for undefined binDir and dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({})
      }).to.throw(Error);
   });
   it("should throw an Error for undefined binDir", function() {
      expect(function() {
         new BodyTrackDatastore({dataDir : "foo"})
      }).to.throw(Error);
   });
   it("should throw an Error for undefined dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({binDir : "foo"})
      }).to.throw(Error);
   });
   it("should throw an Error for null binDir and dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({binDir : null, dataDir : null})
      }).to.throw(Error);
   });
   it("should throw an Error for null binDir", function() {
      expect(function() {
         new BodyTrackDatastore({binDir : null, dataDir : "foo"})
      }).to.throw(Error);
   });
   it("should throw an Error for null dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({binDir : "foo", dataDir : null})
      }).to.throw(Error);
   });
});

describe("The test datastore data directory", function() {
   it("should exist", function() {
      expect(fs.existsSync(DATA_DIR)).to.be.true;
   });
   it("should be a directory", function() {
      expect(fs.statSync(DATA_DIR).isDirectory()).to.be.true;

      // create the datastore instance we'll use for the remaining tests
      var datastore = new BodyTrackDatastore({
                                                binDir : config.binDir,
                                                dataDir : DATA_DIR
                                             });

      var verifyFailure = function(err, done) {
         expect(err).to.not.be.null;

         done();
      };

      describe('BodyTrackDatastore', function() {
         it('should be configured with a valid installation of the BodyTrack Datastore', function() {
            expect(datastore.isConfigValid()).to.be.true;
         });

         var verifySuccess = function(err, response, expectedRecordCount, done) {
            expect(err).to.be.null;
            expect(response).to.not.be.null;
            expect(response.successful_records).to.equal(expectedRecordCount);
            expect(response.failed_records).to.equal(0);

            done();
         };

         describe('importJson()', function() {
            // SUCCESSES...
            it('should import empty JSON object without error', function(done) {
               datastore.importJson(1, "speck", {}, function(err, response) {
                  verifySuccess(err, response, 1, done);
               });
            });
            it('should import an array of one empty object without error', function(done) {
               datastore.importJson(1, "speck", [
                  {}
               ], function(err, response) {
                  verifySuccess(err, response, 1, done);
               });
            });
            it('should import empty data array without error', function(done) {
               datastore.importJson(1, "speck", emptyData, function(err, response) {
                  verifySuccess(err, response, 1, done);
               });
            });
            it('should import multiple objects with empty data arrays without error', function(done) {
               datastore.importJson(1, "speck", multipleEmptyData, function(err, response) {
                  verifySuccess(err, response, multipleEmptyData.length, done);
               });
            });
            it('should import valid data without error', function(done) {
               datastore.importJson(1, "speck", validData, function(err, response) {
                  verifySuccess(err, response, 1, done);
               });
            });
            it('should import valid data separated into multiple chunks without error', function(done) {
               datastore.importJson(1, "speck", multipleValidData, function(err, response) {
                  verifySuccess(err, response, multipleValidData.length, done);
               });
            });

            // FAILURES...
            it('should fail to import undefined data', function(done) {
               datastore.importJson(1, "speck", undefined, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail to import null data', function(done) {
               datastore.importJson(1, "speck", null, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail to import invalid JSON', function(done) {
               datastore.importJson(1, "speck", "{", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail to import invalid JSON', function(done) {
               datastore.importJson(1, "speck", '{"channel_names":["humidity", "particles", "raw_particles"]}', function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.importJson("bubba", "speck", emptyData, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, "", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, null, {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, undefined, {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, 1.2, {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, 42, {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, [], {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, {}, {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, ".foo", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, "foo.", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, "foo..bar", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.importJson(1, "foo$bar", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
         });

         describe("getInfo()", function() {

            var verifySuccess = function(err, info, expectedInfo, done) {
               expect(err).to.be.null;
               expect(info).to.not.be.null;
               expect(info.channel_specs).to.not.be.null;
               expect(info).to.deep.equal(expectedInfo);
               done();
            };
            var emptyChannelSpecs = {"channel_specs" : {}};

            it('should throw an Error for an invalid number of arguments', function() {
               expect(function() {
                  datastore.getInfo()
               }).to.throw(Error);
            });
            it('should throw an Error for an invalid number of arguments', function() {
               expect(function() {
                  datastore.getInfo(1)
               }).to.throw(Error);
            });
            it('should throw an Error for an invalid number of arguments', function() {
               expect(function() {
                  datastore.getInfo(1, "device", "channel")
               }).to.throw(Error);
            });

            it('should return proper channel specs for known user ID', function(done) {
               datastore.getInfo(1, function(err, info) {
                  verifySuccess(err, info, validInfo, done);
               });
            });
            it('should return empty channel specs for unknown user IDs', function(done) {
               datastore.getInfo(42, function(err, info) {
                  verifySuccess(err, info, emptyChannelSpecs, done);
               });
            });
            it('should return proper channel specs for known user ID and valid channel', function(done) {
               datastore.getInfo(1, 'speck', 'particles', function(err, info) {
                  verifySuccess(err, info, validInfoSpeckParticles, done);
               });
            });
            it('should return empty channel specs for known user ID, known device, and unknown channel', function(done) {
               datastore.getInfo(1, 'speck', 'bar', function(err, info) {
                  verifySuccess(err, info, emptyChannelSpecs, done);
               });
            });
            it('should return empty channel specs for known user ID, unknown device, and known channel', function(done) {
               datastore.getInfo(1, 'foo', 'particles', function(err, info) {
                  verifySuccess(err, info, emptyChannelSpecs, done);
               });
            });
            it('should return empty channel specs for known user ID and unknown device and channel', function(done) {
               datastore.getInfo(1, 'foo', 'bar', function(err, info) {
                  verifySuccess(err, info, emptyChannelSpecs, done);
               });
            });

            it('should fail for invalid user ID', function(done) {
               datastore.getInfo('bsdf', function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo(1.2, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo("1.2", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo(undefined, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo(null, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo(";touch /tmp/foo.txt", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid user ID', function(done) {
               datastore.getInfo("", function(err) {
                  verifyFailure(err, done);
               });
            });

            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, undefined, "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, null, "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, ".", "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, "", "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, "..", "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, "sp..eck", "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, "sp$eck", "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, [], "particles", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getInfo(1, {}, "particles", function(err) {
                  verifyFailure(err, done);
               });
            });

            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", undefined, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", null, function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", ".", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", "", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", "..", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", "sp..eck", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", "sp$eck", function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", [], function(err) {
                  verifyFailure(err, done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getInfo(1, "speck", {}, function(err) {
                  verifyFailure(err, done);
               });
            });
         });

         describe("getTile()", function() {

            var verifySuccess = function(err, tile, expectedTile, done) {
               expect(err).to.be.null;
               expect(tile).to.not.be.null;
               expect(tile).to.deep.equal(expectedTile);
               done();
            };

            var verifyValidationError = function(err, nameOfInvalidField, done){
               expect(err).to.not.be.null;
               expect(err instanceof DatastoreError).to.be.true;
               expect(err.data).to.not.be.null;
               expect(err.data.code).to.equal(422);
               expect(err.data.status).to.equal('error');
               expect(err.data.data).to.not.be.null;
               expect(err.data.data[nameOfInvalidField]).to.not.be.null;
               done();
            };

            var emptyTile = {};

            it('should return correct tile for valid tile request', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, 2639, function(err, tile) {
                  verifySuccess(err, tile, tile_10_2639, done);
               });
            });
            it('should return correct tile for valid tile request', function(done) {
               datastore.getTile(1, 'speck', 'particles', 2, 675957, function(err, tile) {
                  verifySuccess(err, tile, tile_2_675957, done);
               });
            });
            it('should return correct tile for valid tile request that contains no data', function(done) {
               datastore.getTile(1, 'speck', 'particles', 2, 675970, function(err, tile) {
                  verifySuccess(err, tile, emptyTile, done);
               });
            });

            it('should return empty tile for invalid tile request', function(done) {
               datastore.getTile(1, 'bubba', 'particles', 10, 2639, function(err, tile) {
                  verifySuccess(err, tile, emptyTile, done);
               });
            });
            it('should return empty tile for invalid tile request', function(done) {
               datastore.getTile(1, 'speck', 'bubba', 10, 2639, function(err, tile) {
                  verifySuccess(err, tile, emptyTile, done);
               });
            });

            it('should fail for invalid user id', function(done) {
               datastore.getTile('bubba', 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });
            it('should fail for invalid user id', function(done) {
               datastore.getTile(1.2, 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });
            it('should fail for invalid user id', function(done) {
               datastore.getTile(null, 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });
            it('should fail for invalid user id', function(done) {
               datastore.getTile(undefined, 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });
            it('should fail for invalid user id', function(done) {
               datastore.getTile({}, 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });
            it('should fail for invalid user id', function(done) {
               datastore.getTile("2;", 'speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'userId', done);
               });
            });

            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, 'speck.', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, '.speck', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, '', 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, null, 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, undefined, 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, "foo..bar", 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });
            it('should fail for invalid device name', function(done) {
               datastore.getTile(1, "foo$bar", 'particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'deviceName', done);
               });
            });

            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', 'particles.', 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', '.particles', 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', '', 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', null, 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', undefined, 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', 'foo..bar', 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });
            it('should fail for invalid channel name', function(done) {
               datastore.getTile(1, 'speck', "foo$bar", 10, 2639, function(err) {
                  verifyValidationError(err, 'channelName', done);
               });
            });

            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', null, 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', undefined, 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', 1.2, 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', {}, 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', [], 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', "bubba", 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });
            it('should fail for invalid level', function(done) {
               datastore.getTile(1, 'speck', 'particles', "2;", 2639, function(err) {
                  verifyValidationError(err, 'level', done);
               });
            });

            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, null, function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, undefined, function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, 1.2, function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, {}, function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, [], function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, "bubba", function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
            it('should fail for invalid offset', function(done) {
               datastore.getTile(1, 'speck', 'particles', 10, "2;", function(err) {
                  verifyValidationError(err, 'offset', done);
               });
            });
         });

         describe("isValidKey()", function() {

            it('should return false for undefined', function() {
               expect(BodyTrackDatastore.isValidKey(undefined)).to.be.false;
            });
            it('should return false for null', function() {
               expect(BodyTrackDatastore.isValidKey(null)).to.be.false;
            });
            it('should return false for empty string', function() {
               expect(BodyTrackDatastore.isValidKey('')).to.be.false;
            });
            it('should return false for .', function() {
               expect(BodyTrackDatastore.isValidKey('.')).to.be.false;
            });
            it('should return false for ..', function() {
               expect(BodyTrackDatastore.isValidKey('..')).to.be.false;
            });
            it('should return false for .a.', function() {
               expect(BodyTrackDatastore.isValidKey('.a.')).to.be.false;
            });
            it('should return false for a.', function() {
               expect(BodyTrackDatastore.isValidKey('a.')).to.be.false;
            });
            it('should return false for .a', function() {
               expect(BodyTrackDatastore.isValidKey('.a')).to.be.false;
            });
            it('should return false for $', function() {
               expect(BodyTrackDatastore.isValidKey('$')).to.be.false;
            });
            it('should return false for foo$bar', function() {
               expect(BodyTrackDatastore.isValidKey('foo$bar')).to.be.false;
            });
            it('should return false for =', function() {
               expect(BodyTrackDatastore.isValidKey('=')).to.be.false;
            });
            it('should return false for a.b..c', function() {
               expect(BodyTrackDatastore.isValidKey('a.b..c')).to.be.false;
            });

            it('should return true for a', function() {
               expect(BodyTrackDatastore.isValidKey('a')).to.be.true;
            });
            it('should return true for a.b', function() {
               expect(BodyTrackDatastore.isValidKey('a.b')).to.be.true;
            });
            it('should return true for a.b.c', function() {
               expect(BodyTrackDatastore.isValidKey('a.b.c')).to.be.true;
            });
            it('should return true for a-b-c', function() {
               expect(BodyTrackDatastore.isValidKey('a-b-c')).to.be.true;
            });
            it('should return true for a._-b', function() {
               expect(BodyTrackDatastore.isValidKey('a._-b')).to.be.true;
            });
            it('should return true for ____', function() {
               expect(BodyTrackDatastore.isValidKey('____')).to.be.true;
            });
            it('should return true for a_b_c', function() {
               expect(BodyTrackDatastore.isValidKey('a_b_c')).to.be.true;
            });
            it('should return true for ----', function() {
               expect(BodyTrackDatastore.isValidKey('----')).to.be.true;
            });
         });
      });
   });
});
