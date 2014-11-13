var config = require('./config');
var fs = require('fs');
var expect = require('chai').expect;
var BodyTrackDatastore = require('../index');
var DatastoreError = require('../lib/errors').DatastoreError;
var deleteDir = require('rimraf');

var log4js = require('log4js');
log4js.configure({
                    "replaceConsole" : false,
                    "appenders" : [
                       {
                          "type" : "console",
                          "layout" : {
                             "type" : "pattern",
                             "pattern" : "%d{ABSOLUTE} [%[%p%]] %c - %m"
                          }
                       }
                    ],
                    "levels" : {
                       "[all]" : "DEBUG"
                    }
                 });
var log = log4js.getLogger("bodytrack-datastore:test");

// data files
var emptyData = require('./data/empty_data.json');
var multipleEmptyData = require('./data/multiple_empty_data.json');
var validData1 = require('./data/valid1.json');
var validData2 = require('./data/valid2.json');
var validData3 = require('./data/valid3.json');
var validData4 = require('./data/valid4.json');
var validData5 = require('./data/valid5.json');
var multipleValidData = require('./data/multiple_valid.json');
var validInfoAllDevices = require('./data/valid_info_all_devices.json');
var validInfoSpeck1Device = require('./data/valid_info_speck1_device.json');
var validInfoSpeck2Device = require('./data/valid_info_speck2_device.json');
var validInfoSpeck1DeviceHumidityChannel = require('./data/valid_info_speck1_device_humidity_channel.json');
var validInfoSpeck2DeviceParticlesChannel = require('./data/valid_info_speck2_device_particles_channel.json');
var gettile_neg3_21630549 = require('./data/gettile_-3_21630549.json');
var gettile_neg9_1384355148 = require('./data/gettile_-9_1384355148.json');

const DATA_DIR = "./test/test_datastore";

before(function(initDone) {
   // delete the data directory, so we're sure we're always starting fresh
   deleteDir(DATA_DIR, function(err) {
      if (err) {
         return initDone(err);
      }

      // create the data directory
      fs.mkdir(DATA_DIR, function(err) {
         if (err) {
            return initDone(err);
         }

         initDone();
      });
   })
});

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

      var verifyValidationError = function(err, nameOfInvalidField, done) {
         expect(err).to.not.be.null;
         expect(err instanceof DatastoreError).to.be.true;
         expect(err.data).to.not.be.null;
         expect(err.data.code).to.equal(422);
         expect(err.data.status).to.equal('error');
         expect(err.data.data).to.not.be.null;
         expect(err.data.data[nameOfInvalidField]).to.not.be.null;
         done();
      };

      describe('BodyTrackDatastore', function() {
         it('should be configured with a valid installation of the BodyTrack Datastore', function() {
            expect(datastore.isConfigValid()).to.be.true;
         });

         describe('importJson()', function() {
            var verifyImportSuccess = function(err, response, deviceName, expectedRecordCount, expectedImportBounds, expectedDeviceBounds, done) {
               expect(err).to.be.null;
               expect(response).to.not.be.null;
               expect(response.min_time).to.equal(expectedImportBounds.min_time);
               expect(response.max_time).to.equal(expectedImportBounds.max_time);
               expect(response.successful_records).to.equal(expectedRecordCount);
               expect(response.failed_records).to.equal(0);

               // call getInfo to verify the min/max times for the device
               datastore.getInfo({
                                    userId : 1,
                                    deviceName : deviceName
                                 }, function(err2, info) {
                  expect(err2).to.be.null;
                  expect(info).to.not.be.null;
                  expect(info.min_time).to.equal(expectedDeviceBounds.min_time);
                  expect(info.max_time).to.equal(expectedDeviceBounds.max_time);

                  done();
               });
            };

            describe('Successes', function() {
               it('should import valid dataset 1 without error', function(done) {
                  datastore.importJson(1, "speck1", validData1, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {min_time : 1384355116, max_time : 1384355157},
                                         {min_time : 1384355116, max_time : 1384355157},
                                         done);
                  });
               });

               it('should import valid dataset 2 without error', function(done) {
                  datastore.importJson(1, "speck1", validData2, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {min_time : 1384355106, max_time : 1384355136},
                                         {min_time : 1384355106, max_time : 1384355157},
                                         done);
                  });
               });

               it('should import valid dataset 3 without error', function(done) {
                  datastore.importJson(1, "speck1", validData3, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {min_time : 1384355142, max_time : 1384355176},
                                         {min_time : 1384355106, max_time : 1384355176},
                                         done);
                  });
               });

               it('should import valid dataset 4 without error', function(done) {
                  datastore.importJson(1, "speck1", validData4, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {min_time : 1384355103, max_time : 1384355179},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });

               it('should import valid dataset 5 without error', function(done) {
                  datastore.importJson(1, "speck1", validData5, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {min_time : 1384355137, max_time : 1384355141},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });

               it('should import valid data separated into multiple chunks without error', function(done) {
                  datastore.importJson(1, "speck2", multipleValidData, function(err, response) {
                     verifyImportSuccess(err, response, "speck2", multipleValidData.length,
                                         {min_time : 1383774015, max_time : 1383774017},
                                         {min_time : 1383774015, max_time : 1383774017},
                                         done);
                  });
               });

               it('should import empty JSON object without error', function(done) {
                  datastore.importJson(1, "speck1", {}, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });
               it('should import an array of one empty object without error', function(done) {
                  datastore.importJson(1, "speck1", [
                     {}
                  ], function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });
               it('should import empty data array without error', function(done) {
                  datastore.importJson(1, "speck1", emptyData, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", 1,
                                         {},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });
               it('should import multiple objects with empty data arrays without error', function(done) {
                  datastore.importJson(1, "speck1", multipleEmptyData, function(err, response) {
                     verifyImportSuccess(err, response, "speck1", multipleEmptyData.length,
                                         {},
                                         {min_time : 1384355103, max_time : 1384355179},
                                         done);
                  });
               });
            });

            describe('Failures', function() {
               it('should fail to import undefined data', function(done) {
                  datastore.importJson(1, "speck1", undefined, function(err) {
                     verifyFailure(err, done);
                  });
               });
               it('should fail to import null data', function(done) {
                  datastore.importJson(1, "speck1", null, function(err) {
                     verifyFailure(err, done);
                  });
               });
               it('should fail to import invalid JSON', function(done) {
                  datastore.importJson(1, "speck1", "{", function(err) {
                     verifyFailure(err, done);
                  });
               });
               it('should fail to import invalid JSON', function(done) {
                  datastore.importJson(1, "speck1", '{"channel_names":["humidity", "particles", "raw_particles"]}', function(err) {
                     verifyFailure(err, done);
                  });
               });
               it('should fail for invalid user ID', function(done) {
                  datastore.importJson("bubba", "speck1", emptyData, function(err) {
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

            describe("Successes", function() {
               it('should return proper channel specs for known user ID for all devices', function(done) {
                  datastore.getInfo({userId : 1}, function(err, info) {
                     verifySuccess(err, info, validInfoAllDevices, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1"}, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1Device, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck2"}, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2Device, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device and humidity channel', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : "humidity"}, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1DeviceHumidityChannel, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device and particles channel', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck2", channelName : "particles"}, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceParticlesChannel, done);
                  });
               });
               it('should return empty channel specs for unknown user ID', function(done) {
                  datastore.getInfo({userId : 42}, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and unknown device', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "bogus"}, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and device but unknown channel', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : "bogus"}, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and unknown device and channel', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "unknown", channelName : "bogus"}, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });

               // TODO: check min and max time
            });

            describe("Failures", function() {
               var verifyGetInfoFailure = function(err, info, done) {
                  expect(err).to.not.be.null;
                  expect(err instanceof DatastoreError).to.be.true;
                  expect(info).to.be.undefined;

                  done();
               };

               it('should fail if the user ID is not specified', function(done) {
                  datastore.getInfo({}, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the user ID is not a number', function(done) {
                  datastore.getInfo({userId : "bubba"}, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the user ID is not an integer', function(done) {
                  datastore.getInfo({userId : 1.2}, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the user ID is null', function(done) {
                  datastore.getInfo({userId : null}, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the device name has a space in it', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "Foo Bar"}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an empty string', function(done) {
                  datastore.getInfo({userId : 1, deviceName : ''}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is a "."', function(done) {
                  datastore.getInfo({userId : 1, deviceName : '.'}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is ".."', function(done) {
                  datastore.getInfo({userId : 1, deviceName : '..'}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is "sp..eck"', function(done) {
                  datastore.getInfo({userId : 1, deviceName : 'sp..eck'}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is "sp$eck"', function(done) {
                  datastore.getInfo({userId : 1, deviceName : 'sp$eck'}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an array', function(done) {
                  datastore.getInfo({userId : 1, deviceName : []}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an object', function(done) {
                  datastore.getInfo({userId : 1, deviceName : {}}, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the channel name has a space in it', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : "Foo Bar"}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an empty string', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : ''}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is a "."', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : '.'}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is ".."', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : '..'}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is "sp..eck"', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : 'sp..eck'}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is "sp$eck"', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : 'sp$eck'}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an array', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : []}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an object', function(done) {
                  datastore.getInfo({userId : 1, deviceName : "speck1", channelName : {}}, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the min time is not a number', function(done) {
                  datastore.getInfo({userId : 1, minTime : "bogus"}, function(err, info) {
                     verifyValidationError(err, 'minTime', done);
                  });
               });
               it('should fail if the max time is not a number', function(done) {
                  datastore.getInfo({userId : 1, maxTime : "bogus"}, function(err, info) {
                     verifyValidationError(err, 'maxTime', done);
                  });
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

            var emptyTile = {};

            describe("Successes", function() {
               it('should return correct tile for valid tile request', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', -3, 21630549, function(err, tile) {
                     verifySuccess(err, tile, gettile_neg3_21630549, done);
                  });
               });
               it('should return correct tile for valid tile request', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', -9, 1384355148, function(err, tile) {
                     verifySuccess(err, tile, gettile_neg9_1384355148, done);
                  });
               });
               it('should return correct tile for valid tile request that contains no data', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, 2639, function(err, tile) {
                     verifySuccess(err, tile, emptyTile, done);
                  });
               });
               it('should return empty tile for a tile request from an unknown device', function(done) {
                  datastore.getTile(1, 'bubba', 'particles', 10, 2639, function(err, tile) {
                     verifySuccess(err, tile, emptyTile, done);
                  });
               });
               it('should return empty tile for a tile request from an unknown channel', function(done) {
                  datastore.getTile(1, 'speck1', 'bubba', 10, 2639, function(err, tile) {
                     verifySuccess(err, tile, emptyTile, done);
                  });
               });
            });

            describe("Failures", function() {
               it('should fail for invalid user id', function(done) {
                  datastore.getTile('bubba', 'speck1', 'particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail for invalid user id', function(done) {
                  datastore.getTile(1.2, 'speck1', 'particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail for invalid user id', function(done) {
                  datastore.getTile(null, 'speck1', 'particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail for invalid user id', function(done) {
                  datastore.getTile(undefined, 'speck1', 'particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail for invalid user id', function(done) {
                  datastore.getTile({}, 'speck1', 'particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail for invalid user id', function(done) {
                  datastore.getTile("2;", 'speck1', 'particles', 10, 2639, function(err) {
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
                  datastore.getTile(1, 'speck1', 'particles.', 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', '.particles', 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', '', 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', null, 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', undefined, 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', 'foo..bar', 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail for invalid channel name', function(done) {
                  datastore.getTile(1, 'speck1', "foo$bar", 10, 2639, function(err) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });

               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', null, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', undefined, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 1.2, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', {}, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', [], 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', "bubba", 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', "2;", 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });

               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, null, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, undefined, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, 1.2, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, {}, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, [], function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, "bubba", function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTile(1, 'speck1', 'particles', 10, "2;", function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
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
