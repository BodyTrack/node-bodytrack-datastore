var config = require('./config');
var fs = require('fs');
var path = require('path');
var expect = require('chai').expect;
var should = require('should');
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
var validData6 = require('./data/valid6.json');
var validData7 = require('./data/valid7.json');
var validData8 = require('./data/valid8.json');
var multipleValidData = require('./data/multiple_valid.json');
var validInfoAllDevices = require('./data/valid_info_all_devices.json');
var validInfoSpeck1Device = require('./data/valid_info_speck1_device.json');
var validInfoSpeck2Device = require('./data/valid_info_speck2_device.json');
var validInfoSpeck2DeviceWithFilteredTime = require('./data/valid_info_speck2_device_with_filtered_time.json');
var validInfoSpeck1DeviceHumidityChannel = require('./data/valid_info_speck1_device_humidity_channel.json');
var validInfoSpeck2DeviceParticlesChannel = require('./data/valid_info_speck2_device_particles_channel.json');
var validInfoAllDevicesWithMostRecent = require('./data/valid_info_all_devices_with_most_recent.json');
var validInfoSpeck1DeviceWithMostRecent = require('./data/valid_info_speck1_device_with_most_recent.json');
var validInfoSpeck2DeviceWithMostRecent = require('./data/valid_info_speck2_device_with_most_recent.json');
var validInfoSpeck1DeviceHumidityChannelWithMostRecent = require('./data/valid_info_speck1_device_humidity_channel_with_most_recent.json');
var validInfoSpeck2DeviceParticlesChannelWithMostRecent = require('./data/valid_info_speck2_device_particles_channel_with_most_recent.json');
var validInfoFakeDevice1 = require('./data/valid_info_fake_device1.json');
var validInfoFakeDeviceWithMostRecent1 = require('./data/valid_info_fake_device_with_most_recent1.json');
var validInfoFakeDevice2 = require('./data/valid_info_fake_device2.json');
var validInfoFakeDeviceWithMostRecent2 = require('./data/valid_info_fake_device_with_most_recent2.json');
var validInfoFakeDevice3 = require('./data/valid_info_fake_device3.json');
var validInfoFakeDeviceWithMostRecent3 = require('./data/valid_info_fake_device_with_most_recent3.json');
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
         new BodyTrackDatastore({ dataDir : "foo" })
      }).to.throw(Error);
   });
   it("should throw an Error for undefined dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({ binDir : "foo" })
      }).to.throw(Error);
   });
   it("should throw an Error for null binDir and dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({ binDir : null, dataDir : null })
      }).to.throw(Error);
   });
   it("should throw an Error for null binDir", function() {
      expect(function() {
         new BodyTrackDatastore({ binDir : null, dataDir : "foo" })
      }).to.throw(Error);
   });
   it("should throw an Error for null dataDir", function() {
      expect(function() {
         new BodyTrackDatastore({ binDir : "foo", dataDir : null })
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
         expect(typeof err.data.data[nameOfInvalidField] !== 'undefined').to.be.true;
         expect(err.data.data[nameOfInvalidField]).to.not.be.null;
         done();
      };

      describe('BodyTrackDatastore', function() {
         it('should be configured with a valid installation of the BodyTrack Datastore', function() {
            expect(datastore.isConfigValid()).to.be.true;
         });

         describe('importJson()', function() {
            var verifyImportSuccess = function(userId, err, response, deviceName, expectedRecordCount, expectedImportBounds, expectedDeviceBounds, done) {
               expect(err).to.be.null;
               expect(response).to.not.be.null;
               expect(response.min_time).to.equal(expectedImportBounds.min_time);
               expect(response.max_time).to.equal(expectedImportBounds.max_time);
               expect(response.successful_records).to.equal(expectedRecordCount);
               expect(response.failed_records).to.equal(0);

               // call getInfo to verify the min/max times for the device
               datastore.getInfo({
                                    userId : userId,
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
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         { min_time : 1384355116, max_time : 1384355157 },
                                         { min_time : 1384355116, max_time : 1384355157 },
                                         done);
                  });
               });

               it('should import valid dataset 2 without error', function(done) {
                  datastore.importJson(1, "speck1", validData2, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         { min_time : 1384355106, max_time : 1384355136 },
                                         { min_time : 1384355106, max_time : 1384355157 },
                                         done);
                  });
               });

               it('should import valid dataset 3 without error', function(done) {
                  datastore.importJson(1, "speck1", validData3, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         { min_time : 1384355142, max_time : 1384355176 },
                                         { min_time : 1384355106, max_time : 1384355176 },
                                         done);
                  });
               });

               it('should import valid dataset 4 without error', function(done) {
                  datastore.importJson(1, "speck1", validData4, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import valid dataset 5 without error', function(done) {
                  datastore.importJson(1, "speck1", validData5, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         { min_time : 1384355137, max_time : 1384355141 },
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import valid dataset 6 without error', function(done) {
                  datastore.importJson(4, "fake", validData6, function(err, response) {
                     verifyImportSuccess(4, err, response, "fake", 1,
                                         { min_time : 1441856362, max_time : 1441856363 },
                                         { min_time : 1441856362, max_time : 1441856363 },
                                         done);
                  });
               });

               it('should import valid data separated into multiple chunks without error', function(done) {
                  datastore.importJson(1, "speck2", multipleValidData, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck2", multipleValidData.length,
                                         { min_time : 1383774015, max_time : 1383774017 },
                                         { min_time : 1383774015, max_time : 1383774017 },
                                         done);
                  });
               });

               it('should import empty JSON object without error', function(done) {
                  datastore.importJson(1, "speck1", {}, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         {},
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import an array of one empty object without error', function(done) {
                  datastore.importJson(1, "speck1", [
                     {}
                  ], function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         {},
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import empty data array without error', function(done) {
                  datastore.importJson(1, "speck1", emptyData, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", 1,
                                         {},
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import multiple objects with empty data arrays without error', function(done) {
                  datastore.importJson(1, "speck1", multipleEmptyData, function(err, response) {
                     verifyImportSuccess(1, err, response, "speck1", multipleEmptyData.length,
                                         {},
                                         { min_time : 1384355103, max_time : 1384355179 },
                                         done);
                  });
               });

               it('should import data for a different user without error', function(done) {
                  datastore.importJson(2,
                                       "speck1",
                                       [{
                                          "channel_names" : ["humidity", "particles", "raw_particles"],
                                          "data" : [
                                             [1384357121, 24, 13, 1],
                                             [1384357122, 25, 14, 2],
                                             [1384357123, 26, 15, 3],
                                             [1384357124, 27, 16, 4],
                                             [1384357125, 28, 17, 5]
                                          ]
                                       }],
                                       function(err, response) {
                                          verifyImportSuccess(2, err, response, "speck1", 1,
                                                              { min_time : 1384357121, max_time : 1384357125 },
                                                              { min_time : 1384357121, max_time : 1384357125 },
                                                              done);
                                       });
               });

               it('should be able to delete samples without error', function(done) {
                  datastore.importJson(2, "speck1", [
                     {
                        "channel_names" : ["humidity", "particles", "raw_particles"],
                        "data" : [
                           // should delete all but sample at time 1384355123
                           [1384357121, false, false, false],
                           [1384357122, false, false, false],
                           //[1384357123, 26, 15, 3],
                           [1384357124, false, false, false],
                           [1384357125, false, false, false],
                        ]
                     }
                  ], function(err, response) {

                     // TODO: Known bugs: deletes from the datastore currently (2014-12-01) don't update the min/max
                     // time or min/max values.  Once that's fixed, we'll need to update this test.

                     verifyImportSuccess(                                                   2,                                                 err, response, "speck1",                        1,
                                         { min_time : 1384357121, max_time : 1384357125 },  // see "Known bugs" above
                                         { min_time : 1384357121, max_time : 1384357125 },  // see "Known bugs" above
                                         function() {

                                            expect(response.channel_specs.humidity.channel_bounds.min_value).to.equal(24);       // see "Known bugs" above
                                            expect(response.channel_specs.humidity.channel_bounds.max_value).to.equal(28);       // see "Known bugs" above

                                            expect(response.channel_specs.particles.channel_bounds.min_value).to.equal(13);      // see "Known bugs" above
                                            expect(response.channel_specs.particles.channel_bounds.max_value).to.equal(17);      // see "Known bugs" above

                                            expect(response.channel_specs.raw_particles.channel_bounds.min_value).to.equal(1);   // see "Known bugs" above
                                            expect(response.channel_specs.raw_particles.channel_bounds.max_value).to.equal(5);   // see "Known bugs" above
                                            done();
                                         });
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
            var emptyChannelSpecs = { "channel_specs" : {} };

            describe("Successes", function() {
               it('should return proper channel specs for known user ID for all devices', function(done) {
                  datastore.getInfo({ userId : 1 }, function(err, info) {
                     verifySuccess(err, info, validInfoAllDevices, done);
                  });
               });
               it('should return proper channel specs for known user ID for all devices (with most recent data samples)', function(done) {
                  datastore.getInfo({ userId : 1, willFindMostRecentSample : true }, function(err, info) {
                     verifySuccess(err, info, validInfoAllDevicesWithMostRecent, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1" }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1Device, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device (with most recent data samples)', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck1",
                                       willFindMostRecentSample : true
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1DeviceWithMostRecent, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck2" }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2Device, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device (with most recent data samples)', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck2",
                                       willFindMostRecentSample : true
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceWithMostRecent, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device with min and max time specified', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck2",
                                       minTime : 1383774015.5,
                                       maxTime : 1383774018
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceWithFilteredTime, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device with min and max time specified (with most recent data samples requested, but ignored)', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck2",
                                       minTime : 1383774015.5,
                                       maxTime : 1383774018,
                                       willFindMostRecentSample : true
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceWithFilteredTime, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device and humidity channel', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck1",
                                       channelName : "humidity"
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1DeviceHumidityChannel, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck1 device and humidity channel (with most recent data samples)', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck1",
                                       channelName : "humidity",
                                       willFindMostRecentSample : true
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck1DeviceHumidityChannelWithMostRecent, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device and particles channel', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck2",
                                       channelName : "particles"
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceParticlesChannel, done);
                  });
               });
               it('should return proper channel specs for known user ID for speck2 device and particles channel (with most recent data samples)', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck2",
                                       channelName : "particles",
                                       willFindMostRecentSample : true
                                    }, function(err, info) {
                     verifySuccess(err, info, validInfoSpeck2DeviceParticlesChannelWithMostRecent, done);
                  });
               });

               it('should return proper channel specs for known user ID for fake device', function(done) {
                  datastore.getInfo({ userId : 4 }, function(err, info) {
                     verifySuccess(err, info, validInfoFakeDevice1, done);
                  });
               });
               it('should return proper channel specs for known user ID for fake device (with most recent data samples)', function(done) {
                  datastore.getInfo({ userId : 4, willFindMostRecentSample : true }, function(err, info) {
                     verifySuccess(err, info, validInfoFakeDeviceWithMostRecent1, done);
                  });
               });
               it('should return proper channel specs for known user ID for fake device, after importing more data (with most recent data samples)', function(done) {
                  datastore.importJson(4, "fake", validData7, function(err, response) {
                     if (err) {
                        return done(err);
                     }
                     else {
                        datastore.getInfo({ userId : 4, willFindMostRecentSample : false }, function(err, info) {
                           if (err) {
                              return done(err);
                           }
                           else {
                              verifySuccess(err, info, validInfoFakeDevice2, function() {
                                 datastore.getInfo({
                                                      userId : 4,
                                                      willFindMostRecentSample : true
                                                   }, function(err, info) {
                                    verifySuccess(err, info, validInfoFakeDeviceWithMostRecent2, done);
                                 });
                              });
                           }
                        });
                     }
                  });
               });
               it('should return proper channel specs for known user ID for fake device for a channel which has both a value and a comment at the same timestamp (with most recent data samples)', function(done) {
                  datastore.importJson(4, "fake", validData8, function(err, response) {
                     if (err) {
                        return done(err);
                     }
                     else {
                        datastore.getInfo({ userId : 4, willFindMostRecentSample : false }, function(err, info) {
                           if (err) {
                              return done(err);
                           }
                           else {
                              verifySuccess(err, info, validInfoFakeDevice3, function() {
                                 datastore.getInfo({
                                                      userId : 4,
                                                      willFindMostRecentSample : true
                                                   }, function(err, info) {
                                    verifySuccess(err, info, validInfoFakeDeviceWithMostRecent3, done);
                                 });
                              });
                           }
                        });
                     }
                  });
               });

               // TODO: need to test a channel that has both a value AND text for a single timestamp--do the test in CLion too!

               it('should return empty channel specs for unknown user ID', function(done) {
                  datastore.getInfo({ userId : 42 }, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and unknown device', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "bogus" }, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and device but unknown channel', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : "bogus" }, function(err, info) {
                     verifySuccess(err, info, emptyChannelSpecs, done);
                  });
               });
               it('should return empty channel specs for known user ID and unknown device and channel', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "unknown", channelName : "bogus" }, function(err, info) {
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
                  datastore.getInfo({ userId : "bubba" }, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the user ID is not an integer', function(done) {
                  datastore.getInfo({ userId : 1.2 }, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the user ID is null', function(done) {
                  datastore.getInfo({ userId : null }, function(err, info) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if the device name has a space in it', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "Foo Bar" }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an empty string', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : '' }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is a "."', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : '.' }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is ".."', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : '..' }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is "sp..eck"', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : 'sp..eck' }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is "sp$eck"', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : 'sp$eck' }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an array', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : [] }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the device name is an object', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : {} }, function(err, info) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if the channel name has a space in it', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck1",
                                       channelName : "Foo Bar"
                                    }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an empty string', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : '' }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is a "."', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : '.' }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is ".."', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : '..' }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is "sp..eck"', function(done) {
                  datastore.getInfo({
                                       userId : 1,
                                       deviceName : "speck1",
                                       channelName : 'sp..eck'
                                    }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is "sp$eck"', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : 'sp$eck' }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an array', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : [] }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the channel name is an object', function(done) {
                  datastore.getInfo({ userId : 1, deviceName : "speck1", channelName : {} }, function(err, info) {
                     verifyValidationError(err, 'channelName', done);
                  });
               });
               it('should fail if the min time is not a number', function(done) {
                  datastore.getInfo({ userId : 1, minTime : "bogus" }, function(err, info) {
                     verifyValidationError(err, 'minTime', done);
                  });
               });
               it('should fail if the max time is not a number', function(done) {
                  datastore.getInfo({ userId : 1, maxTime : "bogus" }, function(err, info) {
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

         describe("getTiles()", function() {
            var verifyGetTilesResponse = function(eventEmitter, expectedResponse, done) {

               var s = "";

               eventEmitter.stdout.on('data', function(data) {
                  s += data;
               });

               eventEmitter.on('close', function() {
                  var json = JSON.parse(s);
                  expect(json).to.deep.equal(expectedResponse);
                  done();
               });
            };

            describe("Successes", function() {
               it('should return correct tile for valid tile request for 1 channel', function(done) {
                  datastore.getTiles([{
                     userId : 1,
                     deviceName : "speck1",
                     channelNames : ["particles"]
                  }], -3, 21630549, function(err, eventEmitter) {
                     if (err) {
                        return done(err);
                     }
                     verifyGetTilesResponse(eventEmitter,
                                            require('./data/gettiles_1.json'),
                                            done);
                  });
               });

               it('should return correct tile for valid tile request for 2 channels', function(done) {
                  datastore.getTiles([{
                                        userId : 1,
                                        deviceName : "speck1",
                                        channelNames : ["particles", "humidity"]
                                     }], -3, 21630549,
                                     function(err, eventEmitter) {
                                        if (err) {
                                           return done(err);
                                        }
                                        verifyGetTilesResponse(eventEmitter,
                                                               require('./data/gettiles_2.json'),
                                                               done);
                                     });
               });
               it('should return correct tile for valid tile request for 2 channels (more verbose)', function(done) {
                  datastore.getTiles([{
                                        userId : 1,
                                        deviceName : "speck1",
                                        channelNames : ["particles"]
                                     },
                                        {
                                           userId : 1,
                                           deviceName : "speck1",
                                           channelNames : ["humidity"]
                                        }], -3, 21630549,
                                     function(err, eventEmitter) {
                                        if (err) {
                                           return done(err);
                                        }
                                        verifyGetTilesResponse(eventEmitter,
                                                               require('./data/gettiles_2.json'),
                                                               done);
                                     });
               });

               it('should return correct tile for valid tile request for 4 channels', function(done) {
                  datastore.getTiles([
                                        { userId : 1, deviceName : "speck1", channelNames : ["particles", "humidity"] },
                                        { userId : 1, deviceName : "speck2", channelNames : ["particles", "humidity"] }
                                     ], 15, 82,
                                     function(err, eventEmitter) {
                                        if (err) {
                                           return done(err);
                                        }
                                        verifyGetTilesResponse(eventEmitter,
                                                               require('./data/gettiles_3.json'),
                                                               done);
                                     });
               });

               it('should return correct tile for valid tile request for 4 channels (more verbose)', function(done) {
                  datastore.getTiles([
                                        { userId : 1, deviceName : "speck1", channelNames : ["particles"] },
                                        { userId : 1, deviceName : "speck1", channelNames : ["humidity"] },
                                        { userId : 1, deviceName : "speck2", channelNames : ["particles"] },
                                        { userId : 1, deviceName : "speck2", channelNames : ["humidity"] }
                                     ], 15, 82,
                                     function(err, eventEmitter) {
                                        if (err) {
                                           return done(err);
                                        }
                                        verifyGetTilesResponse(eventEmitter,
                                                               require('./data/gettiles_3.json'),
                                                               done);
                                     });
               });

               it('should return correct (empty) tile for request containing non-existent user', function(done) {
                  var userIdDeviceChannelObjects = [{
                     userId : 4242,
                     deviceName : "speck1",
                     channelNames : ["particles"]
                  }];
                  datastore.getTiles(userIdDeviceChannelObjects, -3, 21630549, function(err, eventEmitter) {
                     if (err) {
                        return done(err);
                     }
                     var expected = require('./data/gettiles_no_data.json');
                     expected['full_channel_names'] = [userIdDeviceChannelObjects[0].userId + "." + userIdDeviceChannelObjects[0].deviceName + "." + userIdDeviceChannelObjects[0].channelNames[0]];
                     verifyGetTilesResponse(eventEmitter,
                                            expected,
                                            done);
                  });
               });

               it('should return correct (empty) tile for request containing non-existent device', function(done) {
                  var userIdDeviceChannelObjects = [{
                     userId : 1,
                     deviceName : "bogus",
                     channelNames : ["particles"]
                  }];
                  datastore.getTiles(userIdDeviceChannelObjects, -3, 21630549, function(err, eventEmitter) {
                     if (err) {
                        return done(err);
                     }
                     var expected = require('./data/gettiles_no_data.json');
                     expected['full_channel_names'] = [userIdDeviceChannelObjects[0].userId + "." + userIdDeviceChannelObjects[0].deviceName + "." + userIdDeviceChannelObjects[0].channelNames[0]];
                     verifyGetTilesResponse(eventEmitter,
                                            expected,
                                            done);
                  });
               });

               it('should return correct (empty) tile for request containing non-existent channel', function(done) {
                  var userIdDeviceChannelObjects = [{
                     userId : 1,
                     deviceName : "speck1",
                     channelNames : ["bogus"]
                  }];
                  datastore.getTiles(userIdDeviceChannelObjects, -3, 21630549, function(err, eventEmitter) {
                     if (err) {
                        return done(err);
                     }
                     var expected = require('./data/gettiles_no_data.json');
                     expected['full_channel_names'] = [userIdDeviceChannelObjects[0].userId + "." + userIdDeviceChannelObjects[0].deviceName + "." + userIdDeviceChannelObjects[0].channelNames[0]];
                     verifyGetTilesResponse(eventEmitter,
                                            expected,
                                            done);
                  });
               });
            });

            describe("Failures", function() {
               var userIdDeviceChannelObjects = [{
                  userId : 1,
                  deviceName : "speck1",
                  channelNames : ["particles"]
               }];

               it('should fail if userIdDeviceChannelObjects is null', function(done) {
                  datastore.getTiles(null, 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is a number', function(done) {
                  datastore.getTiles(42, 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is a string', function(done) {
                  datastore.getTiles("foobar", 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is an object', function(done) {
                  datastore.getTiles({ "foo" : "bar" }, 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is an empty array', function(done) {
                  datastore.getTiles([], 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });

               it('should fail if userIdDeviceChannelObjects contains an item that is not an object', function(done) {
                  datastore.getTiles([42], 10, 2639, function(err) {
                     verifyValidationError(err, 'userIdDeviceChannelObjects', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects contains an item with an invalid userId', function(done) {
                  datastore.getTiles([{
                     userId : "foo",
                     deviceName : "speck1",
                     channelNames : ["particles"]
                  }], 10, 2639, function(err) {
                     verifyValidationError(err, 'userId', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects contains an item with an invalid deviceName', function(done) {
                  datastore.getTiles([{
                     userId : 1,
                     deviceName : 42,
                     channelNames : ["particles"]
                  }], 10, 2639, function(err) {
                     verifyValidationError(err, 'deviceName', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects contains an item where channelNames is not an array', function(done) {
                  datastore.getTiles([{
                     userId : 1,
                     deviceName : "speck1",
                     channelNames : "foo"
                  }], 10, 2639, function(err) {
                     verifyValidationError(err, 'channelNames', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects contains an item where channelNames is an empty array', function(done) {
                  datastore.getTiles([{
                     userId : 1,
                     deviceName : "speck1",
                     channelNames : []
                  }], 10, 2639, function(err) {
                     verifyValidationError(err, 'channelNames', done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects contains an item with an invalid channelName', function(done) {
                  datastore.getTiles([{
                     userId : 1,
                     deviceName : "speck1",
                     channelNames : [".."]
                  }], 10, 2639, function(err) {
                     verifyValidationError(err, 'channelNames', done);
                  });
               });

               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, null, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  var iAmUndefined;
                  datastore.getTiles(userIdDeviceChannelObjects, iAmUndefined, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 1.2, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, {}, 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, [], 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, "bubba", 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });
               it('should fail for invalid level', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, "2;", 2639, function(err) {
                     verifyValidationError(err, 'level', done);
                  });
               });

               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, null, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, undefined, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, 1.2, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, {}, function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, [], function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, "bubba", function(err) {
                     verifyValidationError(err, 'offset', done);
                  });
               });
               it('should fail for invalid offset', function(done) {
                  datastore.getTiles(userIdDeviceChannelObjects, 10, "2;", function(err) {
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

         describe("exportData()", function() {

            var verifyExportResponse = function(eventEmitter, expectedResponse, done) {
               var csv = "";

               eventEmitter.stdout.on('data', function(data) {
                  csv += data;
               });

               eventEmitter.on('close', function() {
                  expect(csv).to.equal(expectedResponse);
                  done();
               });
            };

            var userId = 3;
            var deviceName = "speck";
            before(function(initDone) {
               datastore.importJson(userId,
                                    deviceName,
                                    [{
                                       "channel_names" : ["humidity", "particles", "raw_particles", "annotation"],
                                       "data" : [
                                          [1384357121, 24, 13, 1, null],
                                          [1384357122, 25, 14, 2, null],
                                          [1384357123, 26, 15, 3, "This is the middle data sample"],
                                          [1384357124, 27, 16, 4, null],
                                          [1384357125, 28, 17, 5, null]
                                       ]
                                    }],
                                    function(err, response) {
                                       return initDone(err);
                                    });
            });

            describe("successes", function() {

               it('should export successfully all channels without filtering', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["humidity", "particles", "raw_particles", "annotation"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.humidity,3.speck.particles,3.speck.raw_particles,3.speck.annotation\n" +
                                                               "1384357121,24,13,1,\n"                                                                   +
                                                               "1384357122,25,14,2,\n"                                                                   +
                                                               "1384357123,26,15,3,\"This is the middle data sample\"\n"                                 +
                                                               "1384357124,27,16,4,\n"                                                                   +
                                                               "1384357125,28,17,5,\n",
                                                               done);
                                       });
               });

               it('should export successfully a single channel without filtering', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles\n" +
                                                               "1384357121,13\n"               +
                                                               "1384357122,14\n"               +
                                                               "1384357123,15\n"               +
                                                               "1384357124,16\n"               +
                                                               "1384357125,17\n",
                                                               done);
                                       });
               });

               it('should ignore multiple instances of a requested channel', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity", "particles", "particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n" +
                                                               "1384357121,13,24\n"                             +
                                                               "1384357122,14,25\n"                             +
                                                               "1384357123,15,26\n"                             +
                                                               "1384357124,16,27\n"                             +
                                                               "1384357125,17,28\n",
                                                               done);
                                       });
               });

               it('should return no records if the maxTime is less than the timestamp of the first data record', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles"]
                                       }],
                                       { maxTime : 1384357120 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles\n",
                                                               done);
                                       });
               });

               it('should return no records if the minTime is greater than the timestamp of the last data record', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles"]
                                       }],
                                       { minTime : 1384357126 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles\n",
                                                               done);
                                       });
               });

               it('should return appropriate records when minTime is specified', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { minTime : 1384357123 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n" +
                                                               "1384357123,15,26\n"                             +
                                                               "1384357124,16,27\n"                             +
                                                               "1384357125,17,28\n",
                                                               done);
                                       });
               });

               it('should return appropriate records when maxTime is specified', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { maxTime : 1384357123 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n" +
                                                               "1384357121,13,24\n"                             +
                                                               "1384357122,14,25\n"                             +
                                                               "1384357123,15,26\n",
                                                               done);
                                       });
               });

               it('should return appropriate records when both minTime and maxTime are specified to select a subset', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { minTime : 1384357122, maxTime : 1384357124 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n" +
                                                               "1384357122,14,25\n"                             +
                                                               "1384357123,15,26\n"                             +
                                                               "1384357124,16,27\n",
                                                               done);
                                       });
               });

               it('should return appropriate records when both minTime and maxTime are specified to select a superset', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { minTime : 1384357120, maxTime : 1384357126 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n" +
                                                               "1384357121,13,24\n"                             +
                                                               "1384357122,14,25\n"                             +
                                                               "1384357123,15,26\n"                             +
                                                               "1384357124,16,27\n"                             +
                                                               "1384357125,17,28\n",
                                                               done);
                                       });
               });

               it('should return no records when both minTime and maxTime are specified, but with values swapped', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { minTime : 1384357124, maxTime : 1384357122 },
                                       function(err, eventEmitter) {
                                          if (err) {
                                             return done(err);
                                          }

                                          verifyExportResponse(eventEmitter,
                                                               "EpochTime,3.speck.particles,3.speck.humidity\n",
                                                               done);
                                       });
               });

            });   // end successes

            describe("failures", function() {
               var verifyValidationError = function(err, eventEmitter, done) {
                  expect(err).to.not.be.null;
                  expect(err instanceof DatastoreError).to.be.true;
                  expect(eventEmitter).to.be.undefined;
                  err.should.have.property('data');
                  err.data.should.have.property('code', 422);
                  err.data.should.have.property('status', 'error');
                  done();
               };
               it('should fail if userIdDeviceChannelObjects is null', function(done) {
                  datastore.exportData(null, null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is a number', function(done) {
                  datastore.exportData(42, null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is a string', function(done) {
                  datastore.exportData("foobar", null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is an object', function(done) {
                  datastore.exportData({ "foo" : "bar" }, null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });
               it('should fail if userIdDeviceChannelObjects is an empty array', function(done) {
                  datastore.exportData([], null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });

               it('should fail if userIdDeviceChannelObjects contains an item that is not an object', function(done) {
                  datastore.exportData([42], null, function(err, eventEmitter) {
                     verifyValidationError(err, eventEmitter, done);
                  });
               });

               it('should fail if the userId is undefined', function(done) {
                  datastore.exportData([{
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if the userId is null', function(done) {
                  datastore.exportData([{
                                          userId : null,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if the userId is invalid', function(done) {
                  datastore.exportData([{
                                          userId : "foo",
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });

               it('should fail if the deviceName is undefined', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if the deviceName is null', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : null,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if the deviceName is invalid', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : "..",
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });

               it('should fail if channelNames is undefined', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if channelNames is null', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : null
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if channelNames is not an array', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : "foo"
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if channelNames is empty', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : []
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });
               it('should fail if channelNames contains an invalid channel name', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ['particles', '..', 'humidity']
                                       }],
                                       null,
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });

               it('should fail if the minTime is not a number', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { minTime : "foo" },
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });

               it('should fail if the maxTime is not a number', function(done) {
                  datastore.exportData([{
                                          userId : userId,
                                          deviceName : deviceName,
                                          channelNames : ["particles", "humidity"]
                                       }],
                                       { maxTime : "foo" },
                                       function(err, eventEmitter) {
                                          verifyValidationError(err, eventEmitter, done);
                                       });
               });

            });   // end failures

         });   // end export()

         describe("deleteDevice()", function() {
            it("Should be able to delete a device", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, 'bubba', function(err) {
                  should.not.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure the correct device was deleted
                  should.deepEqual(userDirBefore, ['bogus', 'bubba', 'speck1', 'speck2']);
                  should.deepEqual(userDirAfter, ['bogus', 'speck1', 'speck2']);

                  done();
               });
            });

            it("Should be able to delete the same device again (no errors, but nothing will actually happen)", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, 'bubba', function(err) {
                  should.not.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure the correct device was deleted
                  should.deepEqual(userDirBefore, ['bogus', 'speck1', 'speck2']);
                  should.deepEqual(userDirAfter, ['bogus', 'speck1', 'speck2']);

                  done();
               });
            });

            it("Should fail to delete a device for a non-existent user", function(done) {
               var userId = "343";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.not.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete a device for an undefined user", function(done) {
               var userId;
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete a device for a null user", function(done) {
               var userId = null;
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete a device for an invalid user", function(done) {
               var userId = "-1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete a device for user '..'", function(done) {
               var userId = "..";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete a device for user '/'", function(done) {
               var userId = "/";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               datastore.deleteDevice(userId, 'speck', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  done();
               });
            });

            it("Should fail to delete an undefined device", function(done) {
               var userId = "1";
               var deviceName;
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, deviceName, function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure nothing was deleted
                  should.deepEqual(userDirAfter, userDirBefore);

                  done();
               });
            });

            it("Should fail to delete a null device", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, null, function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure nothing was deleted
                  should.deepEqual(userDirAfter, userDirBefore);

                  done();
               });
            });

            it("Should fail to delete an empty-string device", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, '', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure nothing was deleted
                  should.deepEqual(userDirAfter, userDirBefore);

                  done();
               });
            });

            it("Should fail to delete a non-existent device", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, 'foo', function(err) {
                  should.not.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure nothing was deleted
                  should.deepEqual(userDirAfter, userDirBefore);

                  done();
               });
            });

            it("Should fail to delete an invalid device", function(done) {
               var userId = "1";
               var dataDirBefore = fs.readdirSync(DATA_DIR);
               var userDirBefore = fs.readdirSync(path.join(DATA_DIR, userId));
               datastore.deleteDevice(userId, '..', function(err) {
                  should.exist(err);

                  var dataDirAfter = fs.readdirSync(DATA_DIR);
                  var userDirAfter = fs.readdirSync(path.join(DATA_DIR, userId));

                  // make sure no user directories were deleted
                  should.deepEqual(dataDirAfter, dataDirBefore);

                  // make sure nothing was deleted
                  should.deepEqual(userDirAfter, userDirBefore);

                  done();
               });
            });

         });   // End deleteDevice()
      });   // end BodyTrackDatastore
   });
});
