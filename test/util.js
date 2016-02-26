var should = require('should');
var util = require('../lib/util');

describe("isDefined()", function() {
   it('should return false for null', function() {
      util.isDefined(null).should.be.false();
   });
   it('should return false for undefined', function() {
      util.isDefined(undefined).should.be.false();
   });

   it('should return true for something that is defined and non-null', function() {
      util.isDefined("foo").should.be.true();
   });
});

describe("isString()", function() {
   it('should return true for empty string', function() {
      util.isString('').should.be.true();
   });
   it('should return true for foo', function() {
      util.isString('foo').should.be.true();
   });
   it('should return true for new String()', function() {
      //noinspection JSPrimitiveTypeWrapperUsage
      util.isString(new String()).should.be.true();
   });
   it('should return true for new String("")', function() {
      //noinspection JSPrimitiveTypeWrapperUsage
      util.isString(new String("")).should.be.true();
   });
   it('should return true for new String("foo")', function() {
      //noinspection JSPrimitiveTypeWrapperUsage
      util.isString(new String("foo")).should.be.true();
   });

   it('should return false for 1', function() {
      util.isString(1).should.be.false();
   });
   it('should return false for {}', function() {
      util.isString({}).should.be.false();
   });
   it('should return false for []', function() {
      util.isString([]).should.be.false();
   });
   it('should return false for null', function() {
      util.isString(null).should.be.false();
   });
   it('should return false for undefined', function() {
      util.isString(undefined).should.be.false();
   });
   it('should return false for new Array()', function() {
      //noinspection JSPrimitiveTypeWrapperUsage
      util.isString(new Array()).should.be.false();
   });
   it('should return false for new Object()', function() {
      //noinspection JSPrimitiveTypeWrapperUsage,JSClosureCompilerSyntax
      util.isString(new Object()).should.be.false();
   });
   it('should return false for true', function() {
      util.isString(true).should.be.false();
   });
   it('should return false for false', function() {
      util.isString(false).should.be.false();
   });
});

describe("isInt()", function() {
   it('should return false for undefined', function() {
      util.isInt(undefined).should.be.false();
   });
   it('should return false for null', function() {
      util.isInt(null).should.be.false();
   });
   it('should return false for the empty string', function() {
      util.isInt('').should.be.false();
   });
   it('should return false for "foo"', function() {
      util.isInt("foo").should.be.false();
   });
   it('should return false for 0.2', function() {
      util.isInt(0.2).should.be.false();
   });
   it('should return false for 14.3', function() {
      util.isInt(14.3).should.be.false();
   });
   it('should return false for 0.7', function() {
      util.isInt(0.7).should.be.false();
   });
   it('should return false for "9,999"', function() {
      util.isInt("9,999").should.be.false();
   });
   it('should return false for "0x45"', function() {
      util.isInt("0x45").should.be.false();
   });
   it('should return false for "1E3"', function() {
      util.isInt("1E3").should.be.false();
   });
   it('should return false for "1.000000000000000000001"', function() {
      util.isInt("1.000000000000000000001").should.be.false();
   });
   it('should return false for "-1.000000000000000000001"', function() {
      util.isInt("-1.000000000000000000001").should.be.false();
   });

   it('should return true for 0', function() {
      util.isInt(0).should.be.true();
   });
   it('should return true for "0"', function() {
      util.isInt("0").should.be.true();
   });
   it('should return true for 1', function() {
      util.isInt(1).should.be.true();
   });
   it('should return true for "1"', function() {
      util.isInt("1").should.be.true();
   });
   it('should return true for 11', function() {
      util.isInt(11).should.be.true();
   });
   it('should return true for "11"', function() {
      util.isInt("11").should.be.true();
   });
   it('should return true for 343', function() {
      util.isInt(343).should.be.true();
   });
   it('should return true for "343"', function() {
      util.isInt("343").should.be.true();
   });
   it('should return true for 0000349', function() {
      //noinspection OctalIntegerJS
      util.isInt(0000349).should.be.true();
   });
   it('should return true for "0000349"', function() {
      util.isInt("0000349").should.be.true();
   });
   it('should return true for 1.000000000000000000001', function() {
      util.isInt(1.000000000000000000001).should.be.true();
   });
   it('should return true for -1', function() {
      util.isInt(-1).should.be.true();
   });
   it('should return true for "-1"', function() {
      util.isInt("-1").should.be.true();
   });
   it('should return true for -11', function() {
      util.isInt(-11).should.be.true();
   });
   it('should return true for "-11"', function() {
      util.isInt("-11").should.be.true();
   });
   it('should return true for -343', function() {
      util.isInt(-343).should.be.true();
   });
   it('should return true for "-343"', function() {
      util.isInt("-343").should.be.true();
   });
   it('should return true for -0000349', function() {
      //noinspection OctalIntegerJS
      util.isInt(-0000349).should.be.true();
   });
   it('should return true for "-0000349"', function() {
      util.isInt("-0000349").should.be.true();
   });
   it('should return true for -1.000000000000000000001', function() {
      util.isInt(-1.000000000000000000001).should.be.true();
   });
});