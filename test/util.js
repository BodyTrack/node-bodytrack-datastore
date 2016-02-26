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

describe("isPositiveInt()", function() {
   var doTest = function(i, expectedOutcome) {
      var msg = null;
      if (util.isString(i)) {
         msg = 'should return ' + expectedOutcome + ' for "' + i + '"';
      }
      else {
         msg = 'should return ' + expectedOutcome + ' for ' + i;
      }

      expectedOutcome = !!expectedOutcome;
      it(msg, function(done) {
         if (expectedOutcome) {
            util.isPositiveInt(i).should.be.true();
         }
         else {
            util.isPositiveInt(i).should.be.false();
         }
         done();
      });
   };

   var doTestForNumAndString = function(num, expectedOutcome) {
      doTest(num, expectedOutcome);
      doTest(String(num), expectedOutcome);
   };

   for (var i = 1; i < 20; i++) {
      doTestForNumAndString(i, true);
   }
   doTestForNumAndString(42, true);
   doTestForNumAndString(343, true);
   doTestForNumAndString(101010, true);
   it("should return true for 42.000000000000000001", function(done) {
      util.isPositiveInt(42.000000000000000001).should.be.true();
      done();
   });

   doTestForNumAndString(0, false);
   // TODO: doTest(01, false);
   doTestForNumAndString(-1, false);
   doTestForNumAndString(-10, false);
   doTestForNumAndString(-10.0, false);
   doTestForNumAndString(-10.3, false);
   doTest('42.', false);
   doTest('42.0', false);
   doTest('42.1', false);
   doTest('42.000000000000000001', false);
   doTest('', false);
   doTest(null, false);

   var x;
   doTest(x, false);
   doTest(1 / 0, false);
   doTest(-1 / 0, false);
   doTest(parseInt("bogus"), false);

   it("should return true for .000000000000000001", function(done) {
      util.isPositiveInt(.000000000000000001).should.be.false();
      done();
   });
   it("should return false for 'bogus'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for '0123'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for '123a'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for 'a123'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for '1a23'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for ' 1'", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for '1 '", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for an array", function(done) {
      util.isPositiveInt([]).should.be.false();
      done();
   });

   it("should return false for an object", function(done) {
      util.isPositiveInt({}).should.be.false();
      done();
   });

});   // End isPositiveInt()