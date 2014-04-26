var expect = require('chai').expect;
var util = require('../lib/util');

describe("isDefined()", function() {
   it('should return false for null', function() {
      expect(util.isDefined(null)).to.be.false;
   });
   it('should return false for undefined', function() {
      expect(util.isDefined(undefined)).to.be.false;
   });

   it('should return true for something that is defined and non-null', function() {
      expect(util.isDefined("foo")).to.be.true;
   });
});

describe("isString()", function() {
   it('should return true for empty string', function() {
      expect(util.isString('')).to.be.true;
   });
   it('should return true for foo', function() {
      expect(util.isString('foo')).to.be.true;
   });
   it('should return true for new String()', function() {
      expect(util.isString(new String())).to.be.true;
   });
   it('should return true for new String("")', function() {
      expect(util.isString(new String(""))).to.be.true;
   });
   it('should return true for new String("foo")', function() {
      expect(util.isString(new String("foo"))).to.be.true;
   });

   it('should return false for 1', function() {
      expect(util.isString(1)).to.be.false;
   });
   it('should return false for {}', function() {
      expect(util.isString({})).to.be.false;
   });
   it('should return false for []', function() {
      expect(util.isString([])).to.be.false;
   });
   it('should return false for null', function() {
      expect(util.isString(null)).to.be.false;
   });
   it('should return false for undefined', function() {
      expect(util.isString(undefined)).to.be.false;
   });
   it('should return false for new Array()', function() {
      expect(util.isString(new Array())).to.be.false;
   });
   it('should return false for new Object()', function() {
      expect(util.isString(new Object())).to.be.false;
   });
   it('should return false for true', function() {
      expect(util.isString(true)).to.be.false;
   });
   it('should return false for false', function() {
      expect(util.isString(false)).to.be.false;
   });
});

describe("isInt()", function() {
   it('should return false for undefined', function() {
      expect(util.isInt(undefined)).to.be.false;
   });
   it('should return false for null', function() {
      expect(util.isInt(null)).to.be.false;
   });
   it('should return false for the empty string', function() {
      expect(util.isInt('')).to.be.false;
   });
   it('should return false for "foo"', function() {
      expect(util.isInt("foo")).to.be.false;
   });
   it('should return false for 0.2', function() {
      expect(util.isInt(0.2)).to.be.false;
   });
   it('should return false for 14.3', function() {
      expect(util.isInt(14.3)).to.be.false;
   });
   it('should return false for 0.7', function() {
      expect(util.isInt(0.7)).to.be.false;
   });
   it('should return false for "9,999"', function() {
      expect(util.isInt("9,999")).to.be.false;
   });
   it('should return false for "0x45"', function() {
      expect(util.isInt("0x45")).to.be.false;
   });
   it('should return false for "1E3"', function() {
      expect(util.isInt("1E3")).to.be.false;
   });
   it('should return false for "1.000000000000000000001"', function() {
      expect(util.isInt("1.000000000000000000001")).to.be.false;
   });
   it('should return false for "-1.000000000000000000001"', function() {
      expect(util.isInt("-1.000000000000000000001")).to.be.false;
   });

   it('should return true for 0', function() {
      expect(util.isInt(0)).to.be.true;
   });
   it('should return true for "0"', function() {
      expect(util.isInt("0")).to.be.true;
   });
   it('should return true for 1', function() {
      expect(util.isInt(1)).to.be.true;
   });
   it('should return true for "1"', function() {
      expect(util.isInt("1")).to.be.true;
   });
   it('should return true for 11', function() {
      expect(util.isInt(11)).to.be.true;
   });
   it('should return true for "11"', function() {
      expect(util.isInt("11")).to.be.true;
   });
   it('should return true for 343', function() {
      expect(util.isInt(343)).to.be.true;
   });
   it('should return true for "343"', function() {
      expect(util.isInt("343")).to.be.true;
   });
   it('should return true for 0000349', function() {
      expect(util.isInt(0000349)).to.be.true;
   });
   it('should return true for "0000349"', function() {
      expect(util.isInt("0000349")).to.be.true;
   });
   it('should return true for 1.000000000000000000001', function() {
      expect(util.isInt(1.000000000000000000001)).to.be.true;
   });
   it('should return true for -1', function() {
      expect(util.isInt(-1)).to.be.true;
   });
   it('should return true for "-1"', function() {
      expect(util.isInt("-1")).to.be.true;
   });
   it('should return true for -11', function() {
      expect(util.isInt(-11)).to.be.true;
   });
   it('should return true for "-11"', function() {
      expect(util.isInt("-11")).to.be.true;
   });
   it('should return true for -343', function() {
      expect(util.isInt(-343)).to.be.true;
   });
   it('should return true for "-343"', function() {
      expect(util.isInt("-343")).to.be.true;
   });
   it('should return true for -0000349', function() {
      expect(util.isInt(-0000349)).to.be.true;
   });
   it('should return true for "-0000349"', function() {
      expect(util.isInt("-0000349")).to.be.true;
   });
   it('should return true for -1.000000000000000000001', function() {
      expect(util.isInt(-1.000000000000000000001)).to.be.true;
   });
});

describe("removeTrailingSlash()", function() {
   it('should return [] for []', function() {
      expect(util.removeTrailingSlash('')).to.equal('');
   });
   it('should return [] for [/]', function() {
      expect(util.removeTrailingSlash('/')).to.equal('');
   });
   it('should return [] for [//]', function() {
      expect(util.removeTrailingSlash('//')).to.equal('');
   });
   it('should return [] for [///]', function() {
      expect(util.removeTrailingSlash('///')).to.equal('');
   });
   it('should return [foo] for [foo]', function() {
      expect(util.removeTrailingSlash('foo')).to.equal('foo');
   });
   it('should return [foo] for [foo/]', function() {
      expect(util.removeTrailingSlash('foo/')).to.equal('foo');
   });
   it('should return [foo] for [foo//]', function() {
      expect(util.removeTrailingSlash('foo//')).to.equal('foo');
   });
   it('should return [foo] for [foo///]', function() {
      expect(util.removeTrailingSlash('foo///')).to.equal('foo');
   });
   it('should return [//foo] for [//foo]', function() {
      expect(util.removeTrailingSlash('//foo')).to.equal('//foo');
   });
   it('should return [//foo] for [//foo/]', function() {
      expect(util.removeTrailingSlash('//foo/')).to.equal('//foo');
   });
   it('should return [//foo] for [//foo//]', function() {
      expect(util.removeTrailingSlash('//foo//')).to.equal('//foo');
   });
   it('should return [//foo] for [//foo///]', function() {
      expect(util.removeTrailingSlash('//foo///')).to.equal('//foo');
   });
   it('should return [' + null + '] for [' + null + ']', function() {
      expect(util.removeTrailingSlash(null)).to.equal(null);
   });
   it('should return [' + undefined + '] for [' + undefined + ']', function() {
      expect(util.removeTrailingSlash(undefined)).to.equal(undefined);
   });
});
