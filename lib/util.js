const INTEGER_PATTERN = /^-?\d+$/;
const POSITIVE_INTEGER_PATTERN = /^[1-9]\d*$/;

/**
 * Returns true if the given argument is not <code>undefined</code> and is non <code>null</code>; <code>false</code>
 * otherwise.
 *
 * @param {*} arg
 * @returns {boolean}
 */
var isDefined = function(arg) {
   return (typeof arg !== 'undefined' && arg != null)
};

/**
 * Returns <code>true</code> if the given argument is not <code>undefined</code>, is non <code>null</code>, and is a <code>string</code>; <code>false</code> otherwise.
 *
 * @param {*} arg
 * @returns {boolean}
 */
var isString = function(arg) {
   // got this from http://stackoverflow.com/a/1303650
   return (isDefined(arg) && toString.call(arg) == '[object String]');
};

/**
 * Returns true if the given argument is a number; <code>false</code> otherwise.  I found this at:
 * <a href="http://stackoverflow.com/a/1830844">http://stackoverflow.com/a/1830844</a>.
 *
 * @param {*} arg
 * @return {boolean}
 */
var isNumber = function(arg) {
   return !isNaN(parseFloat(arg)) && isFinite(arg);
};

/**
 * Returns <code>true</code> if the given argument is defined, non-null, not an array, and is an object; returns
 * <code>false</code> otherwise.
 *
 * @param {*} arg
 */
var isObject = function(arg) {
   // Based on various answers at http://stackoverflow.com/questions/8511281/check-if-a-variable-is-an-object-in-javascript
   return arg !== null && typeof arg === 'object' && !Array.isArray(arg);
};

/**
 * Returns <code>true</code> if the given value is an <code>integer</code>, or a <code>string</code> representation of
 * an <code>integer</code>. Note that this method isn't very smart.  It will return <code>false</code> for
 * <code>strings</code> such as <code>"0x45"</code>, <code>"9,999"</code>, and <code>"1E3"</code>, but will return
 * <code>true</code> for the numbers <code>0x45</code> and <code>1E3</code>.  It will also return <code>true</code> for
 * numbers which get auto-rounded such as <code>1.000000000000000000001</code> (but <code>false</code> for a
 * <code>string</code> version of such numbers).  So, kinda stupid, but sufficient for my needs here.
 *
 * @param {*} n
 * @returns {boolean}
 */
var isInt = function(n) {
   // Based on code from from http://stackoverflow.com/a/3886106
   return isNumber(n) && n % 1 === 0 && INTEGER_PATTERN.test(String(n));
};

/**
 * Performs strict tests to ensure the given value is a positive integer
 *
 * @param {*} n
 * @returns {boolean}
 */
var isPositiveInt = function(n) {
   if (typeof n !== 'undefined' && n != null) {
      var nStr = null;
      if (isString(n)) {
         nStr = n;
      } else if (isNumber(n)) {
         nStr = String(n);
      }

      return nStr != null && POSITIVE_INTEGER_PATTERN.test(nStr);
   }
   return false;
};

module.exports.isDefined = isDefined;
module.exports.isString = isString;
module.exports.isNumber = isNumber;
module.exports.isObject = isObject;
module.exports.isInt = isInt;
module.exports.isPositiveInt = isPositiveInt;