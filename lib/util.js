const INTEGER_PATTERN = /^-?\d+$/;

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

// Got this from http://stackoverflow.com/a/1830844
var isNumber = function(n) {
   return !isNaN(parseFloat(n)) && isFinite(n);
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
 * Removes all slash characters (<code>/</code>) from the end of the string and returns the result.  If the given
 * string is <code>undefined</code> or <code>null</code>, the string is returned unchanged.
 *
 * @param {string} str
 * @returns {string}
 */
var removeTrailingSlash = function(str) {
   if (isDefined(str)) {
      while (str.slice(-1) == "/") {
         str = str.slice(0, -1);  // chop off the last character
      }
   }
   return str;
};

module.exports.isDefined = isDefined;
module.exports.isString = isString;
module.exports.isInt = isInt;
module.exports.removeTrailingSlash = removeTrailingSlash;