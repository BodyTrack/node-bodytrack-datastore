var httpStatusCodes = require('http-status');

/**
 * Creates and returns JSend success object.
 *
 * @param {*} data wrapper for any data returned by the API call. If the call returns no data, this parameter should be
 * set to <code>null</code>.
 * @param {number} [httpStatus] the HTTP status code to use for the response, defaults to 200 (OK).  If specified, it
 * should be an HTTP status code in the range [200, 299].
 */
var createJSendSuccess = function(data, httpStatus) {
   httpStatus = httpStatus || httpStatusCodes.OK;
   return {
      code : httpStatus,
      status : 'success',
      data : data
   }
};

/**
 * Creates and returns JSend error object for a client error, for when an API call is rejected due to invalid data or
 * call conditions.
 *
 * @param {string} message A meaningful, end-user-readable (or at the least log-worthy) message, explaining what went
 * wrong.
 * @param {*} data details of why the request failed. If the reasons for failure correspond to POST values, the response
 * object's keys SHOULD correspond to those POST values. Can be <code>null</code>.
 * @param {number} [httpStatus] the HTTP status code to use for the response, defaults to 400 (Bad Request). If
 * specified, it should be an HTTP status code in the range [400, 499].
 */
var createJSendClientError = function(message, data, httpStatus) {
   httpStatus = httpStatus || httpStatusCodes.BAD_REQUEST;
   return {
      code : httpStatus,
      status : 'error',   // JSend calls actually calls for "fail", but that seems counterintuitive and wrong
      data : data,
      message : message
   }
};

/**
 * Creates and returns JSend error object for a client validation error, for when an API call is rejected due to a data
 * validation error.  The code will be HTTP status code 422 (Unprocessable Entity).
 *
 * @param {string} message A meaningful, end-user-readable (or at the least log-worthy) message, explaining what went
 * wrong.
 * @param {*} data details of why the request failed. If the reasons for failure correspond to POST values, the response
 * object's keys SHOULD correspond to those POST values. Can be <code>null</code>.
 */
var createJSendClientValidationError = function(message, data) {
   return {
      code : httpStatusCodes.UNPROCESSABLE_ENTITY,
      status : 'error',   // JSend calls actually calls for "fail", but that seems counterintuitive and wrong
      data : data,
      message : message
   }
};

/**
 * Creates and returns JSend failure object for a server error, for when an API call fails due to an error on the
 * server.
 *
 * @param {string} message A meaningful, end-user-readable (or at the least log-worthy) message, explaining what went
 * wrong.
 * @param {*} [data] A generic container for any other information about the error, i.e. the conditions that caused the
 * error, stack traces, etc. Can be <code>null</code>.
 * @param {number} [httpStatus] the HTTP status code to use for the response, defaults to 500 (Internal Server Error).
 * If specified, it should be an HTTP status code in the range [500, 599].
 */
var createJSendServerError = function(message, data, httpStatus) {
   httpStatus = httpStatus || httpStatusCodes.INTERNAL_SERVER_ERROR;
   return {
      code : httpStatus,
      status : 'fail',  // JSend calls actually calls for "error", but that seems counterintuitive and wrong
      data : data,
      message : message
   };
};

module.exports.createJSendSuccess = createJSendSuccess;
module.exports.createJSendClientError = createJSendClientError;
module.exports.createJSendClientValidationError = createJSendClientValidationError;
module.exports.createJSendServerError = createJSendServerError;