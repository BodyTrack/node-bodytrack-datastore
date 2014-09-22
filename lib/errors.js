/**
 * Creates an instance of a <code>DatastoreError</code> with the given <code>data</code> object and
 * optional <code>message</code>.
 *
 * @param {object} data Extra data about the error.  Will be saved in this instance's <code>data</code> property.
 * @param {string} [message] Optional message about the error.
 * @constructor
 */
function DatastoreError(data, message) {
   this.constructor.prototype.__proto__ = Error.prototype;
   Error.captureStackTrace(this, this.constructor);
   this.name = this.constructor.name;
   this.data = data;
   this.message = message || "Datastore error";
}

module.exports.DatastoreError = DatastoreError;
