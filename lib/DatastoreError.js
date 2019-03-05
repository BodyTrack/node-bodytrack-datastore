/**
 * Creates an instance of a <code>DatastoreError</code> with the given Error or <code>data</code> object and
 * optional <code>message</code>.  If the first argument is an instance of an Error, then this instance will use the
 * give error's <code>data</code> and <code>message</code> fields.  Otherwise, this instance will consider the first
 * argument to be a data object, and use it for the instance's <code>data</code> field and use the second argument as
 * the <code>message</code> field.
 *
 * @param {Error|object} data Extra data about the error.  Will be saved in this instance's <code>data</code> property.
 * @param {string} [message] Optional message about the error.
 * @constructor
 */
class DatastoreError extends Error {
   constructor(errorOrData, message) {
      super();

      if (Error.captureStackTrace) {
         Error.captureStackTrace(this, DatastoreError);
      }

      this.name = this.constructor.name;
      if (errorOrData instanceof Error) {
         this.data = errorOrData.data;
         this.message = errorOrData.message;
      } else {
         this.data = errorOrData;
         this.message = message;
      }
      this.message = this.message || "Datastore error";
   }
}
module.exports = DatastoreError;
