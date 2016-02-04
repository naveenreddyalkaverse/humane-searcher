export default class ExtensibleError extends Error {
    constructor(message, data) {
        super(message);
        this.name = this.constructor.name;
        this.message = message;
        this.data = data;
        Error.captureStackTrace(this, this.constructor.name);
    }
}