import ExtensibleError from './ExtensibleError';

export default class ValidationError extends ExtensibleError {
    constructor(message, data) {
        super(message, data);
    }
}