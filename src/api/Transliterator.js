import Java from 'java';
import Path from 'path';

Java.classpath.push(Path.join(__dirname, '../java/classes'));
Java.classpath.push(Path.join(__dirname, '../java/jars/IndiUtil.jar'));
Java.options.push('-Xms1024m');
Java.options.push('-Xmx1024m');

export default class Transliterator {
    constructor() {
        this.transliterator = Java.newInstanceSync('transliterate.TransliterateUtil');
    }

    transliterate(value) {
        return this.transliterator.transliterateSync(value);
    }
}