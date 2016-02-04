package transliterate;

import com.eterno.indiUtil.Transliterate;

public class TransliterateUtil {
    public String transliterate(String value) {
        Transliterate transliterate = new Transliterate();

        char[] charArrTitle = value.toCharArray();
        char[] destArrTitle = new char[charArrTitle.length * 4];
        byte[] langArrTitle = new byte[charArrTitle.length * 4];
        transliterate.ReverseTransPlainEnglish(charArrTitle, charArrTitle.length, destArrTitle, langArrTitle);
        String trans = new String(destArrTitle);
        return trans.trim();
    }
}
