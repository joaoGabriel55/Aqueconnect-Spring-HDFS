package imd.smartmetropolis.aqueconnect.utils;

import java.io.UnsupportedEncodingException;

public class FormatterUtil {

    public static Object encodeText(Object data) throws UnsupportedEncodingException {
        if (!(data instanceof String))
            return data;
        String text = (String) data;
        String converted = new String(text.getBytes("ISO-8859-15"), "UTF-8");
        if (converted.contains("�"))
            return text;
        return converted;
    }
}
