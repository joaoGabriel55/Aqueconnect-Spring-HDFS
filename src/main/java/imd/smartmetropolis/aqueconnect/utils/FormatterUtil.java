package imd.smartmetropolis.aqueconnect.utils;

import java.text.Normalizer;

public class FormatterUtil {
    public static String removeAccents(String str) {
        return Normalizer.normalize(str, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }
}
