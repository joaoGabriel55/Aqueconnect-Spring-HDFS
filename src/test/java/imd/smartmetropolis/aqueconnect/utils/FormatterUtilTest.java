package imd.smartmetropolis.aqueconnect.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class FormatterUtilTest {

    @Test
    public void encodeTest() throws UnsupportedEncodingException {
        String correctString = "Ceará-Mirim";
        String stringConverted = (String) FormatterUtil.encodeText("CearÃ¡-Mirim");
        Assert.assertEquals(correctString, stringConverted);

        String stringConverted2 = (String) FormatterUtil.encodeText(correctString);
        Assert.assertEquals(correctString, stringConverted2);
    }

}
