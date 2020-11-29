package imd.smartmetropolis.aqueconnect.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.BASE_PATH;

public class PropertiesParams {

    public static String HASH_CONFIG_VALUE;
    public static String USER_NAME_HDFS;
    public static String BASE_URL;
    public static String BASE_URL_AQUECONNECT;
    public static String HDFS_URI;
    public static String WEB_HDFS_URL;
    public static String BASE_AQUEDUCTE_URL;

    static {
        try {
            initialize();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initialize() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream source = classLoader.getResourceAsStream("properties.cfg");
        Properties conf = new Properties();
        conf.load(source);

        HASH_CONFIG_VALUE = String.valueOf(conf.getProperty("HASH_CONFIG_VALUE"));
        USER_NAME_HDFS = String.valueOf(conf.getProperty("USER_NAME_HDFS"));
        BASE_URL = String.valueOf(conf.getProperty("BASE_URL"));
        BASE_URL_AQUECONNECT = BASE_URL + "aqueconnect/";
        HDFS_URI = String.valueOf(conf.getProperty("HDFS_URI"));
        WEB_HDFS_URL = conf.getProperty("WEB_HDFS_URL") + BASE_PATH;
        BASE_AQUEDUCTE_URL = String.valueOf(conf.getProperty("BASE_AQUEDUCTE_URL"));
    }

}
