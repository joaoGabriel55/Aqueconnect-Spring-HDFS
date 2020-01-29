package imd.smartmetropolis.aqueconnect.service;

import java.util.HashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.BASE_URL_AQUECONNECT;

public class HDFSService {

    private static final String AQUECONNECT_DIRECTORY_SERVICE_URL = BASE_URL_AQUECONNECT + "directory/";

    public static Map<String, Object> buildHATEOAS(String userId, Map<String, Object> data) {

        if ((data.containsKey("type") && data.containsKey("pathSuffix")) && data.get("type").equals("DIRECTORY")) {
            Map<String, Object> _links = new HashMap<>();
            _links.put("self",
                    AQUECONNECT_DIRECTORY_SERVICE_URL + userId + "/?path=" + data.get("pathSuffix")
            );
            data.put("_links", _links);
        }
        return data;
    }

}
