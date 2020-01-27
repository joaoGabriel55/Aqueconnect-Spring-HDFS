package imd.smartmetropolis.aqueconnect.service;

import java.util.HashMap;
import java.util.Map;

public class HDFSService {

    public static final String AQUECONNECT_URL = "http://localhost:7000/aqueconnect/directory/";

    public static Map<String, Object> buildHATEOAS(String userId, Map<String, Object> data) {

        if ((data.containsKey("type") && data.containsKey("pathSuffix")) && data.get("type").equals("DIRECTORY")) {
            Map<String, Object> _links = new HashMap<>();
            _links.put("self",
                    AQUECONNECT_URL + userId + "/?path=" + data.get("pathSuffix")
            );
            data.put("_links", _links);
        }
        return data;
    }

}
