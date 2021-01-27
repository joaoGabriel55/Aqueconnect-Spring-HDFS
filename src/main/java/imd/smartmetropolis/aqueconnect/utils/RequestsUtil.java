package imd.smartmetropolis.aqueconnect.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_URL_AQUECONNECT;
import static org.apache.http.HttpStatus.SC_OK;

public class RequestsUtil {
    public static final String AQUECONNECT_DIRECTORY_SERVICE_URL = BASE_URL_AQUECONNECT + "directory/";
    public static final String BASE_PATH = "/user/data/";
    public static final String HASH_CONFIG = "hash-config";

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

    public static HttpResponse execute(HttpEntityEnclosingRequestBase request) throws IOException {
        return HttpClientBuilder.create().build().execute(request);
    }

    public static HttpEntityEnclosingRequestBase httpPost(String url, Object payload, Map<String, String> headers) {
        HttpPost request = new HttpPost(url);
        headers.remove("content-length");
        headers.remove("content-type");
        headers.forEach(request::addHeader);
        request.setEntity(buildEntity(payload));
        return request;
    }

    @SuppressWarnings("ALL")
    public static Map<String, Object> buildResponse(int statusCode,
                                                    String statusMessage,
                                                    InputStream inputStream) throws IOException {
        Map<String, Object> response = new HashMap<>();
        if (statusCode == SC_OK) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> mapFromJson = mapper.readValue(readBody(inputStream), Map.class);
            response.put("statusCode", statusCode);
            response.put("message", statusMessage);
            response.putAll(mapFromJson);
            return response;
        }
        response.put("statusCode", statusCode);
        response.put("message", statusMessage);
        return response;
    }

    private static StringEntity buildEntity(Object payload) {
        String jsonString;
        if (payload instanceof String)
            jsonString = (String) payload;
        else if (!(payload instanceof ArrayList))
            jsonString = new JSONObject(payload).toString();
        else
            jsonString = payload.toString();

        return new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    }

    private static String readBody(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder body = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null)
            body.append(line);

        return body.toString();
    }

}
