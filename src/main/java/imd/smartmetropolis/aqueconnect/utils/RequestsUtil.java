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

import static org.apache.http.HttpStatus.SC_OK;

public class RequestsUtil {
    public static final String SGEOL_INSTANCE = "sgeol-instance";
    public static final String APP_TOKEN = "application-token";
    public static final String USER_TOKEN = "user-token";

    public static HttpResponse execute(HttpEntityEnclosingRequestBase request) throws IOException {
        return HttpClientBuilder.create().build().execute(request);
    }

    public static HttpEntityEnclosingRequestBase httpPost(String url, Object payload, Map<String, String> headers) {
        HttpPost request = new HttpPost(url);
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
