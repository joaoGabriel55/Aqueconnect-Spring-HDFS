package imd.smartmetropolis.aqueconnect.security;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class PermissionChecker {

    private final static Logger LOG = LoggerFactory.getLogger(PermissionChecker.class);

    /**
     * Check if User from IDM have permission to access Smart Sync API.
     */
    public boolean checkSmartSyncPermissionAccess(String sgeolInstance, String userToken, HttpServletRequest req) {
        if (userToken != null && !userToken.equals("") && sgeolInstance != null && !sgeolInstance.equals("")) {
            HttpGet request = new HttpGet(sgeolInstance + "/idm/users/info");
            // add request headers
            request.addHeader("user-token", userToken);
            try (CloseableHttpClient httpClient = HttpClients.createDefault();
                 CloseableHttpResponse response = httpClient.execute(request)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    // return it as a String
                    String result = EntityUtils.toString(entity);
                    JSONObject userLoggedJson = new JSONObject(result);
                    req.setAttribute("user-id", userLoggedJson.getString("id"));
                    JSONArray roles = userLoggedJson.getJSONArray("roles");
                    for (Object role : roles) {
                        JSONObject roleJson = new JSONObject(role.toString());
                        if (roleJson.getString("name").contains("gerente")) {
                            LOG.info("Authentication Status: {}", "SUCCESS");
                            return true;
                        }
                    }
                }
            } catch (ParseException | IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }
}
