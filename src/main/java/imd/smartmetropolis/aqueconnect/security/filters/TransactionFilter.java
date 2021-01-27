package imd.smartmetropolis.aqueconnect.security.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.utils.Response;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.HASH_CONFIG_VALUE;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.HASH_CONFIG;

@Component
@Log4j2
@Order(1)
public class TransactionFilter implements Filter {

    @Value("${aqueconnect.use-own-hash-config}")
    private String useOwnHashConfig;

    @Value("${aqueconnect.hash-config}")
    private String aqueconnectOwnHashConfigValue;

    @Override
    public void init(final FilterConfig filterConfig) {
        log.info("Initializing filter :{}", this);
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException {
        HttpServletRequest req = (HttpServletRequest) request;
        log.info("Using own hash-config-value: {}", useOwnHashConfig);
        log.info("Starting Transaction for req: {}", req.getRequestURI());
        try {

            String hashConfig = req.getHeader(HASH_CONFIG);
            if (hashConfig == null) {
                String msg = "Inform a 'hash-config' header.";
                log.error(msg);
                buildResponseError(response, msg);
                return;
            }

            String errorMsg = isValidHashConfigValue(hashConfig);
            if (errorMsg != null) {
                log.error(errorMsg);
                buildResponseError(response, errorMsg);
                return;
            }

            if (req.getRequestURI().contains("/file-import-setup-resource") && Boolean.valueOf(useOwnHashConfig)) {
                String msg = "This service endpoint is unavailable when 'use-own-hash-config' has 'true'";
                log.error(msg);
                buildResponseError(response, msg);
                return;
            }

            chain.doFilter(request, response);
        } catch (Exception e) {
            String msg = e.getMessage();
            log.error(msg);
            buildResponseError(response, e.getMessage());
            return;
        }
        log.info("Committing Transaction for req :{}", req.getRequestURI());
    }

    private String isValidHashConfigValue(String hashConfig) {
        String msg = "Inform a 'hash-config' valid for obtain API access.";
        if (Boolean.valueOf(useOwnHashConfig)) {
            if (!aqueconnectOwnHashConfigValue.equals(hashConfig)) {
                return msg;
            }
        } else {
            if (!externalAppConfigHashConfigIsValid(hashConfig)) {
                return msg;
            }
        }
        return null;
    }

    private boolean externalAppConfigHashConfigIsValid(String hashConfig) {
        HttpGet request = new HttpGet(BASE_AQUEDUCTE_URL + "external-app-config/" + hashConfig);
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                JSONObject result = (JSONObject) toJSONObject(response.getEntity().getContent()).get("data");
                String hashConfigDb = (String) result.get("hashConfig");
                if (!hashConfigDb.equals(HASH_CONFIG_VALUE))
                    return false;
                return true;
            }
        } catch (ParseException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private JSONObject toJSONObject(InputStream content) throws IOException, JSONException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
        String jsonText = IOUtils.toString(rd);
        return new JSONObject(jsonText);
    }

    private void buildResponseError(ServletResponse response, String message) throws IOException {
        Response<String> errorResponse = new Response<>();
        errorResponse.getErrors().add(message);

        byte[] responseToSend = restResponseBytes(errorResponse);
        ((HttpServletResponse) response).setHeader("Content-Type", "application/json");
        ((HttpServletResponse) response).setStatus(401);
        response.getOutputStream().write(responseToSend);
    }

    private byte[] restResponseBytes(Response<String> errorResponse) throws IOException {
        String serialized = new ObjectMapper().writeValueAsString(errorResponse);
        return serialized.getBytes();
    }

    @Override
    public void destroy() {
        log.warn("Destructing filter :{}", this);
    }
}
