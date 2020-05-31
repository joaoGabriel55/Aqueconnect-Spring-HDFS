package imd.smartmetropolis.aqueconnect.security.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.security.PermissionChecker;
import imd.smartmetropolis.aqueconnect.utils.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.AUTH;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.SGEOL_INSTANCE;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.USER_TOKEN;

@Component
@Order(1)
public class TransactionFilter implements Filter {

    private final static Logger LOG = LoggerFactory.getLogger(TransactionFilter.class);

    @Override
    public void init(final FilterConfig filterConfig) {
        LOG.info("Initializing filter :{}", this);
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException {
        PermissionChecker permissionChecker = new PermissionChecker();

        HttpServletRequest req = (HttpServletRequest) request;
        LOG.info("Starting Transaction for req :{}", req.getRequestURI());
        try {
            if (AUTH) {
                boolean hasPermission = permissionChecker.checkSmartSyncPermissionAccess(
                        req.getHeader(SGEOL_INSTANCE), req.getHeader(USER_TOKEN), (HttpServletRequest) request
                );
                if (!hasPermission) {
                    buildResponseError(response, "You don't have permission to access Aqueconnect API");
                    return;
                }
            }
            chain.doFilter(request, response);
        } catch (Exception e) {
            buildResponseError(response, "Internal error.");
            return;
        }
        LOG.info("Committing Transaction for req :{}", req.getRequestURI());
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
        LOG.warn("Destructing filter :{}", this);
    }
}
