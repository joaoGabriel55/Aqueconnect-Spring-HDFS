package imd.smartmetropolis.aqueconnect.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.utils.RequestsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.*;

@Component
public class TaskStatusService {

    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_DONE = "DONE";
    public static final String STATUS_ERROR = "ERROR";

    @Autowired
    private ObjectMapper mapper;

    public void sendTaskStatusProgress(String sgeolInstance,
                                       String appToken,
                                       String userToken,
                                       String taskId,
                                       String status,
                                       String description,
                                       String topic
    ) {
        if (taskId != null) {
            try {
                Map<String, String> headers = new LinkedHashMap<>();
                headers.put(SGEOL_INSTANCE, sgeolInstance);
                headers.put(APP_TOKEN, appToken);
                headers.put(USER_TOKEN, userToken);

                Map<String, Object> task = new LinkedHashMap<>();
                task.put("id", taskId);
                task.put("description", description);
                task.put("status", status);

                String uri = BASE_AQUEDUCTE_URL + "task/topic/" + topic + "/" + taskId;
                RequestsUtil.execute(RequestsUtil.httpPost(uri, mapper.writeValueAsString(task), headers));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
