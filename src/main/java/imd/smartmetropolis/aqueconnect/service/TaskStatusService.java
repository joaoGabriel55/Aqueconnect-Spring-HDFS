package imd.smartmetropolis.aqueconnect.service;

import imd.smartmetropolis.aqueconnect.utils.RequestsUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.*;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtils.httpPost;

@Component
public class TaskStatusService {

    // Topics
    public static final String UPLOAD_TOPIC = "status-task-upload-process";
    public static final String IMPORT_DATA_TOPIC = "status-task-import-process";

    public static final String PROCESSING = "PROCESSING";
    public static final String DONE = "DONE";
    public static final String ERROR = "ERROR";

    public Map<String, Object> sendTaskStatusProgress(
            String appToken,
            String userToken,
            String taskId,
            String status,
            String description,
            String topicName
            ) {
        Map<String, Object> taskMap = null;
        if (taskId != null) {
            taskMap = new LinkedHashMap<>();
            taskMap.put("status", status);
            taskMap.put("description", description);
            try {
                Map<String, String> headers = new LinkedHashMap<>();
                headers.put(APP_TOKEN, appToken);
                headers.put(USER_TOKEN, userToken);
                String url = BASE_AQUEDUCTE_URL + "task/topic/" + topicName + "/" + taskId;
                RequestsUtils.execute(httpPost(url, taskMap, headers));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return taskMap;
    }

}
