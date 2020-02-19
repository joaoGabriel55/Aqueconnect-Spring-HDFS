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

    public static final String IMPORT_DATA = "IMPORT_DATA";
    public static final String UPLOAD_FILE = "UPLOAD_FILE";
    public static final String RELATIONSHIP_ENTITIES = "RELATIONSHIP_ENTITIES";

    public static final String PROCESSING = "PROCESSING";
    public static final String DONE = "DONE";
    public static final String ERROR = "ERROR";

    public Map<String, Object> sendTaskStatusProgress(
            String topicName,
            String appToken,
            String userToken,
            Integer id,
            String userId,
            Integer index,
            String title,
            String type,
            String status,
            String description
    ) {
        Map<String, Object> taskMap = null;
        if (id != null && userId != null) {
            taskMap = new LinkedHashMap<>();
            taskMap.put("id", id);
            taskMap.put("userId", userId);
            taskMap.put("index", index);
            taskMap.put("title", title);
            taskMap.put("type", type);
            taskMap.put("status", status);
            taskMap.put("description", description);
            try {
                Map<String, String> headers = new LinkedHashMap<>();
                headers.put(APP_TOKEN, appToken);
                headers.put(USER_TOKEN, userToken);
                String url = BASE_AQUEDUCTE_URL + "task/topic/" + topicName;
                RequestsUtils.execute(httpPost(url, taskMap, headers));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return taskMap;
    }

}
