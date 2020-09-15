package imd.smartmetropolis.aqueconnect.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.utils.RequestsUtil;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;

@Component
@Log4j2
public class TaskStatusService {

    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_DONE = "DONE";
    public static final String STATUS_ERROR = "ERROR";

    @Autowired
    private ObjectMapper mapper;

    public void sendTaskStatusProgress(Map<String, String> headers,
                                       String taskId,
                                       String status,
                                       String description,
                                       String topic
    ) throws IOException {
        if (taskId != null) {
            try {
                Map<String, Object> task = new LinkedHashMap<>();
                task.put("id", taskId);
                task.put("description", description);
                task.put("status", status);

                String uri = BASE_AQUEDUCTE_URL + "task/topic/" + topic + "/" + taskId;
                RequestsUtil.execute(RequestsUtil.httpPost(uri, mapper.writeValueAsString(task), headers));
                log.info("sendTaskStatusProgress: task - {}", task.toString());
            } catch (IOException e) {
                e.printStackTrace();
                log.error(e.getMessage() + " {}", e.getStackTrace());
                throw new IOException();
            }
        }
    }
}
