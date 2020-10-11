package imd.smartmetropolis.aqueconnect.services.implementations;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.config.AMQPConfig;
import imd.smartmetropolis.aqueconnect.services.TaskStatusService;
import lombok.extern.log4j.Log4j2;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@Log4j2
public class TaskStatusServiceImpl implements TaskStatusService {

    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_DONE = "DONE";
    public static final String STATUS_ERROR = "ERROR";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void sendTaskStatusProgress(
            Map<String, String> headers, String taskId, String status, String description, String topic
    ) throws IOException {
        if (taskId != null) {
            try {
                Map<String, Object> task = new LinkedHashMap<>();
                task.put("id", taskId);
                task.put("description", description);
                task.put("status", status);
                task.put("topic", topic);

                String taskJson = new ObjectMapper().writeValueAsString(task);
                rabbitTemplate.convertAndSend(AMQPConfig.EXCHANGE_NAME, "", taskJson);
                log.info("sendTaskStatusProgress: task - {}", taskJson);
            } catch (IOException e) {
                e.printStackTrace();
                log.error(e.getMessage() + " {}", e.getStackTrace());
                throw new IOException();
            }
        }
    }
}
