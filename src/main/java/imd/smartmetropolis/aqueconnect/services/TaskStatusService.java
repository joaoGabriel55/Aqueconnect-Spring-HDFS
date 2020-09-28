package imd.smartmetropolis.aqueconnect.services;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public interface TaskStatusService {
    void sendTaskStatusProgress(Map<String, String> headers, String taskId, String status, String description, String topic) throws IOException;
}
