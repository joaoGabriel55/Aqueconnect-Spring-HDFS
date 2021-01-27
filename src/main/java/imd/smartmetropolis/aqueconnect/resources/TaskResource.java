package imd.smartmetropolis.aqueconnect.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.services.TaskStatusService;
import imd.smartmetropolis.aqueconnect.utils.RequestsUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_DONE;

@RestController
@Log4j2
@RequestMapping("/task")
public class TaskResource {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private TaskStatusService taskStatusService;

    @PostMapping
    public ResponseEntity<Void> sendTaskStatusHttpProtocol(
            @RequestHeader Map<String, String> headers,
            @RequestBody Map<String, Object> task
    ) {
        try {
            Long sendTime = Calendar.getInstance().getTimeInMillis();
            task.put("time", Double.parseDouble(sendTime.toString()));

            String uri = BASE_AQUEDUCTE_URL + "task/topic/top/" + task.get("id");

            HttpResponse response = RequestsUtil.execute(
                    RequestsUtil.httpPost(uri, mapper.writeValueAsString(task), headers)
            );
            if (response.getStatusLine().getStatusCode() != org.apache.http.HttpStatus.SC_OK) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            return ResponseEntity.status(HttpStatus.OK).build();
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }


    @PostMapping("/amqp")
    public ResponseEntity<Void> sendTaskStatusAmqpProtocol(
            @RequestHeader Map<String, String> headers,
            @RequestBody Map<String, Object> task
    ) {
        try {
            taskStatusService.sendTaskStatusProgress(
                    headers,
                    task.get("id").toString(),
                    STATUS_DONE,
                    task.get("description").toString(),
                    "topic"
            );
            return ResponseEntity.status(HttpStatus.OK).build();
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }


}
