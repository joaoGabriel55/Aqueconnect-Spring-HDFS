package imd.smartmetropolis.aqueconnect.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import imd.smartmetropolis.aqueconnect.AbstractTest;
import imd.smartmetropolis.aqueconnect.utils.RequestsUtil;
import org.junit.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.HASH_CONFIG_VALUE;
import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_DONE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TaskStatusServiceTest extends AbstractTest {

    @Autowired
    private TaskStatusService service;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ObjectMapper mapper;

    @Test
    public void sendTaskStatusProgressAmqpTest() throws IOException {
        assertDoesNotThrow(() ->
                service.sendTaskStatusProgress(
                        new HashMap<>(), "1234", STATUS_DONE, "desc", "topic"
                )
        );
    }

    @Test
    public void sendTaskStatusProgressHttpTest() throws JsonProcessingException {

        Map<String, Object> task = new LinkedHashMap<>();
        task.put("id", "123");
        task.put("description", "desc");
        task.put("status", STATUS_DONE);

        Long sendTime = Calendar.getInstance().getTimeInMillis();
        task.put("time", Double.parseDouble(sendTime.toString()));

        String uri = BASE_AQUEDUCTE_URL + "task/topic/" + "top" + "/" + task.get("id");

        Map<String, String> headers = new HashMap<>();
        headers.put("hash-config", HASH_CONFIG_VALUE);
        headers.put("user-token", "125ce79a-9e4f-407d-875e-e3453e800f20");

        assertDoesNotThrow(() ->
                RequestsUtil.execute(RequestsUtil.httpPost(uri, mapper.writeValueAsString(task), headers))
        );
    }

}
