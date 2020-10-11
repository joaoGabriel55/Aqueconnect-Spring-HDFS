package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.AbstractTest;
import org.junit.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;

import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_DONE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TaskStatusServiceTest extends AbstractTest {

    @Autowired
    private TaskStatusService service;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    public void sendTaskStatusProgressTest() throws IOException {
        assertDoesNotThrow(() ->
                service.sendTaskStatusProgress(
                        new HashMap<>(), "1234", STATUS_DONE, "desc", "topic"
                )
        );
    }

}
