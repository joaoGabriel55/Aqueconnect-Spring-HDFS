package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.services.DataFileImportService;
import imd.smartmetropolis.aqueconnect.services.FileService;
import imd.smartmetropolis.aqueconnect.services.TaskStatusService;
import imd.smartmetropolis.aqueconnect.services.implementations.FileJsonConverterServiceImpl;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_DONE;
import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_ERROR;


/**
 * This Resource is responsible for provide services which allow
 * the another service, Aqueducte, import files data content (csv, [xls, ..., maybe])
 */
@RestController
@Log4j2
@RequestMapping("/file-import-setup-resource")
public class DataFileImportResource {
    private static final String IMPORT_DATA_TOPIC = "status-task-import-process";

    @Autowired
    private DataFileImportService service;

    @Autowired
    @Qualifier("hdfsServiceImpl")
    private FileService fileService;

    @Autowired
    private TaskStatusService taskStatusService;

    @GetMapping(value = "/file-fields/{userId}")
    public ResponseEntity<Map<String, Object>> getFileFields(
            @PathVariable String userId, @RequestParam(required = false) String path, @RequestParam String delimiter
    ) {
        Map<String, Object> response = new HashMap<>();
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is empty");
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }

        try {
            String fileLine = fileService.readFileLines(1, userId, path);
            if (fileLine == null || fileLine.equals("")) {
                response.put("message", "File line is empty");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            response.put("fieldsMap",
                    service.getFileFieldsMap(Arrays.asList(fileLine.replace("\n", "").split(delimiter)))
            );
            log.info(response.get("fieldsMap"));
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e.getStackTrace());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    @PostMapping(value = "/convert-to-json/{userId}")
    public ResponseEntity<Map<String, Object>> convertToJSON(
            @PathVariable String userId,
            @RequestParam(required = false) String path,
            @RequestParam String delimiter,
            @RequestBody Map<String, Integer> fieldsSelected
    ) {
        Map<String, Object> response = new HashMap<>();
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is empty");
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }

        try {
            String fileLine = fileService.readFileLines(5, userId, path).replace(delimiter, ",");
            if (fileLine == null || fileLine.equals("")) {
                response.put("message", "File line is empty");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            FileJsonConverterServiceImpl processor = new FileJsonConverterServiceImpl();
            List<Map<String, Object>> result = processor.jsonConverter(fileLine, fieldsSelected);
            response.put("data", result);
            log.info(response.get("data"));
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @PostMapping(value = {
            "/data-import-by-aqueducte/{type}/{userId}",
            "/data-import-by-aqueducte/{type}/{userId}/{taskId}"
    })
    public ResponseEntity<Map<String, Object>> importDataByAqueducte(
            @RequestHeader Map<String, String> headers,
            @PathVariable String type,
            @PathVariable String userId,
            @PathVariable(required = false) String taskId,
            @RequestParam Map<String, String> allParams,
            @RequestBody FieldsSelectedConfig fieldsSelectedConfig
    ) throws Exception {
        Map<String, Object> response = new HashMap<>();

        String path = allParams.get("path");

        String delimiter = allParams.get("delimiter");
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is required");
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }

        try {
            long linesCount = fileService.lineCount(userId, path);
            BufferedReader reader = fileService.openFileBuffer(userId, path);

            service.importFileDataNGSILDByAqueducte(
                    headers, allParams, type, reader, fieldsSelectedConfig, delimiter, linesCount
            );

            response.put("message", "Dados importados para Layer: " + type);
            log.info(response.get("message"));
            this.taskStatusService.sendTaskStatusProgress(
                    headers, taskId, STATUS_DONE, String.valueOf(response.get("message")), IMPORT_DATA_TOPIC
            );
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (IOException e) {
            response.put("message", e.getMessage());
            log.error(response.get("message"));
            this.taskStatusService.sendTaskStatusProgress(
                    headers, taskId, STATUS_ERROR, String.valueOf(response.get("message")), IMPORT_DATA_TOPIC
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

}
