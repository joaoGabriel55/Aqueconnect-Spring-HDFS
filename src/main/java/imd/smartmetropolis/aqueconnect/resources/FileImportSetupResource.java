package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.processors.FileConverterToJSONProcessor;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import imd.smartmetropolis.aqueconnect.service.FileDataImportToSGEOLService;
import imd.smartmetropolis.aqueconnect.service.TaskStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.service.TaskStatusService.STATUS_DONE;
import static imd.smartmetropolis.aqueconnect.service.TaskStatusService.STATUS_ERROR;
import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.APP_TOKEN;
import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.USER_TOKEN;

/**
 * This Resource is responsible for provide services which allow
 * the another service, Aqueducte, import files data content (csv, [xls, ..., maybe])
 */
@RestController
@RequestMapping("/file-import-setup-resource")
public class FileImportSetupResource {

    @Autowired
    private FileDataImportToSGEOLService service;

    @Autowired
    private TaskStatusService taskStatusService;

    @GetMapping(value = "/file-fields/{userId}")
    public ResponseEntity<Map<String, Object>> getFileFields(@PathVariable String userId,
                                                             @RequestParam(required = false) String path,
                                                             @RequestParam String delimiter
    ) {
        Map<String, Object> response = new HashMap<>();
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is empty");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }

        try {
            String fileLine = HandleHDFSImpl.getInstance().readFileLines(1, userId, path);
            if (fileLine == null || fileLine.equals("")) {
                response.put("message", "File line is empty");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            response.put("fieldsMap",
                    service.getFieldsMap(Arrays.asList(fileLine.replace("\n", "").split(delimiter)))
            );
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    // TODO: Define a limit?
    @PostMapping(value = "/convert-to-json/{userId}")
    public ResponseEntity<Map<String, Object>> convertToJSON(@PathVariable String userId,
                                                             @RequestParam(required = false) String path,
                                                             @RequestParam String delimiter,
                                                             @RequestBody Map<String, Integer> fieldsSelected
    ) {
        Map<String, Object> response = new HashMap<>();
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is empty");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }

        try {
            String fileLine = HandleHDFSImpl.getInstance().readFileLines(5, userId, path).replace(delimiter, ",");
            if (fileLine == null || fileLine.equals("")) {
                response.put("message", "File line is empty");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
            List<Map<String, Object>> result = processor.jsonConverter(fileLine, fieldsSelected);
            response.put("data", result);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }


    // TODO: Create DTOs for import setup with context (Context source)
    @PostMapping(value = "/import-to-sgeol-by-aqueducte/{layer}/{userId}/{taskId}/{taskIndex}")
    public ResponseEntity<Map<String, Object>> importToSGEOLByAqueducte(@RequestHeader(APP_TOKEN) String appToken,
                                                                        @RequestHeader(USER_TOKEN) String userToken,
                                                                        @PathVariable String layer,
                                                                        @PathVariable String userId,
                                                                        @PathVariable(required = false) String taskId,
                                                                        @PathVariable(required = false) Integer taskIndex,
                                                                        @RequestParam(required = false) String path,
                                                                        @RequestParam String delimiter,
                                                                        @RequestBody FieldsSelectedConfig fieldsSelectedConfig
    ) {
        Map<String, Object> response = new HashMap<>();
        if (delimiter == null || delimiter.equals("")) {
            response.put("message", "Delimiter param is empty");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
        try {
            long linesCount = HandleHDFSImpl.getInstance().lineCount(userId, path);
            BufferedReader reader = HandleHDFSImpl.getInstance().openFileBuffer(userId, path);
            List<String> entitiesIDs = service.importFileDataNGSILDByAqueducte(
                    appToken,
                    userToken,
                    layer,
                    reader,
                    fieldsSelectedConfig,
                    delimiter,
                    linesCount
            );
            if (entitiesIDs == null) {
                response.put("message", "Error in importation");
                this.taskStatusService.sendTaskStatusProgress(response, taskId, taskIndex, STATUS_ERROR);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            response.put("entitiesImported", entitiesIDs);
            this.taskStatusService.sendTaskStatusProgress(response, taskId, taskIndex, STATUS_DONE);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (IOException e) {
            response.put("message", e.getMessage());
            this.taskStatusService.sendTaskStatusProgress(response, taskId, taskIndex, STATUS_ERROR);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }


}
