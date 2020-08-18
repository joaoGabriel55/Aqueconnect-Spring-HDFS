package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import imd.smartmetropolis.aqueconnect.services.TaskStatusService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.*;

import static imd.smartmetropolis.aqueconnect.services.HDFSService.isValidFormat;
import static imd.smartmetropolis.aqueconnect.services.TaskStatusService.STATUS_DONE;
import static imd.smartmetropolis.aqueconnect.services.TaskStatusService.STATUS_ERROR;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.*;

@RestController
@Log4j2
@CrossOrigin(origins = "*")
public class HDFSResource {
    private static final String UPLOAD_TOPIC = "status-task-upload-process";

    @Autowired
    private TaskStatusService taskStatusService;

    @GetMapping(value = "/directory/{userId}")
    public ResponseEntity<List<Map<String, Object>>> listDirectoryHDFS(@PathVariable String userId,
                                                                       @RequestParam(required = false) String path) {
        try {
            List<Map<String, Object>> response = HandleHDFSImpl.getInstance().listDirectory(userId, path);
            if (response == null) {
                log.error("Empty directory");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
            }
            log.info(path);
            return ResponseEntity.status(HttpStatus.OK).body(response);

        } catch (Exception e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
        }
    }

    @PostMapping(value = "/directory/{userId}")
    public ResponseEntity<Map<String, String>> createDirectoryHDFS(@PathVariable String userId,
                                                                   @RequestParam(required = false) String path) {
        Map<String, String> response = new HashMap<>();
        try {
            if (path == null || path.equals("")) {
                response.put("message", "Path not informed for directory creation.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean created = HandleHDFSImpl.getInstance().createDirectory(userId, path);

            if (!created) {
                response.put("message", "Directory creation failed.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            response.put("message", "Directory created.");
            log.info(response.get("message"));
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            log.error(e.getMessage() + " {}", e.getStackTrace());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @PutMapping(value = "/directory-or-file/{userId}")
    public ResponseEntity<Map<String, String>> renameDirectoryOrFile(@PathVariable String userId,
                                                                     @RequestParam String oldName,
                                                                     @RequestParam String newName
    ) {
        Map<String, String> response = new HashMap<>();
        try {
            if ((oldName == null || oldName.equals("")) || (newName == null || newName.equals(""))) {
                response.put("message", "newName and oldName must be informed.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(userId, oldName, newName);

            if (!renamed) {
                response.put("message", "Directory/file rename failed.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            response.put("message", "Directory/file renamed.");
            log.info(response.get("message"));
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            log.error(e.getMessage() + " {}", e.getStackTrace());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @DeleteMapping(value = "/directory-or-file/{userId}")
    public ResponseEntity<Map<String, String>> removeDirectoryOrFile(@PathVariable String userId,
                                                                     @RequestParam(required = false) String path) {
        Map<String, String> response = new HashMap<>();
        try {

            if (path == null || path.equals("")) {
                response.put("message", "Path not informed for directory/file removal.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile(userId, path);

            if (!removed) {
                response.put("message", "Directory/file removal failed.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            response.put("message", "Directory/file removed.");
            log.info(response.get("message"));
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            log.error(e.getMessage() + " {}", e.getStackTrace());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @PostMapping(value = {"/file/{userId}/", "/file/{userId}/{taskId}"}, consumes = "multipart/form-data")
    public ResponseEntity<Map<String, Object>> writeFileByUploadHDFS(
            @RequestHeader(SGEOL_INSTANCE) String sgeolInstance,
            @RequestHeader(APP_TOKEN) String appToken,
            @RequestHeader(USER_TOKEN) String userToken,
            @PathVariable String userId,
            @PathVariable(required = false) String taskId,
            @RequestParam(required = false) String path,
            @RequestParam("file") MultipartFile file
    ) throws IOException {
        Map<String, Object> response = new HashMap<>();
        if (path == null || path.equals("")) {
            response.put("message", "Path not informed to create file.");
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
        if (!file.isEmpty()) {
            if (!isValidFormat(Objects.requireNonNull(file.getContentType()))) {
                response.put("message", "File type is invalid");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
            try {
                HandleHDFSImpl.getInstance().writeFileInputStream(userId, path, file.getInputStream());
            } catch (Exception e) {
                response.put("message", "Error to upload file");
                log.error(response.get("message"));
                this.taskStatusService.sendTaskStatusProgress(sgeolInstance, appToken, userToken,
                        taskId, STATUS_ERROR, String.valueOf(response.get("message")), UPLOAD_TOPIC);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }  
            response.put("message", path + " was created.");
            log.info(response.get("message"));
            this.taskStatusService.sendTaskStatusProgress(sgeolInstance, appToken, userToken,
                    taskId, STATUS_DONE, String.valueOf(response.get("message")), UPLOAD_TOPIC);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        }
        response.put("message", "File is empty");
        log.error(response.get("message"));
        this.taskStatusService.sendTaskStatusProgress(sgeolInstance, appToken, userToken,
                taskId, STATUS_ERROR, String.valueOf(response.get("message")), UPLOAD_TOPIC);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public ResponseEntity<Map<String, String>> writeFileHDFS(@PathVariable String userId,
                                                             @PathVariable String importSetupName,
                                                             @PathVariable String fileName,
                                                             @RequestBody String data
    ) {
        Map<String, String> response = new HashMap<>();
        try {
            String path = importSetupName + "/" + fileName;
            HandleHDFSImpl.getInstance().writeFileString(userId, path, data);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
        response.put("message", fileName + " was created.");
        log.info(response.get("message"));
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping(value = "/line-count-file/{userId}")
    public ResponseEntity<Long> lineCountFile(@PathVariable String userId,
                                              @RequestParam(required = false) String path) {
        try {
            long count = HandleHDFSImpl.getInstance().lineCount(userId, path);
            if (count == 0) {
                log.error(count + " Lines");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
            }
            log.info(count + " Lines");
            return ResponseEntity.status(HttpStatus.OK).body(count);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
    }

}