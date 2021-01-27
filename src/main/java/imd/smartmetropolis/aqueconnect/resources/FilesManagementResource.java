package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.services.FileService;
import imd.smartmetropolis.aqueconnect.services.TaskStatusService;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_DONE;
import static imd.smartmetropolis.aqueconnect.services.implementations.TaskStatusServiceImpl.STATUS_ERROR;

@RestController
@Log4j2
@CrossOrigin(origins = "*")
public class FilesManagementResource {
    private static final String UPLOAD_TOPIC = "status-task-upload-process";

    @Autowired
    @Qualifier("hdfsServiceImpl")
    private FileService fileService;

    @Autowired
    private TaskStatusService taskStatusService;

    @GetMapping(value = "/directory/{userId}")
    public ResponseEntity<List<Map<String, Object>>> listDirectoryHDFS(
            @PathVariable String userId, @RequestParam(required = false) String path
    ) {
        try {
            List<Map<String, Object>> response = fileService.listDirectory(userId, path);
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
    public ResponseEntity<Map<String, String>> createDirectoryHDFS(
            @PathVariable String userId, @RequestParam(required = false) String path
    ) {
        Map<String, String> response = new HashMap<>();
        try {
            if (path == null || path.equals("")) {
                response.put("message", "Path not informed for directory creation.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean created = fileService.createDirectory(userId, path);

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
    public ResponseEntity<Map<String, String>> renameDirectoryOrFile(
            @PathVariable String userId, @RequestParam String oldName, @RequestParam String newName
    ) {
        Map<String, String> response = new HashMap<>();
        try {
            if ((oldName == null || oldName.equals("")) || (newName == null || newName.equals(""))) {
                response.put("message", "newName and oldName must be informed.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean renamed = fileService.renameDirectoryOrFile(userId, oldName, newName);

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
    public ResponseEntity<Map<String, String>> removeDirectoryOrFile(
            @PathVariable String userId, @RequestParam(required = false) String path
    ) {
        Map<String, String> response = new HashMap<>();
        try {

            if (path == null || path.equals("")) {
                response.put("message", "Path not informed for directory/file removal.");
                log.error(response.get("message"));
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean removed = fileService.removeDirectoryOrFile(userId, path);

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

    @GetMapping(value = "/get-file-resource/{userId}")
    public ResponseEntity<InputStreamResource> getFileResource(
            @PathVariable String userId, @RequestParam(required = false) String path, HttpServletRequest request
    ) {
        try {
            InputStreamResource resource = fileService.getFileResource(userId, path);
            if (resource == null) {
                log.error("File not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            String[] pathVector = path.split("/");
            String fileName = pathVector[pathVector.length - 1];
            Tika tika = new Tika();
            String contentType = tika.detect(fileName);
            if (contentType == null) {
                contentType = "application/octet-stream";
            }

            log.info("File retrieve successfully");
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType(contentType))
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                    .body(resource);
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping(value = {"/file/{userId}/", "/file/{userId}/{taskId}"}, consumes = "multipart/form-data")
    public ResponseEntity<Map<String, Object>> writeFileByUploadHDFS(
            @RequestHeader Map<String, String> headers,
            @PathVariable String userId,
            @PathVariable(required = false) String taskId,
            @RequestParam(required = false) String path,
            @RequestParam(required = false, defaultValue = "false") boolean isImage,
            @RequestParam("file") MultipartFile file
    ) throws IOException {
        Map<String, Object> response = new HashMap<>();
        if (path == null || path.equals("")) {
            response.put("message", "Path not informed to create file.");
            log.error(response.get("message"));
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
        if (!file.isEmpty()) {
            try {
                if (!isImage) fileService.writeFileInputStream(userId, path, file.getInputStream());
                else fileService.writeImage(userId, path, file.getInputStream());
            } catch (Exception e) {
                response.put("message", "Error to upload file");
                log.error(response.get("message"));
                this.taskStatusService.sendTaskStatusProgress(
                        headers, taskId, STATUS_ERROR, String.valueOf(response.get("message")), UPLOAD_TOPIC
                );
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }
            response.put("message", path + " was created.");
            log.info(response.get("message"));
            this.taskStatusService.sendTaskStatusProgress(
                    headers, taskId, STATUS_DONE, String.valueOf(response.get("message")), UPLOAD_TOPIC
            );
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        }
        response.put("message", "File is empty");
        log.error(response.get("message"));
        this.taskStatusService.sendTaskStatusProgress(
                headers, taskId, STATUS_ERROR, String.valueOf(response.get("message")), UPLOAD_TOPIC
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public ResponseEntity<Map<String, String>> writeFileByTextHDFS(
            @PathVariable String userId,
            @PathVariable String importSetupName,
            @PathVariable String fileName,
            @RequestBody String data
    ) {
        Map<String, String> response = new HashMap<>();
        try {
            String path = importSetupName + "/" + fileName;
            fileService.writeFileString(userId, path, data);
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
    public ResponseEntity<Long> lineCountFile(
            @PathVariable String userId, @RequestParam(required = false) String path
    ) {
        try {
            long count = fileService.lineCount(userId, path);
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