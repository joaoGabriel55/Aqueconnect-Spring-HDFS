package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin(origins = "*")
public class HDFSResource {


    @GetMapping(value = "/directory/{userId}")
    public ResponseEntity<List<Map<String, Object>>> listDirectoryHDFS(@PathVariable String userId, @RequestParam(required = false) String path) {
        try {
            List<Map<String, Object>> response = HandleHDFSImpl.getInstance().listDirectory(userId, path);
            if (response != null)
                return ResponseEntity.status(HttpStatus.OK).body(response);

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
        }
    }

    @PostMapping(value = "/directory/{userId}")
    public ResponseEntity<Map<String, String>> createDirectoryHDFS(@PathVariable String userId, @RequestParam(required = false) String path) {
        Map<String, String> response = new HashMap<>();
        try {
            if (path == null || path == "") {
                response.put("message", "Path not informed for directory creation.");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean created = HandleHDFSImpl.getInstance().createDirectory(userId, path);

            if (created) {
                response.put("message", "Directory created.");
                return ResponseEntity.status(HttpStatus.CREATED).body(response);
            }
            response.put("message", "Directory creation failed.");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
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
            if ((oldName == null || oldName == "") || (newName == null || newName == "")) {
                response.put("message", "newName and oldName must be informed.");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(userId, oldName, newName);

            if (renamed) {
                response.put("message", "Directory/file renamed.");
                return ResponseEntity.status(HttpStatus.OK).body(response);
            }

            response.put("message", "Directory/file rename failed.");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @DeleteMapping(value = "/directory-or-file/{userId}")
    public ResponseEntity<Map<String, String>> removeDirectoryOrFile(@PathVariable String userId, @RequestParam(required = false) String path) {
        Map<String, String> response = new HashMap<>();
        try {

            if (path == null || path == "") {
                response.put("message", "Path not informed for directory/file removal.");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile(userId, path);

            if (removed) {
                response.put("message", "Directory/file removed.");
                return ResponseEntity.status(HttpStatus.OK).body(response);
            }

            response.put("message", "Directory/file removal failed.");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        } catch (Exception e) {
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    @PostMapping(value = "/file/{userId}", consumes = "multipart/form-data")
    public ResponseEntity<Map<String, String>> writeFileByUploadHDFS(@PathVariable String userId,
                                                                     @RequestParam(required = false) String path,
                                                                     @RequestParam("file") MultipartFile file
    ) {
        Map<String, String> response = new HashMap<>();
        if (!file.isEmpty()) {
            try {
                if (path == null || path == "") {
                    response.put("message", "Path not informed to create file.");
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
                }
                HandleHDFSImpl.getInstance().writeFileInputStream(userId, path, file.getInputStream());

            } catch (Exception e) {
                response.put("message", e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }
            response.put("message", path + " was created.");
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        }
        response.put("message", "File is empty");
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
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
        response.put("message", fileName + " was created.");
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}