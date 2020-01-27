package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFS;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: UserID into Request Header
@RestController
@CrossOrigin(origins = "*")
public class HDFSResource {


    @GetMapping(value = "/directory/{userId}")
    public ResponseEntity<List<Map<String, Object>>> getDirectoryHDFS(@PathVariable String userId, @RequestParam(required = false) String path) {
        try {
            List<Map<String, Object>> response = HandleHDFSImpl.getInstance().listDirectory(userId, path);
            if (response != null)
                return ResponseEntity.status(HttpStatus.OK).body(response);

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ArrayList<>());
        }
    }


    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public ResponseEntity<String> writeFileHDFS(@PathVariable String userId,
                                                @PathVariable String importSetupName,
                                                @PathVariable String fileName,
                                                @RequestBody String data
    ) {
        try {
            HandleHDFS filesProcessor = new HandleHDFSImpl();
            String path = importSetupName + "/" + fileName;
            filesProcessor.writeFile(userId, path, data);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(fileName);
    }
}