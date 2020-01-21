package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFiles;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "*")
public class HDFSResource {

    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public ResponseEntity<String> writeFileHDFS(@PathVariable String userId,
                                                @PathVariable String importSetupName,
                                                @PathVariable String fileName,
                                                @RequestBody String data
    ) {
        try {
            HandleHDFSFiles filesProcessor = new HandleHDFSFilesImpl();
            String path = userId + "/" + importSetupName + "/" + fileName;
            filesProcessor.writeFile("/user/data/" + path, data);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(fileName);
    }
}