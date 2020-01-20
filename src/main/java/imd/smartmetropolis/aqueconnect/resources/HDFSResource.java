package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFiles;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.core.Response;

@RestController
@CrossOrigin(origins = "*")
public class HDFSResource {

    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public Response writeFileHDFS(@PathVariable String userId,
                                  @PathVariable String importSetupName,
                                  @PathVariable String fileName,
                                  @RequestBody String data
    ) {
        try {
            HandleHDFSFiles filesProcessor = new HandleHDFSFilesImpl();
            String path = userId + "/" + importSetupName + "/" + fileName;
            filesProcessor.writeFile(path, data);
        } catch (Exception e) {
            return Response.status(500).entity(e.getMessage()).build();
        }

        return Response.status(201).entity(data).build();
    }
}