package imd.smartmetropolis.aqueconnect.resources;

import javax.ws.rs.core.Response;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFiles;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;

@RestController
@RequestMapping("/aqueconnect")
@CrossOrigin(origins = "*")
public class HdfsResource {

    @PostMapping(value = "/hdfs-data-file/{userId}/{importSetupName}/{fileName}")
    public Response writeFileHDFS(
        @PathVariable String userId,
        @PathVariable String importSetupName,
        @PathVariable String fileName, 
        @RequestBody String data
    ) {
        try {
            HandleHDFSFiles filesProcessor = new HandleHDFSFilesImpl();
            filesProcessor.writeFile(userId, importSetupName, fileName, data);
        } catch (Exception e) {
            return Response.status(500).entity(e.getMessage()).build();
        }

        return Response.status(201).entity(data).build();
    }
}