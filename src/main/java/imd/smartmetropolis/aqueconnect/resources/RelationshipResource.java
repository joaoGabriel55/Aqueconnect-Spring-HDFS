package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.dtos.DataSetRelationship;
import imd.smartmetropolis.aqueconnect.dtos.RelationshipMap;
import imd.smartmetropolis.aqueconnect.processors.RelationshipProcessor;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * RelationshipResource
 */
@RestController
@CrossOrigin(origins = "*")
public class RelationshipResource {

    @PostMapping(value = "/relationships")
    public ResponseEntity<String> makeRelationships(@RequestBody DataSetRelationship dataSetRelationship) {


        try {
            for (RelationshipMap relationshipMap : dataSetRelationship.getRelationshipMap()) {
                new RelationshipProcessor(
                        HandleHDFSFilesImpl.getInstance().readFile(relationshipMap.getFilePathOne()),
                        HandleHDFSFilesImpl.getInstance().readFile(relationshipMap.getFilePathTwo()))
                        .makeRelationship()
                        .confirmRelationship(relationshipMap.getFilePathOne())
                        .confirmRelationship(relationshipMap.getFilePathTwo());
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.OK).build();
    }

}