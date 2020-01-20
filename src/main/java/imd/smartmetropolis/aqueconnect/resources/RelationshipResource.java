package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.dtos.DataSetRelationship;
import imd.smartmetropolis.aqueconnect.processors.RelationshipProcessor;
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
        RelationshipProcessor processor = new RelationshipProcessor();

        try {
            processor.makeRelationship(dataSetRelationship.getRelationshipMap());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.OK).build();
    }

}