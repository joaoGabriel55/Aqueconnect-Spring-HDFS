package imd.smartmetropolis.aqueconnect.resources;

import imd.smartmetropolis.aqueconnect.dtos.DataSetRelationship;
import imd.smartmetropolis.aqueconnect.processors.RelationshipProcessor;
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
            return ResponseEntity.status(500).body(e.getMessage());
        }

        return ResponseEntity.ok("Okay");
    }

}