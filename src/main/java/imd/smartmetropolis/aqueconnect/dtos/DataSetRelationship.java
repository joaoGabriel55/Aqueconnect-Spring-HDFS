package imd.smartmetropolis.aqueconnect.dtos;

import java.util.List;

/**
 * DataSetRelationship
 */
public class DataSetRelationship {

    List<String> dataSetsPaths;
    List<RelationshipMap> relationshipMap;

    public List<String> getDataSetsPaths() {
        return dataSetsPaths;
    }

    public void setDataSetsPaths(List<String> dataSetsPaths) {
        this.dataSetsPaths = dataSetsPaths;
    }

    public List<RelationshipMap> getRelationshipMap() {
        return relationshipMap;
    }

    public void setRelationshipMap(List<RelationshipMap> relationshipMap) {
        this.relationshipMap = relationshipMap;
    }
}