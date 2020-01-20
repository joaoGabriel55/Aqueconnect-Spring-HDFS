package imd.smartmetropolis.aqueconnect.processors;

import com.google.gson.Gson;
import imd.smartmetropolis.aqueconnect.dtos.RelationshipMap;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RelationshipProcessor
 */
@SuppressWarnings("ALL")
public class RelationshipProcessor {

    public void makeRelationship(List<RelationshipMap> relationshipMapList) {
        for (RelationshipMap relationshipMap : relationshipMapList) {
            List<Map<String, Object>> dataset1 = loadDataSets(relationshipMap.getFilePathOne());
            List<Map<String, Object>> dataset2 = loadDataSets(relationshipMap.getFilePathTwo());

            makeRelationshipUsingIndexProcessor(dataset1, dataset2, getIdentityFields(dataset1.get(0)));

            removeRelationshipConfigAndTransientFieldsFromCollection(dataset1);
            removeRelationshipConfigAndTransientFieldsFromCollection(dataset2);

            HandleHDFSFilesImpl.getInstance().writeFile(relationshipMap.getFilePathOne(), new Gson().toJson(dataset1));
            HandleHDFSFilesImpl.getInstance().writeFile(relationshipMap.getFilePathTwo(), new Gson().toJson(dataset2));

        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getIdentityFields(Object data) {
        final List<String> identityFields = new ArrayList<>();
        final Map<String, Object> object = (Map<String, Object>) data;
        for (final Map.Entry<String, Object> objectEntry : object.entrySet()) {
            if (objectEntry.getValue() instanceof Map) {
                final Map<String, Object> objectMap = (Map<String, Object>) objectEntry.getValue();
                if (objectMap.containsKey("relationshipConfig")) {
                    final String identifyField = (String) ((Map<String, Object>) objectMap.get("relationshipConfig"))
                            .get("identityField");
                    identityFields.add(identifyField);
                }
            }
        }
        return identityFields;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Integer> getIndexes(String identifyField, List<Map<String, Object>> dataset2) {
        final Map<Object, Integer> indexes = new HashMap<>();
        for (final Object object : dataset2) {
            final Map<String, Object> objectMap = (Map<String, Object>) object;
            if (objectMap.containsKey(identifyField)) {
                final int index = dataset2.indexOf(objectMap);
                final Object value = ((Map<String, Object>) objectMap.get(identifyField)).get("value");
                indexes.put(value, index);
            }
        }
        return indexes;
    }

    @SuppressWarnings("unchecked")
    private void makeRelationshipUsingIndexProcessor(
            List<Map<String, Object>> dataset1,
            List<Map<String, Object>> dataset2,
            List<String> identityFields
    ) {
        if (identityFields.size() > 0) {
            for (final String identityField : identityFields) {
                final Map<Object, Integer> indexes = getIndexes(identityField, dataset2);
                for (Map<String, Object> data : dataset1) {
                    final Map<String, Object> dataCpy = data;
                    for (final Map.Entry<String, Object> objectEntry : dataCpy.entrySet()) {
                        if (objectEntry.getValue() instanceof Map) {
                            this.makeRelationshipUsingIndexProcessor(data, identityField, indexes, dataset2,
                                    objectEntry.getKey(), (Map<String, Object>) objectEntry.getValue());
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void makeRelationshipUsingIndexProcessor(
            Map<String, Object> data,
            String identifyField,
            Map<Object, Integer> indexes,
            List<Map<String, Object>> dataset2,
            String key,
            Map<String, Object> value
    ) {
        if (value.get("type").equals("Relationship") && value.containsKey("relationshipConfig")) {
            final String relationshipConfigIdentifyField = (String) ((Map<String, Object>) value
                    .get("relationshipConfig")).get("identityField");
            if (relationshipConfigIdentifyField.equals(identifyField)) {
                final Integer index = indexes.get(value.get("object"));
                if (index != null) {
                    final String idFK = (String) ((Map<String, Object>) dataset2.get(indexes.get(value.get("object"))))
                            .get("id");
                    final Map<String, Object> relationship = new HashMap<>();
                    relationship.put("type", "Relationship");
                    relationship.put("object", idFK);
                    data.put(key, relationship);

                    // TODO: Bidirectional

                    if (value.containsKey("relationshipConfig")) {
                        data.remove("relationshipConfig");
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void removeRelationshipConfigAndTransientFieldsFromCollection(List<Map<String, Object>> dataset1) {
        for (final Object data : dataset1) {
            final Map<String, Object> dataCpy = (Map<String, Object>) data;
            for (final Map.Entry<String, Object> objectEntry : dataCpy.entrySet()) {
                if (objectEntry.getValue() instanceof Map) {
                    final Map<String, Object> valueMap = (Map<String, Object>) objectEntry.getValue();
                    if (valueMap.containsKey("relationshipConfig"))
                        ((Map<String, Object>) data).remove("relationshipConfig");

                    if (valueMap instanceof Map && valueMap.get("type").equals("Transient"))
                        ((Map<String, Object>) data).remove(objectEntry.getKey());
                }
            }
        }
    }

    private List<Map<String, Object>> loadDataSets(String dataSetPath) {
        return HandleHDFSFilesImpl.getInstance().readFile(dataSetPath);
    }

}