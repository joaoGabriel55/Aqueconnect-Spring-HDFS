package imd.smartmetropolis.aqueconnect.processors;

import com.google.gson.Gson;
import imd.smartmetropolis.aqueconnect.dtos.RelationshipMap;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSFilesImpl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RelationshipProcessor
 */
@SuppressWarnings("ALL")
public class RelationshipProcessor {
    private List<ConcurrentHashMap<String, Object>> dataset1;
    private List<ConcurrentHashMap<String, Object>> dataset2;

    public void makeRelationship(List<RelationshipMap> relationshipMapList) {
        for (RelationshipMap relationshipMap : relationshipMapList) {
            dataset1 = loadDataSets(relationshipMap.getFilePathOne());
            dataset2 = loadDataSets(relationshipMap.getFilePathTwo());

            makeRelationshipUsingIndexProcessor(getIdentityFields(dataset1.get(0)));

            dataset1 = removeRelationshipConfigAndTransientFieldsFromCollection(dataset1);
            dataset2 = removeRelationshipConfigAndTransientFieldsFromCollection(dataset2);

            HandleHDFSFilesImpl.getInstance().writeFile(relationshipMap.getFilePathOne(), new Gson().toJson(dataset1));
            HandleHDFSFilesImpl.getInstance().writeFile(relationshipMap.getFilePathTwo(), new Gson().toJson(dataset2));
            dataset1 = null;
            dataset2 = null;
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getIdentityFields(Object data) {
        List<String> identityFields = new ArrayList<>();
        Map<String, Object> object = (Map<String, Object>) data;
        for (Map.Entry<String, Object> objectEntry : object.entrySet()) {
            if (objectEntry.getValue() instanceof Map) {
                Map<String, Object> objectMap = (Map<String, Object>) objectEntry.getValue();
                if (objectMap.containsKey("relationshipConfig")) {
                    String identifyField = (String) ((Map<String, Object>) objectMap.get("relationshipConfig"))
                            .get("identityField");
                    identityFields.add(identifyField);
                }
            }
        }
        return identityFields;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Integer> getIndexes(String identifyField, List<ConcurrentHashMap<String, Object>> dataset2) {
        Map<Object, Integer> indexes = new HashMap<>();
        for (Object object : dataset2) {
            Map<String, Object> objectMap = (Map<String, Object>) object;
            if (objectMap.containsKey(identifyField)) {
                int index = dataset2.indexOf(objectMap);
                Object value = ((Map<String, Object>) objectMap.get(identifyField)).get("value");
                indexes.put(value, index);
            }
        }
        return indexes;
    }

    @SuppressWarnings("unchecked")
    private void makeRelationshipUsingIndexProcessor(
            List<String> identityFields
    ) {
        if (identityFields.size() > 0) {
            for (Iterator<String> iterator = identityFields.iterator(); iterator.hasNext(); ) {
                String identityField = iterator.next();
                Map<Object, Integer> indexes = getIndexes(identityField, dataset2);
                for (Iterator<ConcurrentHashMap<String, Object>> mapIterator = dataset1.iterator(); mapIterator.hasNext(); ) {
                    ConcurrentHashMap<String, Object> data = mapIterator.next();
                    for (Iterator<Map.Entry<String, Object>> iteratorMapProps = data.entrySet().iterator(); iteratorMapProps.hasNext(); ) {
                        Map.Entry<String, Object> objectEntry = iteratorMapProps.next();
                        if (objectEntry.getValue() instanceof Map && mapIterator.hasNext()) {
                            this.makeRelationshipUsingIndexProcessor(
                                    dataset1.iterator().next(),
                                    identityField,
                                    indexes,
                                    objectEntry.getKey(),
                                    (Map<String, Object>) objectEntry.getValue()
                            );
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void makeRelationshipUsingIndexProcessor(
            ConcurrentHashMap<String, Object> data,
            String identifyField,
            Map<Object, Integer> indexes,
            String key,
            Map<String, Object> value
    ) {
        if (value.get("type").equals("Relationship") && value.containsKey("relationshipConfig")) {
            String relationshipConfigIdentifyField = (String) ((Map<String, Object>) value
                    .get("relationshipConfig")).get("identityField");
            if (relationshipConfigIdentifyField.equals(identifyField)) {
                Object objectValue = value.get("object");
                ConcurrentHashMap<String, Object> dataset2Element = indexes.containsKey(objectValue) ? dataset2.get(indexes.get(objectValue)) : null;
                if (dataset2Element != null) {
                    String idFK = (String) dataset2Element.get("id");
                    Map<String, Object> relationship = new HashMap<>();
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
    private List<ConcurrentHashMap<String, Object>> removeRelationshipConfigAndTransientFieldsFromCollection(List<ConcurrentHashMap<String, Object>> dataset) {
        for (Iterator<ConcurrentHashMap<String, Object>> iterator = dataset.iterator(); iterator.hasNext(); ) {
            ConcurrentHashMap<String, Object> data = (ConcurrentHashMap<String, Object>) iterator.next();
            for (Iterator<ConcurrentHashMap.Entry<String, Object>> iteratorMapProps = data.entrySet().iterator(); iteratorMapProps.hasNext(); ) {
                ConcurrentHashMap.Entry<String, Object> objectEntry = iteratorMapProps.next();
                if (objectEntry.getValue() instanceof Map) {
                    Map<String, Object> valueMap = (Map<String, Object>) objectEntry.getValue();
                    if (valueMap.containsKey("relationshipConfig"))
                        ((ConcurrentHashMap<String, Object>) dataset.iterator().next()).remove("relationshipConfig");

                    if (valueMap instanceof Map && valueMap.get("type").equals("Transient"))
                        ((ConcurrentHashMap<String, Object>) dataset.iterator().next()).remove(objectEntry.getKey());
                }
            }
        }
        return dataset;
    }

    private List<ConcurrentHashMap<String, Object>> loadDataSets(String dataSetPath) {
        return HandleHDFSFilesImpl.getInstance().readFile(dataSetPath);
    }

}