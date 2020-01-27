package imd.smartmetropolis.aqueconnect.processors;

import com.google.gson.Gson;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * RelationshipProcessor
 */
@SuppressWarnings("ALL")
public class RelationshipProcessor {
    private List<ConcurrentHashMap<String, Object>> dataset1;
    private List<ConcurrentHashMap<String, Object>> dataset2;

    public RelationshipProcessor(
            List<ConcurrentHashMap<String, Object>> dataset1,
            List<ConcurrentHashMap<String, Object>> dataset2
    ) {
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
    }

    public RelationshipProcessor() {
    }

    public RelationshipProcessor makeRelationship() {
        makeRelationshipUsingIndexProcessor(getIdentityFields(dataset1.get(0)));
        return this;
    }

    public void confirmRelationship(String filePath1, String filePath2) {
        HandleHDFSImpl.getInstance().writeFile(null, filePath1, new Gson().toJson(dataset1));
        HandleHDFSImpl.getInstance().writeFile(null, filePath2, new Gson().toJson(dataset2));
    }

    @SuppressWarnings("unchecked")
    public List<String> getIdentityFields(Object data) {
        List<String> identityFields = new ArrayList<>();
        Map<String, Object> object = (Map<String, Object>) data;
        for (Map.Entry<String, Object> objectEntry : object.entrySet()) {
            if (objectEntry.getValue() instanceof Map) {
                Map<String, Object> objectMap = (Map<String, Object>) objectEntry.getValue();
                if (objectMap.containsKey("relationshipConfig")) {
                    String identifyField = (String) ((Map<String, Object>) objectMap.get("relationshipConfig")).get("identityField");
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
    public void makeRelationshipUsingIndexProcessor(List<String> identityFields) {
        if (identityFields.size() > 0) {
            for (Iterator<String> iterator = identityFields.iterator(); iterator.hasNext(); ) {
                String identityField = iterator.next();
                Map<Object, Integer> indexes = getIndexes(identityField, dataset2);
                for (Iterator<ConcurrentHashMap<String, Object>> mapIterator = dataset1.iterator(); mapIterator.hasNext(); ) {
                    ConcurrentHashMap<String, Object> data = mapIterator.next();
                    for (Iterator<Map.Entry<String, Object>> iteratorMapProps = data.entrySet().iterator(); iteratorMapProps.hasNext(); ) {
                        Map.Entry<String, Object> objectEntry = iteratorMapProps.next();
                        if (objectEntry.getValue() instanceof Map && mapIterator.hasNext()) {
                            boolean isMakeRelationship = this.makeRelationshipUsingIndexProcessor(
                                    data,
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
    private boolean makeRelationshipUsingIndexProcessor(
            ConcurrentHashMap<String, Object> data,
            String identifyField,
            Map<Object, Integer> indexes,
            String key,
            Map<String, Object> value
    ) {
        if (value.get("type").equals("Relationship") && value.containsKey("relationshipConfig")) {
            String relationshipConfigIdentifyField = (String) ((Map<String, Object>) value.get("relationshipConfig")).get("identityField");
            if (relationshipConfigIdentifyField.equals(identifyField)) {
                Object objectValue = value.get("object");
                if (objectValue instanceof List) {
                    List<Object> objects = (List<Object>) objectValue;
                    List<String> idsFK = objects.stream()
                            .filter(elem -> indexes.containsKey(elem))
                            .map(elem -> {
                                ConcurrentHashMap<String, Object> dataset2Element = dataset2.get(indexes.get(elem));
                                return (String) dataset2Element.get("id");
                            })
                            .collect(Collectors.toList());
                    Map<String, Object> relationship = new HashMap<>();
                    relationship.put("type", "Relationship");
                    relationship.put("object", idsFK);
                    data.put(key, relationship);
                    return true;
                } else {
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
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void cleanDatasets(List<String> datasetPaths) {
        for (String filePath : datasetPaths) {
            List<ConcurrentHashMap<String, Object>> dataSet = new RelationshipProcessor()
                    .removeRelationshipConfigAndTransientFieldsFromCollection(
                            HandleHDFSImpl.getInstance().readFile(null, filePath)
                    );
            HandleHDFSImpl.getInstance().writeFile(null, filePath, new Gson().toJson(dataSet));
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
                        valueMap.remove("relationshipConfig");

                    if (valueMap.containsKey("object") && !valueMap.get("object").toString().contains("urn:ngsi-ld"))
                        valueMap.put("object", null);

                    if (valueMap instanceof Map && (valueMap.get("type").equals("Transient") || valueMap.get("type").equals("Primary")))
                        data.remove(objectEntry.getKey());
                }
            }
        }
        return dataset;
    }

    public List<ConcurrentHashMap<String, Object>> getDataset1() {
        return dataset1;
    }

    public void setDataset1(List<ConcurrentHashMap<String, Object>> dataset1) {
        this.dataset1 = dataset1;
    }

    public List<ConcurrentHashMap<String, Object>> getDataset2() {
        return dataset2;
    }

    public void setDataset2(List<ConcurrentHashMap<String, Object>> dataset2) {
        this.dataset2 = dataset2;
    }
}