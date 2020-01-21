package imd.smartmetropolis.aqueconnect;

import imd.smartmetropolis.aqueconnect.processors.RelationshipProcessor;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TestRelationship {

    public static void main(String[] args) {
        try {
            String URL_PATH = "/home/quaresma/git/Aqueconnect-Spring-HDFS/src/test/java/imd/smartmetropolis/aqueconnect/datatest/files/output/";

            String obj = new BufferedReader(new FileReader(URL_PATH + "alunos.json")).lines().collect(Collectors.joining());
            String obj2 = new BufferedReader(new FileReader(URL_PATH + "escolas.json")).lines().collect(Collectors.joining());

            List<ConcurrentHashMap<String, Object>> dataSet1 = new JSONArray(obj).toList().stream().map(elem -> new ConcurrentHashMap<>(((Map<String, Object>) elem))).collect(Collectors.toList());
            List<ConcurrentHashMap<String, Object>> dataSet2 = new JSONArray(obj2).toList().stream().map(elem -> new ConcurrentHashMap<>(((Map<String, Object>) elem))).collect(Collectors.toList());
            RelationshipProcessor processor = new RelationshipProcessor(dataSet1, dataSet2);
            processor.makeRelationship();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
