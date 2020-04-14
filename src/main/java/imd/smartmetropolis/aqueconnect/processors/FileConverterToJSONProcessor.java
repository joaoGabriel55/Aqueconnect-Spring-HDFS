package imd.smartmetropolis.aqueconnect.processors;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

public class FileConverterToJSONProcessor {

    public List<Map<String, Object>> jsonConverter(String contentFile, Map<String, Integer> fieldsSelected) {
        List<Map<String, Object>> listOfObjects = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new StringReader(contentFile))) {
            List<String[]> allData;
            allData = reader.readAll();
            Map<String, Object> csvToJson = new HashMap<>();
            listOfObjects = generateJson(csvToJson, allData, fieldsSelected);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listOfObjects;
    }

    private List<Map<String, Object>> generateJson(
            Map<String, Object> csvToJsonNSGILD,
            List<String[]> allData,
            Map<String, Integer> fieldsSelected
    ) {
        List<Map<String, Object>> listOfObjects = new ArrayList<>();
        List<String> rowPropertiesName = null;
        int index = 0;
        for (String[] row : allData) {
            if (index == 0) {
                rowPropertiesName = Arrays.asList(row)
                        .stream()
                        .filter(key -> (key != null || !"".equalsIgnoreCase(key)) && fieldsSelected.containsKey(key))
                        .collect(Collectors.toList());
            } else {
                int indexData = 0;
                for (Object cell : row) {
                    if (cell == "" || cell.toString().length() == 0)
                        cell = null;

                    for (Map.Entry<String, Integer> entry : fieldsSelected.entrySet()) {
                        if (entry.getValue() == indexData) {
                            String key = entry.getKey()
                                    .toLowerCase()
                                    .replaceAll("\\s+", "")
                                    .trim();
                            Object cellNumber = asNumber((String) cell);
                            if (cellNumber != null)
                                csvToJsonNSGILD.put(key, cellNumber);
                            else
                                csvToJsonNSGILD.put(key, cell);
                        }

                    }
                    indexData++;
                }
                listOfObjects.add(csvToJsonNSGILD);
                csvToJsonNSGILD = new HashMap<>();
            }
            index++;
        }
        return listOfObjects;
    }

    private Object asNumber(String strNum) {
        try {
            if (strNum.contains(".") || strNum.contains(","))
                return Double.parseDouble(strNum);
            else
                return Integer.parseInt(strNum);
        } catch (NumberFormatException | NullPointerException nfe) {
            return null;
        }
    }

}
