package imd.smartmetropolis.aqueconnect.processors;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.utils.FormatterUtil.removeAccents;

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
        int index = 0;
        for (String[] row : allData) {
            if (index != 0) {
                int indexData = 0;
                for (Object cell : row) {
                    if (cell == "" || cell.toString().length() == 0)
                        cell = null;
                    for (Map.Entry<String, Integer> entry : fieldsSelected.entrySet()) {
                        if (entry.getValue() == indexData) {
                            String key = entry.getKey()
                                    .replace(" ", "_")
                                    .toLowerCase()
                                    .trim();
                            Object cellNumber = asNumber((String) cell);
                            if (cellNumber != null)
                                csvToJsonNSGILD.put(removeAccents(key), cellNumber);
                            else
                                csvToJsonNSGILD.put(removeAccents(key), cell);
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
