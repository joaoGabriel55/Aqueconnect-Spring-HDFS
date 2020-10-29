package imd.smartmetropolis.aqueconnect.services.implementations;

import com.google.common.collect.HashBiMap;
import com.opencsv.CSVReader;
import imd.smartmetropolis.aqueconnect.services.FileJsonConverterService;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;

import static imd.smartmetropolis.aqueconnect.utils.FormatterUtil.encodeText;

@Service
@Log4j2
public class FileJsonConverterServiceImpl implements FileJsonConverterService {
    @Override
    public List<Map<String, Object>> jsonConverter(String contentFile, Map<String, Integer> fieldsSelected) {
        List<Map<String, Object>> listOfObjects = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new StringReader(contentFile))) {
            List<String[]> allData;
            allData = reader.readAll();
            Map<String, Object> csvToJson = new HashMap<>();
            listOfObjects = generateJson(csvToJson, allData, fieldsSelected);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
        }
        log.info("jsonConverter");
        return listOfObjects;
    }

    private List<Map<String, Object>> generateJson(
            Map<String, Object> csvToJsonNSGILD,
            List<String[]> allData,
            Map<String, Integer> fieldsSelected
    ) throws UnsupportedEncodingException {
        List<Map<String, Object>> listOfObjects = new ArrayList<>();
        int index = 0;
        for (String[] row : allData) {
            if (index != 0) {
                int indexData = 0;
                for (Object cell : row) {
                    if (cell == "" || cell.toString().length() == 0)
                        cell = null;

                    if (fieldsSelected.containsValue(indexData)) {
                        HashBiMap<String, Integer> fields = HashBiMap.create();
                        fields.putAll(fieldsSelected);
                        String key = fields.inverse().get(indexData);
                        Object cellNumber = asNumber((String) cell);
                        if (cellNumber != null)
                            csvToJsonNSGILD.put(key, cellNumber);
                        else
                            csvToJsonNSGILD.put(key, encodeText(cell));
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
            if (strNum.contains(".") || strNum.contains(",")) {
                NumberFormat format = NumberFormat.getInstance(Locale.FRANCE);
                Number number = format.parse(strNum.replace('.', ','));
                return number.doubleValue();
            }
            return null;
        } catch (NumberFormatException | NullPointerException | ParseException e) {
            return null;
        }
    }
}
