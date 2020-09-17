package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.processors.FileConverterToJSONProcessor;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.execute;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.httpPost;
import static org.apache.http.HttpStatus.SC_OK;

@Component
@Log4j2
public class FileDataImportToSGEOLService {

    private static final String NGSILD_IMPORT_DATA_FILE = BASE_AQUEDUCTE_URL + "import-ngsild-data/file/";

    public Map<String, Integer> getFieldsMap(List<String> fields) {
        Map<String, Integer> fieldsMap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldsMap.put(field, fields.indexOf(field));
        }
        log.info("getFieldsMap");
        return fieldsMap;
    }

    private Map<String, Object> getJsonData(
            FileConverterToJSONProcessor processor, String finalLine, Map<String, Integer> fieldsSelected
    ) {
        List<Map<String, Object>> result = processor.jsonConverter(finalLine, fieldsSelected);
        return result.get(0);
    }

    public void importFileDataNGSILDByAqueducte(
            Map<String, String> headers, Map<String, String> allParams, String type,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) throws Exception {
        FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
        ImportNGSILDDataConfig importConfig = fieldsSelectedConfig.getImportNGSILDDataConfig();
        if (importConfig == null) {
            String msg = "ImportNGSILDDataConfig is null";
            log.error(msg);
            throw new Exception(msg);
        }
        int blockSize = 1000;
        long remains = countLines % blockSize;
        try {
            int lineCount = 0;
            String header = null;
            while (reader.ready()) {
                String line = (reader.readLine() + "\n").replace(delimiter, ",");
                if (line != null && !line.equals("")) {
                    if (header == null) {
                        header = line;
                    } else {
                        String finalLine = header + line;
                        Map<String, Object> result = getJsonData(
                                processor, finalLine, fieldsSelectedConfig.getFieldsSelected()
                        );
                        if (lineCount <= blockSize && lineCount <= countLines) {
                            importConfig.getDataCollection().add(result);
                        } else {
                            importConfig.getDataCollection().add(result);
                            convertJsonIntoNGSILDAndImportData(headers, allParams, type, importConfig);
                            importConfig.setDataCollection(new ArrayList<>());
                            lineCount = 0;
                        }
                    }
                }
                lineCount++;
            }
            reader.close();
            if (importConfig != null && importConfig.getDataCollection().size() <= remains)
                convertJsonIntoNGSILDAndImportData(headers, allParams, type, importConfig);
            log.info("importFileDataNGSILDByAqueducte");
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException(e.getMessage());
        }
    }

    private void convertJsonIntoNGSILDAndImportData(
            Map<String, String> headers, Map<String, String> allParams, String type, ImportNGSILDDataConfig importConfig
    ) throws Exception {
        try {
            URIBuilder url = new URIBuilder(NGSILD_IMPORT_DATA_FILE + type);
            for (Map.Entry<String, String> query : allParams.entrySet())
                url.setParameter(query.getKey(), query.getValue());

            HttpResponse responsePure = execute(httpPost(url.build().toString(), importConfig, headers));
            int statusCode = responsePure.getStatusLine().getStatusCode();
            if (statusCode == SC_OK) {
                log.info("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
            } else {
                log.error("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
                throw new Exception();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }
}
