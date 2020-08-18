package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.processors.FileConverterToJSONProcessor;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpResponse;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.*;
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

    private void addEntitiesId(List<String> ngsildDataIds, List<String> entitiesIDs) {
        if (ngsildDataIds != null) {
            if (entitiesIDs.size() <= 500000)
                entitiesIDs.addAll(ngsildDataIds);
        }
    }

    public List<String> importFileDataNGSILDByAqueducte(
            String sgeolInstance,
            String appToken,
            String userToken,
            String layer,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) throws IOException {
        List<String> entitiesIDs = new ArrayList<>();
        FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
        ImportNGSILDDataConfig importConfig = fieldsSelectedConfig.getImportNGSILDDataConfig();
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
                        if (lineCount <= blockSize && lineCount <= countLines) {
                            String finalLine = header + line;
                            List<Map<String, Object>> result = processor.jsonConverter(
                                    finalLine, fieldsSelectedConfig.getFieldsSelected()
                            );
                            if (importConfig != null) {
                                importConfig.getDataCollection().add(result.get(0));
                            }
                        } else {
                            List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                                    sgeolInstance, appToken, userToken, layer, importConfig
                            );
                            addEntitiesId(ngsildDataIds, entitiesIDs);
                            lineCount = 0;
                        }
                    }
                }
                lineCount++;
            }
            reader.close();
            if (importConfig != null && importConfig.getDataCollection().size() <= remains) {
                List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                        sgeolInstance, appToken, userToken, layer, importConfig
                );
                addEntitiesId(ngsildDataIds, entitiesIDs);
            }
            log.info("importFileDataNGSILDByAqueducte");
            return entitiesIDs;
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    private List<String> convertJsonIntoNGSILDAndImportData(
            String sgeolInstance,
            String appToken,
            String userToken,
            String layer,
            ImportNGSILDDataConfig importConfig
    ) throws IOException {
        try {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(SGEOL_INSTANCE, sgeolInstance);
            headers.put(APP_TOKEN, appToken);
            headers.put(USER_TOKEN, userToken);
            HttpResponse responsePure = execute(httpPost(NGSILD_IMPORT_DATA_FILE + layer, importConfig, headers));
            Map<String, Object> response = buildResponse(
                    responsePure.getStatusLine().getStatusCode(),
                    responsePure.getStatusLine().getReasonPhrase(),
                    responsePure.getEntity().getContent()
            );
            int statusCode = (int) response.get("statusCode");
            if (statusCode == SC_OK) {
                log.info("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
                return (List<String>) response.get("data");
            } else {
                log.error("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }
}
