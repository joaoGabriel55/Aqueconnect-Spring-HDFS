package imd.smartmetropolis.aqueconnect.service;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataWithoutContextConfig;
import imd.smartmetropolis.aqueconnect.processors.FileConverterToJSONProcessor;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.*;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtils.*;
import static org.apache.http.HttpStatus.SC_OK;

@Component
public class FileDataImportToSGEOLService {

    private static final String NGSILD_IMPORT_FILE_WITHOUT_CONTEXT = BASE_AQUEDUCTE_URL + "importToSgeol/file/";

    // TODO: Send to Devboard that task status.
    @Autowired
    private TaskStatusService taskStatusService;

    public Map<String, Integer> getFieldsMap(List<String> fields) {
        Map<String, Integer> fieldsMap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldsMap.put(field, fields.indexOf(field));
        }
        return fieldsMap;
    }


    public List<String> importFileDataNGSILDByAqueducte(
            String appToken,
            String userToken,
            String layer,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) {
        List<String> entitiesIDs = new ArrayList<>();
        FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
        ImportNGSILDDataWithoutContextConfig importConfig = new ImportNGSILDDataWithoutContextConfig();
        Integer blockSize = 50000;
        long remains = countLines % blockSize;
        try {
            Integer lineCount = 0;
            String header = null;
            while (reader.ready()) {
                String line = (reader.readLine() + "\n").replace(delimiter, ",");
                if (line != null && line != "") {
                    if (header == null) {
                        header = line;
                    } else {
                        if (lineCount <= blockSize && lineCount <= countLines) {
                            String finalLine = header + line;
                            List<Map<String, Object>> result = processor.jsonConverter(
                                    finalLine,
                                    fieldsSelectedConfig.getFieldsSelected()
                            );
                            importConfig.setGeoLocationConfig(
                                    fieldsSelectedConfig
                                            .getImportNGSILDDataWithoutContextConfig()
                                            .getGeoLocationConfig()
                            );

                            importConfig.getDataContentForNGSILDConversion().add(result.get(0));
                        } else {
                            List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                                    appToken, userToken, layer, importConfig
                            );
                            if (ngsildDataIds != null) {
                                entitiesIDs.addAll(ngsildDataIds);
                            }
                            importConfig = new ImportNGSILDDataWithoutContextConfig();
                            lineCount = 0;
                        }

                    }

                }
                lineCount++;
            }
            reader.close();
            if (importConfig.getDataContentForNGSILDConversion().size() <= remains) {
                List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                        appToken, userToken, layer, importConfig
                );
                if (ngsildDataIds != null) {
                    entitiesIDs.addAll(ngsildDataIds);
                }
            }
        } catch (IOException e) {
            return null;
        }
        return entitiesIDs;
    }


    private List<String> convertJsonIntoNGSILDAndImportData(String appToken,
                                                            String userToken,
                                                            String layer,
                                                            ImportNGSILDDataWithoutContextConfig importConfig) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(APP_TOKEN, appToken);
        headers.put(USER_TOKEN, userToken);
        try {

            HttpResponse responsePure = execute(
                    httpPost(NGSILD_IMPORT_FILE_WITHOUT_CONTEXT + layer, importConfig, headers)
            );

            Map<String, Object> response = buildResponse(
                    responsePure.getStatusLine().getStatusCode(),
                    responsePure.getStatusLine().getReasonPhrase(),
                    responsePure.getEntity().getContent()
            );
            int statusCode = (int) response.get("statusCode");
            if (statusCode == SC_OK) {
                return (List<String>) response.get("data");
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
