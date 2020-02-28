package imd.smartmetropolis.aqueconnect.service;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
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

@SuppressWarnings("ALL")
@Component
public class FileDataImportToSGEOLService {

    public static final String CONTEXT = "context";

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

    /**
     * @param type: context || standard
     */
    private LinkedHashMap<String, Object> getImportSetupConfig(String typeImportSetup, FieldsSelectedConfig fieldsSelectedConfig) {
        try {
            LinkedHashMap<String, Object> importConfig = new LinkedHashMap<>();
            if (typeImportSetup.equals(CONTEXT)) {
                importConfig.put("contextLink", fieldsSelectedConfig.getImportNGSILDDataConfig().get("contextLink"));
                importConfig.put("matchingConfigContent", fieldsSelectedConfig.getImportNGSILDDataConfig().get("matchingConfigContent"));
            } else {
                importConfig.put("geoLocationConfig", fieldsSelectedConfig.getImportNGSILDDataConfig().get("geoLocationConfig"));
            }
            importConfig.put("dataContentForNGSILDConversion", new ArrayList<>());
            return importConfig;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<String> importFileDataNGSILDByAqueducte(
            String appToken,
            String userToken,
            String typeImportSetup,
            String layer,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) {
        List<String> entitiesIDs = new ArrayList<>();
        FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
        Integer blockSize = 50000;
        long remains = countLines % blockSize;

        LinkedHashMap<String, Object> importConfig = getImportSetupConfig(typeImportSetup, fieldsSelectedConfig);

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
                            ((List<Map<String, Object>>) importConfig
                                    .get("dataContentForNGSILDConversion"))
                                    .add(result.get(0));
                        } else {
                            List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                                    appToken, userToken, typeImportSetup, layer, importConfig
                            );
                            if (ngsildDataIds != null) {
                                entitiesIDs.addAll(ngsildDataIds);
                            }
                            importConfig = new LinkedHashMap<>();
                            lineCount = 0;
                        }

                    }

                }
                lineCount++;
            }
            reader.close();
            if (((List<LinkedHashMap<String, Object>>) importConfig.get("dataContentForNGSILDConversion")).size() <= remains) {
                List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                        appToken, userToken, typeImportSetup, layer, importConfig
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
                                                            String typeImportSetup,
                                                            String layer,
                                                            LinkedHashMap<String, Object> importConfig) {
        try {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(APP_TOKEN, appToken);
            headers.put(USER_TOKEN, userToken);

            String urlImportService = NGSILD_IMPORT_FILE_WITHOUT_CONTEXT + layer;
            if (typeImportSetup.equals(CONTEXT))
                urlImportService = NGSILD_IMPORT_FILE_WITHOUT_CONTEXT + CONTEXT + "/" + layer;

            HttpResponse responsePure = execute(
                    httpPost(urlImportService, importConfig, headers)
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
