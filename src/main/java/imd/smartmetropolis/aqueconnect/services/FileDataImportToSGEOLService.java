package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.context.ImportNGSILDDataWithContextConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard.ImportNGSILDDataWithoutContextConfig;
import imd.smartmetropolis.aqueconnect.processors.FileConverterToJSONProcessor;
import org.apache.http.HttpResponse;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtils.*;
import static org.apache.http.HttpStatus.SC_OK;

@Component
public class FileDataImportToSGEOLService {

    private static final String NGSILD_IMPORT_FILE_STANDARD = BASE_AQUEDUCTE_URL + "importToSgeol/file/";
    private static final String NGSILD_IMPORT_FILE_CONTEXT = NGSILD_IMPORT_FILE_STANDARD + "context/";

    private final ImportNGSILDDataConfigService importDataConfigService = ImportNGSILDDataConfigService.getServiceInstance();

    public Map<String, Integer> getFieldsMap(List<String> fields) {
        Map<String, Integer> fieldsMap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldsMap.put(field, fields.indexOf(field));
        }
        return fieldsMap;
    }

    private void mountImportConfig(FieldsSelectedConfig fieldsSelectedConfig, ImportNGSILDDataConfig importConfig) {
        if (fieldsSelectedConfig.getImportNGSILDDataWithoutContextConfig() != null &&
                importConfig instanceof ImportNGSILDDataWithoutContextConfig) {
            importDataConfigService.mountImportConfigStandard(
                    fieldsSelectedConfig, (ImportNGSILDDataWithoutContextConfig) importConfig
            );
        } else if (fieldsSelectedConfig.getImportNGSILDDataWithContextConfig() != null &&
                importConfig instanceof ImportNGSILDDataWithContextConfig) {
            importDataConfigService.mountImportConfigContext(
                    fieldsSelectedConfig, (ImportNGSILDDataWithContextConfig) importConfig
            );
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
        ImportNGSILDDataConfig importConfig = importDataConfigService.getInstanceImportConfig(typeImportSetup);
        int blockSize = 50000;
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
                                mountImportConfig(fieldsSelectedConfig, importConfig);
                                importConfig.getDataContentForNGSILDConversion().add(result.get(0));
                            }
                        } else {
                            List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                                    appToken, userToken, typeImportSetup, layer, importConfig
                            );
                            addEntitiesId(ngsildDataIds, entitiesIDs);
                            importConfig = importDataConfigService.getInstanceImportConfig(typeImportSetup);
                            lineCount = 0;
                        }
                    }
                }
                lineCount++;
            }
            reader.close();
            if (importConfig != null && importConfig.getDataContentForNGSILDConversion().size() <= remains) {
                List<String> ngsildDataIds = convertJsonIntoNGSILDAndImportData(
                        appToken, userToken, typeImportSetup, layer, importConfig
                );
                addEntitiesId(ngsildDataIds, entitiesIDs);
            }
        } catch (
                IOException e) {
            return null;
        }
        return entitiesIDs;
    }

    private void addEntitiesId(List<String> ngsildDataIds, List<String> entitiesIDs) {
        if (ngsildDataIds != null) {
            if (entitiesIDs.size() <= 500000)
                entitiesIDs.addAll(ngsildDataIds);
        }
    }

    private List<String> convertJsonIntoNGSILDAndImportData(String appToken,
                                                            String userToken,
                                                            String typeImportSetup,
                                                            String layer,
                                                            ImportNGSILDDataConfig importConfig) {
        try {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(APP_TOKEN, appToken);
            headers.put(USER_TOKEN, userToken);
            String URI = typeImportSetup.equals("context") ? NGSILD_IMPORT_FILE_CONTEXT : NGSILD_IMPORT_FILE_STANDARD;
            HttpResponse responsePure = execute(httpPost(URI + layer, importConfig, headers));
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
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
