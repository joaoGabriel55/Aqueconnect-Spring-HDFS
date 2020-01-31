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

import static imd.smartmetropolis.aqueconnect.utils.PropertiesParams.BASE_AQUEDUCTE_URL;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtils.*;
import static org.apache.http.HttpStatus.SC_OK;

@Component
public class FileDataImportToSGEOLService {

    private static final String NGSILD_CONVERTER_WITHOUT_CONTEXT = BASE_AQUEDUCTE_URL + "ngsildConverter/";

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


    public List<String> importFileDataNGSILDByAqueducte(String layer,
                                                        BufferedReader reader,
                                                        FieldsSelectedConfig fieldsSelectedConfig,
                                                        String delimiter
    ) {
        List<String> entitiesIDs = new ArrayList<>();
        FileConverterToJSONProcessor processor = new FileConverterToJSONProcessor();
        ImportNGSILDDataWithoutContextConfig importConfig = new ImportNGSILDDataWithoutContextConfig();
        try {
            String header = null;
            while (reader.ready()) {
                String line = (reader.readLine() + "\n").replace(delimiter, ",");
                if (line != null && line != "") {
                    if (header == null) {
                        header = line;
                    } else {
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
                        importConfig.setDataContentForNGSILDConversion(result);
                        // TODO: Send Data to Aqueducte format and send this data to SGEOL middleware
                        // TODO: Auth - set headers
                        List<LinkedHashMap<String, Object>> ngsildData = convertJsonIntoNGSILD(layer, importConfig);
                        if (ngsildData != null) {
                            //TODO Import to SGEOL
                            List<String> entityID = importDataNGSILD("", "", layer, ngsildData);
                            if (entityID != null && entityID.size() > 0)
                                entitiesIDs.add(entityID.get(0));
                        }

                    }

                }
            }
            reader.close();
        } catch (IOException e) {
            return null;
        }
        return entitiesIDs;
    }

    private List<LinkedHashMap<String, Object>> convertJsonIntoNGSILD(String layer,
                                                                      ImportNGSILDDataWithoutContextConfig importConfig) {
        try {
            HttpResponse responsePure = execute(
                    httpPost(NGSILD_CONVERTER_WITHOUT_CONTEXT + layer, importConfig)
            );

            Map<String, Object> response = buildResponse(
                    responsePure.getStatusLine().getStatusCode(),
                    responsePure.getStatusLine().getReasonPhrase(),
                    responsePure.getEntity().getContent()
            );
            int statusCode = (int) response.get("statusCode");
            if (statusCode == SC_OK) {
                return (List<LinkedHashMap<String, Object>>) response.get("data");
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<String> importDataNGSILD(String appToken,
                                          String userToken,
                                          String layer,
                                          List<LinkedHashMap<String, Object>> ngsildData
    ) {
        List<String> entitiesIDs = new ArrayList<>();
        return entitiesIDs;
    }
}
