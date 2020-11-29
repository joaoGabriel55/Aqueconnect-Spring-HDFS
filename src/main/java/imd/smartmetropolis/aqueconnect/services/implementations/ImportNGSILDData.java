package imd.smartmetropolis.aqueconnect.services.implementations;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static imd.smartmetropolis.aqueconnect.services.implementations.DataFileImportServiceImpl.NGSILD_IMPORT_DATA_FILE;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.execute;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.httpPost;
import static org.apache.http.HttpStatus.SC_OK;

@Log4j2
public class ImportNGSILDData implements Callable<Map<String, Object>> {
    private Map<String, String> headers;
    private Map<String, String> allParams;
    private String type;
    private ImportNGSILDDataConfig importConfig;

    public ImportNGSILDData(
            Map<String, String> headers, Map<String, String> allParams, String type, ImportNGSILDDataConfig importConfig
    ) {
        this.headers = headers;
        this.allParams = allParams;
        this.type = type;
        this.importConfig = importConfig;
    }

    @Override
    public Map<String, Object> call() throws Exception {
        return convertJsonIntoNGSILDAndImportData(headers, allParams, type, importConfig);
    }

    private Map<String, Object> convertJsonIntoNGSILDAndImportData(
            Map<String, String> headers, Map<String, String> allParams, String type, ImportNGSILDDataConfig importConfig
    ) throws Exception {
        try {
            Map<String, Object> responseResult = new HashMap<>();
            URIBuilder url = new URIBuilder(NGSILD_IMPORT_DATA_FILE + type);
            for (Map.Entry<String, String> query : allParams.entrySet())
                url.setParameter(query.getKey(), query.getValue());

            HttpResponse responsePure = execute(httpPost(url.build().toString(), importConfig, headers));
            int statusCode = responsePure.getStatusLine().getStatusCode();

            responseResult.put("status_code", responsePure.getStatusLine().getStatusCode());
            responseResult.put("data_collection_size", importConfig.getDataCollection().size());

            if (statusCode == SC_OK) {
                log.info("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
                return responseResult;
            }
            log.error("convertJsonIntoNGSILDAndImportData: status code - {}", statusCode);
            return responseResult;
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }
}
