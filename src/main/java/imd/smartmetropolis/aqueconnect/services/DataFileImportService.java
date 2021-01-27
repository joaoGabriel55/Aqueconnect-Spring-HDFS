package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import org.apache.http.StatusLine;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Component
public interface DataFileImportService {

    Map<String, Integer> getFileFieldsMap(List<String> fields);

    List<Future<Map<String, Object>>> importFileDataNGSILDByAqueducte(
            ExecutorService executor,
            Map<String, String> headers, Map<String, String> allParams, String type,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) throws Exception;
}
