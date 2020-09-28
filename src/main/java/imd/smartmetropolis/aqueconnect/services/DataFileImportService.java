package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;

@Component
public interface DataFileImportService {

    Map<String, Integer> getFileFieldsMap(List<String> fields);

    void importFileDataNGSILDByAqueducte(
            Map<String, String> headers, Map<String, String> allParams, String type,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) throws Exception;
}
