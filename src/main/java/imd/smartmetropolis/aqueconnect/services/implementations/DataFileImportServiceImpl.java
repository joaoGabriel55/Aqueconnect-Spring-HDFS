package imd.smartmetropolis.aqueconnect.services.implementations;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.services.DataFileImportService;
import imd.smartmetropolis.aqueconnect.services.FileJsonConverterService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.BASE_AQUEDUCTE_URL;

@Service
@Log4j2
public class DataFileImportServiceImpl implements DataFileImportService {

    public static final String NGSILD_IMPORT_DATA_FILE = BASE_AQUEDUCTE_URL + "import-ngsild-data/file/";

    @Autowired
    private FileJsonConverterService fileJsonConverterService;

    @Override
    public Map<String, Integer> getFileFieldsMap(List<String> fields) {
        Map<String, Integer> fieldsMap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldsMap.put(field, fields.indexOf(field));
        }
        log.info("getFieldsMap");
        return fieldsMap;
    }

    @Override
    public List<Future<Map<String, Object>>> importFileDataNGSILDByAqueducte(
            ExecutorService executor,
            Map<String, String> headers, Map<String, String> allParams, String type,
            BufferedReader reader,
            FieldsSelectedConfig fieldsSelectedConfig,
            String delimiter,
            long countLines
    ) throws Exception {
        List<Future<Map<String, Object>>> futureList = new ArrayList<>();

        ImportNGSILDDataConfig importConfig = fieldsSelectedConfig.getImportNGSILDDataConfig();
        if (importConfig == null) {
            String msg = "ImportNGSILDDataConfig is null";
            log.error(msg);
            throw new Exception(msg);
        }
        int blockSize = 500;
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
                        Map<String, Object> result = getJsonData(finalLine, fieldsSelectedConfig.getFieldsSelected());
                        if (lineCount <= blockSize && lineCount <= countLines) {
                            importConfig.getDataCollection().add(result);
                        } else {
                            importConfig.getDataCollection().add(result);
                            log.info("Data collection size {}", importConfig.getDataCollection().size());
                            if (importConfig != null && importConfig.getDataCollection().size() == (blockSize + 1)) {
                                ImportNGSILDDataConfig configCopy = new ImportNGSILDDataConfig(
                                        importConfig.getPrimaryField(),
                                        importConfig.getContextLinks(),
                                        importConfig.getMatchingConverterSetup(),
                                        importConfig.getDataCollection()
                                );
                                futureList.add(executor.submit(new ImportNGSILDData(headers, allParams, type, configCopy)));
                                importConfig.setDataCollection(new ArrayList<>());
                            }
                            lineCount = 0;
                        }
                    }
                }
                lineCount++;
            }
            reader.close();
            if (importConfig != null && importConfig.getDataCollection().size() <= remains) {
                log.info("Data collection size {}", importConfig.getDataCollection().size());
                futureList.add(executor.submit(new ImportNGSILDData(headers, allParams, type, importConfig)));
            }

            log.info("importFileDataNGSILDByAqueducte");
            return futureList;
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException(e.getMessage());
        }
    }

    private Map<String, Object> getJsonData(String finalLine, Map<String, Integer> fieldsSelected) {
        List<Map<String, Object>> result = fileJsonConverterService.jsonConverter(finalLine, fieldsSelected);
        return result.get(0);
    }
}
