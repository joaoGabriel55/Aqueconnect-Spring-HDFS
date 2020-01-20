package imd.smartmetropolis.aqueconnect.processors.hdfs;

import java.util.List;
import java.util.Map;

/**
 * HandleHDFSFiles
 */
public interface HandleHDFSFiles {

    void writeFile(String path, String fileContent);

    List<Map<String, Object>> readFile(String path);
}