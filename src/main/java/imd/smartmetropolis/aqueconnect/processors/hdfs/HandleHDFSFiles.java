package imd.smartmetropolis.aqueconnect.processors.hdfs;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandleHDFSFiles
 */
public interface HandleHDFSFiles {

    void writeFile(String path, String fileContent);

    List<ConcurrentHashMap<String, Object>> readFile(String path);
}