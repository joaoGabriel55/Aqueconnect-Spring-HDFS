package imd.smartmetropolis.aqueconnect.processors.hdfs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandleHDFS
 */
public interface HandleHDFS {

    void writeFile(String userId, String path, String fileContent);

    List<ConcurrentHashMap<String, Object>> readFile(String userId, String path);

    List<Map<String, Object>> listDirectory(String userId, String path);

    boolean createDirectory(String userId, String name);

    boolean renameDirectoryOrFile(String userId, String oldName, String newName);

    boolean removeDirectoryOrFile(String userId, String name);
}