package imd.smartmetropolis.aqueconnect.processors.hdfs;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandleHDFS
 */
public interface HandleHDFS {

    BufferedReader openFileBuffer(String userId, String path) throws IOException;

    void writeFileInputStream(String userId, String path, InputStream fileContent) throws Exception;

    void writeFileString(String userId, String path, String fileContent);

    String readFileLines(int lineCount, String userId, String path);

    Long lineCount(String userId, String path) throws IOException;

    String readFileAsString(Path hdfsReadPath);

    List<ConcurrentHashMap<String, Object>> readFileJson(String userId, String path);

    List<Map<String, Object>> listDirectory(String userId, String path);

    boolean createDirectory(String userId, String name);

    boolean renameDirectoryOrFile(String userId, String oldName, String newName);

    boolean removeDirectoryOrFile(String userId, String name);
}