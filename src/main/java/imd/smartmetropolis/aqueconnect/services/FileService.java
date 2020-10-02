package imd.smartmetropolis.aqueconnect.services;

import org.apache.hadoop.fs.Path;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public interface FileService {
    BufferedReader openFileBuffer(String userId, String path) throws IOException;

    InputStreamResource getFileResource(String userId, String path) throws IOException;

    void writeFileInputStream(String userId, String path, InputStream fileContent) throws Exception;

    void writeFileString(String userId, String path, String fileContent);

    void writeImage(String userId, String path, InputStream fileContent) throws IOException;

    String readFileLines(int lineCount, String userId, String path) throws IOException;

    Long lineCount(String userId, String path) throws IOException;

    String readFileAsString(Path hdfsReadPath) throws IOException;

    List<ConcurrentHashMap<String, Object>> readFileJson(String userId, String path) throws Exception;

    List<Map<String, Object>> listDirectory(String userId, String path) throws Exception;

    boolean createDirectory(String userId, String name) throws IOException;

    boolean renameDirectoryOrFile(String userId, String oldName, String newName) throws IOException;

    boolean removeDirectoryOrFile(String userId, String name) throws IOException;
}
