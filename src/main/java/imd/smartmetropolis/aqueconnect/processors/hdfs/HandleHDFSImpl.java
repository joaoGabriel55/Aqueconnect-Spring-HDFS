package imd.smartmetropolis.aqueconnect.processors.hdfs;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.*;
import static imd.smartmetropolis.aqueconnect.services.HDFSService.buildHATEOAS;

/**
 * {@link HandleHDFSImpl}
 */
@Log4j2
public class HandleHDFSImpl implements HandleHDFS {
    public static final String BASE_PATH = "/user/data/";

    private static FileSystem fs;

    private static HandleHDFS handleHDFS;

    public static HandleHDFS getInstance() {
        if (handleHDFS == null) {
            handleHDFS = new HandleHDFSImpl();
            initConfHDFS();
        }
        return handleHDFS;
    }

    private static void initConfHDFS() {
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", HDFS_URI);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", USER_NAME_HDFS);
        System.setProperty("hadoop.home.dir", "/");
        // Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(HDFS_URI), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BufferedReader openFileBuffer(String userId, String path) throws IOException {
        try {
            String fullPath = BASE_PATH + userId + "/" + path;
            Path hdfsReadPath = new Path(userId != null ? fullPath : path);
            FSDataInputStream inputStream = fs.open(hdfsReadPath);
            log.info("openFileBuffer: {}", path);
            return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @Override
    public void writeFileInputStream(String userId, String path, InputStream fileContent) throws Exception {
        // Create a path
        String pathWriteFirstTime = BASE_PATH + userId + "/" + path;
        Path hdfsWritePath = new Path(userId != null ? pathWriteFirstTime : path);
        try {
            FSDataOutputStream outputStream = fs.create(hdfsWritePath, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileContent, StandardCharsets.ISO_8859_1));
            while (reader.ready()) {
                String line = reader.readLine() + "\n";
                outputStream.writeBytes(line);
            }
            log.info("writeFileInputStream: {}", path);
            outputStream.close();
        } catch (IOException e) {
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new Exception();
        }
    }

    @Override
    public void writeFileString(String userId, String path, String fileContent) {
        // Create a path
        String pathWriteFirstTime = BASE_PATH + userId + "/" + path;
        Path hdfsWritePath = new Path(userId != null ? pathWriteFirstTime : path);
        try {
            FSDataOutputStream outputStream = fs.create(hdfsWritePath, true);
            outputStream.writeBytes(fileContent);
            outputStream.close();
            log.info("writeFileString: {}", path);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
        }
    }

    @Override
    public String readFileLines(int lineCount, String userId, String path) throws IOException {
        try {
            BufferedReader reader = openFileBuffer(userId, path);
            int countLine = 0;
            StringBuilder stringBuilder = new StringBuilder();
            while (reader.ready() && countLine < lineCount) {
                String line = reader.readLine() + "\n";
                stringBuilder.append(line);
                countLine++;
            }
            reader.close();
            log.info("readFileLines: {} - Lines: {}", path, lineCount);
            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ConcurrentHashMap<String, Object>> readFileJson(String userId, String path) throws Exception {
        try {
            String pathWriteFirstTime = BASE_PATH + userId + "/" + path;
            Path hdfsReadPath = new Path(userId != null ? pathWriteFirstTime : path);

            // Init input stream
            String out = readFileAsString(hdfsReadPath);
            JSONArray jsonArray = new JSONArray(out);
            log.info("readFileJson: {}", path);
            return jsonArray
                    .toList()
                    .stream()
                    .map(elem -> new ConcurrentHashMap<>(((Map<String, Object>) elem)))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new Exception();
        }
    }

    @Override
    public String readFileAsString(Path hdfsReadPath) throws IOException {
        try {
            FSDataInputStream inputStream = fs.open(hdfsReadPath);
            log.info("readFileAsString: {}", hdfsReadPath);
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @Override
    public Long lineCount(String userId, String path) throws IOException {
        try {
            BufferedReader readerLines = openFileBuffer(userId, path);
            Long linesCount = readerLines.lines().count();
            readerLines.close();
            log.info("lineCount: {}", path);
            return linesCount;
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @SuppressWarnings("ALL")
    @Override
    public List<Map<String, Object>> listDirectory(String userId, String path) throws Exception {
        try {
            String pathString = path != null ? "/" + path : "";
            String urlPath = WEB_HDFS_URL + userId + pathString + "?op=LISTSTATUS";
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Map> responseEntity = restTemplate.getForEntity(urlPath, Map.class);
            Map<String, Object> objectMap = responseEntity.getBody();
            if (objectMap != null && objectMap.containsKey("FileStatuses")) {
                Map<String, Object> fileStatuses = (Map<String, Object>) objectMap.get("FileStatuses");
                if (fileStatuses.containsKey("FileStatus")) {
                    List<Map<String, Object>> fileStatusList = (List<Map<String, Object>>) fileStatuses.get("FileStatus");
                    log.info("listDirectory: {}", path);
                    return fileStatusList.stream().map(elem -> buildHATEOAS(userId, elem)).collect(Collectors.toList());
                }
                return null;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new Exception();
        }
    }

    @Override
    public boolean createDirectory(String userId, String name) throws IOException {
        // Create a path
        Path directory = new Path(BASE_PATH + userId + "/" + name);
        try {
            log.info("createDirectory {}", name);
            return fs.mkdirs(directory);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @Override
    public boolean renameDirectoryOrFile(String userId, String oldName, String newName) throws IOException {
        // Create a path
        String pathDefault = BASE_PATH + userId + "/";
        Path oldNamePath = new Path(pathDefault + oldName);
        Path newNamePath = new Path(pathDefault + newName);
        try {
            log.info("renameDirectoryOrFile: oldName - {} | newName - {}", oldName, newName);
            return fs.rename(oldNamePath, newNamePath);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

    @Override
    public boolean removeDirectoryOrFile(String userId, String name) throws IOException {
        // Create a path
        Path directory = new Path(BASE_PATH + userId + "/" + name);
        try {
            log.error("removeDirectoryOrFile: {}", name);
            return fs.delete(directory, true);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage() + " {}", e.getStackTrace());
            throw new IOException();
        }
    }

}