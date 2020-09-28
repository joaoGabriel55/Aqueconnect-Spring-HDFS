package imd.smartmetropolis.aqueconnect.services.implementations;

import imd.smartmetropolis.aqueconnect.services.FileService;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.WEB_HDFS_URL;
import static imd.smartmetropolis.aqueconnect.utils.HdfsUtils.initConfHDFS;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.BASE_PATH;
import static imd.smartmetropolis.aqueconnect.utils.RequestsUtil.buildHATEOAS;

@Service
@Log4j2
@Qualifier("hdfsServiceImpl")
public class HdfsServiceImpl implements FileService {

    private static FileSystem fs;

    public HdfsServiceImpl() {
        if (fs == null)
            fs = initConfHDFS();
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
    public InputStreamResource getFileResource(String userId, String path) throws IOException {
        try {
            String fullPath = BASE_PATH + userId + "/" + path;
            Path hdfsReadPath = new Path(userId != null ? fullPath : path);
            FSDataInputStream inputStream = fs.open(hdfsReadPath);
            InputStreamResource resource = new InputStreamResource(inputStream);
            log.info("getFileResource: {}", path);
            return resource;
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
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileContent, StandardCharsets.ISO_8859_1));
            IOUtils.copy(bufferedReader, outputStream);
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
