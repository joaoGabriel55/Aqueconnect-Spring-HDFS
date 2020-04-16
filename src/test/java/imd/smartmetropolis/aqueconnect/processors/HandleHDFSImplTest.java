package imd.smartmetropolis.aqueconnect.processors;

import imd.smartmetropolis.aqueconnect.AbstractTest;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HandleHDFSImplTest extends AbstractTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void readFile() {
        Path path = new Path("/user/data/cf7dbe44-30eb-4145-8636-9ce0bc49e0ed/MATRICULA_SUDESTE.csv");
        String result = HandleHDFSImpl.getInstance().readFileAsString(path);
        assertFalse(result.isEmpty());
    }

    @Test
    public void listDirectory() {
        List<Map<String, Object>> jsonObject = HandleHDFSImpl.getInstance().listDirectory(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "testhdfs1"
        );
        assertFalse(jsonObject.isEmpty());
    }

    @Test
    public void createDirectorySuccess() {
        boolean created = HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertTrue(created);
        assertTrue(HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void renameDirectorySuccess() {
        HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1",
                "test2"
        );
        assertTrue(renamed);
        assertTrue(HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test2"));
    }

    @Test
    public void renameFileSuccess() {
        HandleHDFSImpl.getInstance().writeFileString("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1/text.txt",
                "test1/text2.txt"
        );
        assertTrue(renamed);
        assertTrue(HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void removeDirectorySuccess() {
        HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertTrue(removed);
    }

    @Test
    public void removeFileSuccess() {
        HandleHDFSImpl.getInstance().writeFileString("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt");
        assertTrue(removed);
        assertTrue(HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

}
