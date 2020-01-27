package imd.smartmetropolis.aqueconnect.processors;

import imd.smartmetropolis.aqueconnect.AbstractTest;
import imd.smartmetropolis.aqueconnect.processors.hdfs.HandleHDFSImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HandleHDFSImplTest extends AbstractTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void listDirectory() throws Exception {
        List<Map<String, Object>> jsonObject = HandleHDFSImpl.getInstance().listDirectory(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "testhdfs1"
        );
        assertEquals(true, !jsonObject.isEmpty());
    }

    @Test
    public void createDirectorySuccess() throws Exception {
        boolean created = HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertEquals(true, created);
        assertEquals(true,
                HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void renameDirectorySuccess() throws Exception {
        HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1",
                "test2"
        );
        assertEquals(true, renamed);
        assertEquals(true,
                HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test2"));
    }

    @Test
    public void renameFileSuccess() throws Exception {
        HandleHDFSImpl.getInstance().writeFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean renamed = HandleHDFSImpl.getInstance().renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1/text.txt",
                "test1/text2.txt"
        );
        assertEquals(true, renamed);
        assertEquals(true,
                HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void removeDirectorySuccess() throws Exception {
        HandleHDFSImpl.getInstance().createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertEquals(true, removed);
    }

    @Test
    public void removeFileSuccess() throws Exception {
        HandleHDFSImpl.getInstance().writeFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean removed = HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt");
        assertEquals(true, removed);
        assertEquals(true,
                HandleHDFSImpl.getInstance().removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

}
