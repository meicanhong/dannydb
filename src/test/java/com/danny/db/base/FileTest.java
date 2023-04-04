package com.danny.db.base;

import com.danny.db.Backend;
import com.danny.db.RandomHashGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileTest {
    private String fileName;

    @BeforeAll
    void init() throws Exception {
        String projectPath = System.getProperty("user.dir");
        this.fileName = projectPath + "/data/test.data";
        Files.deleteIfExists(Paths.get(this.fileName));
        String dirPath = projectPath + "/data";
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    @Test
    void write() throws Exception {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        FileChannel channel = file.getChannel();
        for (int i = 0; i < 10000; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            buffer.putInt(i);
            buffer.putInt(i);
            buffer.position(0);
            channel.write(buffer);
        }
        channel.close();
    }

}
