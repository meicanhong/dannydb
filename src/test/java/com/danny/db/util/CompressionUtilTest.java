package com.danny.db.util;

import com.danny.db.RandomHashGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompressionUtilTest {
    private RandomAccessFile compressFile;
    private RandomAccessFile decompressFile;

    @BeforeAll
    void init() throws Exception {
        String projectPath = System.getProperty("user.dir");
        String compressFileName = projectPath + "/data/compress.dat";
        String decompressFileName = projectPath + "/data/decompress.dat";
        Files.deleteIfExists(Paths.get(compressFileName));
        Files.deleteIfExists(Paths.get(decompressFileName));
        String dirPath = projectPath + "/data";
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        this.compressFile = new RandomAccessFile(compressFileName, "rw");
        this.decompressFile = new RandomAccessFile(decompressFileName, "rw");

        initTestData();
    }

    void initTestData() throws Exception {
        String value = "";
        int size = 1000000;
        FileChannel channel = compressFile.getChannel();
        for (int i = 0; i < size; i++) {
            value = RandomHashGenerator.getRandomHash();
            channel.write(ByteBuffer.wrap(value.getBytes()));
        }
        channel.position(0);
    }

    /**
     * 测试压缩和解压缩
     * @throws Exception
     */
    @Test
    void test() throws Exception {
        FileChannel compressFileChannel = compressFile.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate((int) compressFileChannel.size());
        List<byte[]> datas = new ArrayList<>();
        while (compressFileChannel.read(buffer) != -1) {
            buffer.flip();
            byte[] data = new byte[buffer.limit()];
            buffer.get(data);
            datas.add(data);
            buffer.clear();
        }

        FileChannel decompressFileChannel = decompressFile.getChannel();
        for (byte[] data : datas) {
            byte[] compress = CompressionUtil.compress(data);
            decompressFileChannel.write(ByteBuffer.wrap(compress));
            byte[] decompress = CompressionUtil.decompress(compress);
            Arrays.equals(compress, decompress);
        }

        System.out.println("压缩率: " + (double) decompressFileChannel.position() / compressFileChannel.position());
    }

}