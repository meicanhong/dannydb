package com.danny.db.base;

import com.danny.db.Backend;
import com.danny.db.RandomHashGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileTest {
    private RandomAccessFile file;
    private FileChannel channel;
    private String fileName;
    private static int coreNumber = Runtime.getRuntime().availableProcessors();
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreNumber * 2, coreNumber * 2, 360, TimeUnit.SECONDS, new LinkedBlockingQueue<>());


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
        this.file = new RandomAccessFile(fileName, "rw");
        this.channel = file.getChannel();
    }

    @Test
    void write() throws Exception {
        for (int i = 0; i < 10000; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            buffer.putInt(i);
            buffer.putInt(i);
            buffer.position(0);
            channel.write(buffer);
        }
        channel.close();
    }

    @Test
    void concurrentWrite() throws Exception {
        int size = 100;
        CountDownLatch countDownLatch = new CountDownLatch(size);

        for (int i = 0; i < size; i++) {
            int finalI = i;
            threadPoolExecutor.submit(() -> {
                String value = finalI + "\n";
                ByteBuffer buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
                try {
                    channel.write(buffer);
                    System.out.println(channel.position());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
    }
}
