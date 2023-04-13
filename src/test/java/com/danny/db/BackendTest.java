package com.danny.db;

import com.danny.db.util.RandomHashGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BackendTest {
    private Backend backend;

    private static int coreNumber = Runtime.getRuntime().availableProcessors();
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreNumber, coreNumber * 2, 360, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    @BeforeAll
    void init() throws Exception {
        String path = System.getProperty("user.dir");
        Files.deleteIfExists(Paths.get(path + "/data/danny.data"));
        this.backend = new Backend(path + "/data");
    }

    @Test
    void putAndGet() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value = "danny".getBytes();
        backend.put(key, value);
        String result = new String(backend.get(key));
        System.out.println(result);
        Assertions.assertEquals("danny", result);
    }

    @Test
    void delete() throws Exception {
        byte[] key = "delKey".getBytes();
        byte[] value = "delValue".getBytes();
        backend.put(key, value);
        value = backend.get(key);
        Assertions.assertEquals("delValue", new String(value));
        backend.delete(key, value);
        value = backend.get(key);
        Assertions.assertNull(value);
    }

    @Test
    void merge() throws Exception {
        putAndGet();
        delete();
        backend.merge();
    }

    @Test
    void pressureTest() throws NoSuchAlgorithmException {
        String key = "";
        String value = "";
        int size = 1000000;
        List<byte[][]> datas = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            key = RandomHashGenerator.getRandomHash();
            value = RandomHashGenerator.getRandomHash();
            byte[][] data = {key.getBytes(), value.getBytes()};
            datas.add(data);
        }
        AtomicLong dataByteSize = new AtomicLong();
        datas.stream().forEach(data -> dataByteSize.addAndGet(data[0].length + data[1].length + Entry.ENTRY_HEADER_SIZE));
        System.out.println("Data Size: " + dataByteSize.get() / 1024 / 1024 + " MB");

        long startTime = System.currentTimeMillis();
        backend.putBatch(datas);
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        System.out.println("Insert "+ size +" Records Time elapsed: " + timeElapsed + " ms");

        startTime = System.currentTimeMillis();
        for (int i = 0; i < datas.size(); i++) {
            int finalI = i;
            threadPoolExecutor.submit(() -> {
                try {
                    byte[] result = backend.get(datas.get(finalI)[0]);
                    Assertions.assertEquals(new String(datas.get(finalI)[1]), new String(result));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        endTime = System.currentTimeMillis();
        timeElapsed = endTime - startTime;
        System.out.println("Got "+ size + " Records Time elapsed: " + timeElapsed + " ms");
    }
}