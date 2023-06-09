package com.danny.db;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class Backend {
    private static int coreNumber = Runtime.getRuntime().availableProcessors();
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreNumber, coreNumber * 2, 360, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    private Action action;

    public Backend(String dirPath) throws IOException {
        this.action = new Action(dirPath);
    }

    public byte[] get(byte[] key) throws IOException {
        return action.get(key);
    }

    public void putBatch(List<byte[][]> datas) {
        if (datas == null) return;
        for (byte[][] data : datas) {
            threadPoolExecutor.submit(() -> {
                try {
                    action.put(data[0], data[1]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public void put(byte[] key, byte[] value) throws ExecutionException, InterruptedException {
        threadPoolExecutor.submit(() -> {
            try {
                action.put(key, value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).get();
    }
    public void delete(byte[] key, byte[] value) throws ExecutionException, InterruptedException {
        threadPoolExecutor.submit(() -> {
            try {
                action.delete(key, value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).get();
    }

    public void merge() throws ExecutionException, InterruptedException {
        threadPoolExecutor.submit(() -> {
            try {
                action.merge();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).get();
    }

    public void close() {
        threadPoolExecutor.shutdown();
    }

}
