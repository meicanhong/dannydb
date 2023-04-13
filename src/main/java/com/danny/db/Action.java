package com.danny.db;

import com.danny.db.util.ByteArrayWrapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Action {
    private ConcurrentHashMap<ByteArrayWrapper, Long> indexes;
    private DataFile dataFile;
    private String dirPath;

    public Action(String dirPath) throws IOException {
        Objects.requireNonNull(dirPath, "dirPath cannot be null");
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        this.dataFile = DataFile.createDataFile(dirPath);
        this.indexes = new ConcurrentHashMap<>();
        this.dirPath = dirPath;

        loadIndexesFromFile();
    }

    private void loadIndexesFromFile() {
        if (dataFile == null) {
            return;
        }

        long offset =0;
        while (true) {
            try {
                Entry entry = dataFile.read(offset);
                if (entry == null) break;
                if (entry.getMark() != Entry.DEL) {
                    indexes.put(new ByteArrayWrapper(entry.getKey()), offset);
                }
                offset += entry.getDiskSize();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public byte[] get(byte[] key) throws IOException {
        Long offset = indexes.get(new ByteArrayWrapper(key));
        if (offset == null) {
            return null;
        }
        Entry entry = dataFile.read(offset);
        if (entry == null) {
            throw new RuntimeException("entry is null for offset: " + offset);
        }
        if (entry.getMark() == Entry.DEL) {
            return null;
        }
        if (entry.getValue() == null) {
            return null;
        }
        return entry.getValue();
    }

    public void put(byte[] key, byte[] value) throws IOException {
        Entry entry = new Entry(key, value, Entry.PUT);
        long offset = dataFile.write(entry);
        indexes.put(new ByteArrayWrapper(key), offset);
    }

    public void delete(byte[] key, byte[] value) throws IOException {
        Entry entry = new Entry(key, value, Entry.DEL);
        long offset = dataFile.write(entry);
        indexes.put(new ByteArrayWrapper(key), offset);
    }

    public void merge() throws IOException {
        if (dataFile.getOffset() == 0) {
            return;
        }

        /**
         * 内存中的索引是最新的，找到磁盘中所有的 entry 的 offset
         * 与内存中的 offset 对比，不一致则为失效数据，一致则为有效数据
         */
        long offset = 0;
        Entry entry;
        List<Entry> validEntries = new ArrayList<Entry>();
        while ((entry = dataFile.read(offset)) != null) {
            Long memoryOffset = indexes.get(new ByteArrayWrapper(entry.getKey()));
            if (memoryOffset != null && memoryOffset == offset && entry.getMark() != Entry.DEL) {
                validEntries.add(entry);
            }
            offset += entry.getDiskSize();
        }

        if (validEntries.size() == 0) return;

        DataFile mergeFile = DataFile.createMergeFile(dirPath);
        for (Entry validEntry : validEntries) {
            long writeOff = mergeFile.getOffset();
            mergeFile.write(validEntry);
            indexes.put(new ByteArrayWrapper(validEntry.getKey()), writeOff);
        }

        dataFile.close();
        Files.deleteIfExists(Paths.get(dataFile.getAbsolutePath()));
        Files.move(Paths.get(mergeFile.getAbsolutePath()), Paths.get(dataFile.getAbsolutePath()));
        this.dataFile = mergeFile;
    }
}
