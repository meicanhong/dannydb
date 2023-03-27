package com.danny.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DataFile {
    public static final String FileName = "bitcask.data";
    public static final String MergeFileName = "bitcask.data.merge";

    private RandomAccessFile file;
    private String absolutePath;
    private long offset;

    private DataFile(RandomAccessFile file, String absolutePath, long offset) {
        this.file = file;
        this.absolutePath = absolutePath;
        this.offset = offset;
    }

    public long getOffset() {
        return this.offset;
    }

    public String getAbsolutePath() {
        return this.absolutePath;
    }

    public RandomAccessFile getFile() {
        return this.file;
    }

    public static DataFile createDataFile(String path) throws IOException {
        String fileName = path + File.separator + FileName;
        DataFile dataFile = createFile(fileName);
        return dataFile;
    }

    public static DataFile createMergeFile(String path) throws IOException {
        String fileName = path + File.separator + MergeFileName;
        DataFile dataFile = createFile(fileName);
        return dataFile;
    }

    private static DataFile createFile(String fileName) throws IOException {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        long offset = file.length();
        return new DataFile(file, fileName, offset);
    }

    /**
     * 该方法是从 Datafile 中读取一个 Entry，并解析返回 Entry 对象
     * 首先读取 Entry 的元数据: KeySize, ValueSize, Mark，并通过元数据 decode 一个 Entry 实例
     * 然后判断 KeySize、ValueSize > 0，进行赋值填充 Entry 实例操作
     * 最终返回解析好的 Entry 实例
     * @param offset
     * @return
     * @throws IOException
     */
    public Entry read(long offset) throws IOException {
        byte[] bytes = new byte[Entry.ENTRY_HEADER_SIZE];
        file.seek(offset);
        file.readFully(bytes);
        Entry entry = Entry.decode(bytes);

        offset += Entry.ENTRY_HEADER_SIZE;
        if (entry.getKeySize() > 0) {
            byte[] key = new byte[entry.getKeySize()];
            file.seek(offset);
            file.readFully(key);
            entry.setKey(key);
        }

        offset += entry.getKeySize();
        if (entry.getValueSize() > 0) {
            byte[] value = new byte[entry.getValueSize()];
            file.seek(offset);
            file.readFully(value);
            entry.setValue(value);
        }

        return entry;
    }

    public void write(Entry entry) throws IOException {
        byte[] encode = entry.encode();
        file.seek(offset);
        file.write(encode);
        offset += entry.getSize();
    }

    public void close() throws IOException {
        file.close();
    }
}
