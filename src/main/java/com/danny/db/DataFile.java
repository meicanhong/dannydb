package com.danny.db;

import com.danny.db.util.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DataFile {
    public static final String FileName = "danny.data";
    public static final String MergeFileName = "danny.data.merge";

    private RandomAccessFile file;
    private FileChannel channel;
    private String absolutePath;
    private AtomicLong offset;

    private ReentrantLock lock = new ReentrantLock();

    private DataFile(RandomAccessFile file, String absolutePath, long offset) {
        this.file = file;
        this.absolutePath = absolutePath;
        this.offset = new AtomicLong(offset);
        this.channel = file.getChannel();
    }

    public long getOffset() {
        return this.offset.get();
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
        if (channel.size() <= offset) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Entry.ENTRY_HEADER_SIZE);
        channel.position(offset);
        channel.read(buffer);
        buffer.flip();
        Entry entry = Entry.decode(buffer.array());

        if (entry.getKeySize() > 0) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(entry.getKeySize());
            channel.read(keyBuffer);
            keyBuffer.flip();
            byte[] key = CompressionUtil.decompress(keyBuffer.array());
            entry.setKey(key);
        }

        if (entry.getValueSize() > 0) {
            ByteBuffer valueBuffer = ByteBuffer.allocate(entry.getValueSize());
            channel.read(valueBuffer);
            valueBuffer.flip();
            byte[] value = CompressionUtil.decompress(valueBuffer.array());
            entry.setValue(value);
        }
        return entry;
    }

    public long write(Entry entry) throws IOException {
        ByteBuffer encode = entry.encode();
        lock.lock();
        channel.position(offset.get());
        channel.write(encode);
        long offset = this.offset.getAndAdd(entry.getDiskSize());
        lock.unlock();
        return offset;
    }

    public void close() throws IOException {
        channel.force(true);
        file.close();
    }
}
