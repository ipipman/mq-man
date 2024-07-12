package cn.ipman.mq.store;

import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 消息存储类，用于存储和检索消息。
 * 使用MappedByteBuffer来映射文件到内存，以提高读写效率。
 */
public class MessageStore {

    String topic;
    public static final int LEN = 1024 * 10;  // 100KB 每个文件的大小
    MappedByteBuffer mappedByteBuffer = null;
    FileChannel channel = null;
    @Getter
    int currentFileIndex = 0;
    int currentOffset = 0;
    Map<Integer, MappedByteBuffer> fileBuffers = new HashMap<>();
    public final static String STORE_DIR = "storage/";
    public final static String STORE_FILE_FORMAT = ".dat";

    public MessageStore(String topic) {
        this.topic = topic;
    }

    @SneakyThrows
    public void init() {
        File dir = new File(STORE_DIR + this.topic);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File[] files = dir.listFiles((d, name) -> name.endsWith(STORE_FILE_FORMAT));
        if (files != null) {
            List<Integer> fileIndexer = new LinkedList<>();
            for (File file : files) {
                int fileIndex = Integer.parseInt(file.getName().replace(STORE_FILE_FORMAT, ""));
                fileIndexer.add(fileIndex);
            }
            int lastFileIndex = fileIndexer.stream().max(Integer::compare).orElse(0);
            fileIndexer.stream().sorted().toList().forEach(fi -> {
                loadFile(fi, lastFileIndex);
            });
        }
        if (files == null || files.length == 0) {
            openFile(0);
        } else {
            currentFileIndex = files.length - 1;
            mappedByteBuffer = fileBuffers.get(currentFileIndex);
            currentOffset = mappedByteBuffer.position();
        }
    }

    public int nextOffset(int offset, Indexer.Entry entry) {

        int fileIndex = entry.getFileIndex();
        int expected = offset + entry.getLength();
        Indexer.FileSegment fileSegment = Indexer.getFileSegment(this.topic, fileIndex);
        if (fileSegment == null) {
            return expected;
        }

        if (expected >= fileSegment.getMaxOffset()) {
            System.out.println("reset offset to next file " + expected);
            int nextIndex = fileIndex + 1;
            if (fileBuffers.containsKey(nextIndex)) {
                return nextIndex * LEN;
            }
        }
        return expected;
    }


    @SneakyThrows
    private void loadFile(int fileIndex, int lastFileIndex) {
        File file = new File(STORE_DIR + this.topic + File.separator + fileIndex + STORE_FILE_FORMAT);
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);
        fileBuffers.put(fileIndex, buffer);

        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        byte[] header = new byte[10];
        readOnlyBuffer.get(header);
        int offset = 0;
        while (header[9] > 0) {
            String trim = new String(header, StandardCharsets.UTF_8).trim();
            int len = Integer.parseInt(trim) + 10;
            Indexer.addEntry(topic, offset + fileIndex * LEN, len, fileIndex);
            offset += len;
            readOnlyBuffer.position(offset);
            if (readOnlyBuffer.remaining() < 10) break;
            readOnlyBuffer.get(header);
        }

        // 初始化topic下多个文件时, 计算每个文件最大offset, 用于消费时按offset切换文件
        if (fileIndex != lastFileIndex){
            int maxOffset = (readOnlyBuffer.position() - 10) + fileIndex * LEN;
            Indexer.addFileSegments(this.topic, fileIndex, maxOffset);
            System.out.println("init load file topic/index/maxOffset => " + this.topic
                    + fileIndex + "/" + maxOffset);
        }
        readOnlyBuffer.clear();
    }

    @SneakyThrows
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void openFile(int fileIndex) {
        File file = new File(STORE_DIR + this.topic + File.separator + fileIndex + STORE_FILE_FORMAT);
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);
        fileBuffers.put(fileIndex, mappedByteBuffer);
        currentFileIndex = fileIndex;
        currentOffset = 0;
    }

    public synchronized int write(Message<String> message) {
        String json = JSON.toJSONString(message);
        int len = json.getBytes(StandardCharsets.UTF_8).length;
        len = len + 10;

        if (mappedByteBuffer.remaining() < len) {
            int maxOffset = mappedByteBuffer.position() + currentFileIndex * LEN;
            Indexer.addFileSegments(this.topic, currentFileIndex, maxOffset);
            try {
                channel.close();
            } catch (NullPointerException | IOException e) {
                e.printStackTrace();
            }
            openFile(++currentFileIndex);
        }

        // 如果文件被写满, 需要重新计算position, 从而得出最终的offset
        int position = mappedByteBuffer.position();
        int offset = position + currentFileIndex * LEN;

        // 重新计算更新offset后的message长度
        message.getHeaders().put("X-offset", String.valueOf(offset));
        json = JSON.toJSONString(message);
        len = json.getBytes(StandardCharsets.UTF_8).length;
        String format = String.format("%010d", len);
        String msg = format + json;
        len = len + 10;

        Indexer.addEntry(this.topic, offset, len, currentFileIndex);
        mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
        currentOffset = mappedByteBuffer.position();
        return offset;
    }

    public int pos() {
        return currentOffset + currentFileIndex * LEN;
    }

    public Message<String> read(int offset) {
        int fileIndex = offset / LEN;
        int localOffset = offset % LEN;

        MappedByteBuffer buffer = fileBuffers.get(fileIndex);
        if (buffer == null) {
            return null;
        }

        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(this.topic, offset);
        if (entry == null) return null;
        readOnlyBuffer.position(localOffset + 10);

        int len = entry.getLength() - 10;
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);
        String json = new String(bytes, StandardCharsets.UTF_8);

        Message<String> message = JSON.parseObject(json, new TypeReference<Message<String>>() {
        });

        readOnlyBuffer.clear();
        return message;
    }

    public int total() {
        return Indexer.getEntries(topic) != null ? Indexer.getEntries(topic).size() : 0;
    }
}
