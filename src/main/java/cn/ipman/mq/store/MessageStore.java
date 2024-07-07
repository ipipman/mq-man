package cn.ipman.mq.store;

import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
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
import java.util.Map;

/**
 * 消息存储类，用于存储和检索消息。
 * 使用MappedByteBuffer来映射文件到内存，以提高读写效率。
 */
public class MessageStore {

    String topic;
    public static final int LEN = 1024 * 10;  // 100KB 每个文件的大小
    MappedByteBuffer mappedByteBuffer = null;
    FileChannel channel = null;
    int currentFileIndex = 0;
    int currentOffset = 0;
    Map<Integer, MappedByteBuffer> fileBuffers = new HashMap<>();

    public MessageStore(String topic) {
        this.topic = topic;
    }

    @SneakyThrows
    public void init() {
        File dir = new File(this.topic);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File[] files = dir.listFiles((d, name) -> name.endsWith(".dat"));
        if (files != null) {
            for (File file : files) {
                int fileIndex = Integer.parseInt(file.getName().replace(".dat", ""));
                loadFile(fileIndex);
            }
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
        int result = offset + entry.getFileIndex() * LEN;
        System.out.println("************::" + result);
        return result;
    }

    @SneakyThrows
    private void loadFile(int fileIndex) {
        File file = new File(this.topic + File.separator + fileIndex + ".dat");
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
        readOnlyBuffer.clear();
    }

    @SneakyThrows
    private void openFile(int fileIndex) {
        File file = new File(this.topic + File.separator + fileIndex + ".dat");
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

    public int write(Message<String> message) {
        String json = JSON.toJSONString(message);
        int len = json.getBytes(StandardCharsets.UTF_8).length;
        String format = String.format("%010d", len);
        String msg = format + json;
        len = len + 10;

        if (mappedByteBuffer.remaining() < len) {
            System.out.println("写满了......");
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            openFile(++currentFileIndex);
        }

        int position = mappedByteBuffer.position();
        Indexer.addEntry(this.topic, position + currentFileIndex * LEN, len, currentFileIndex);
        mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
        currentOffset = mappedByteBuffer.position();
        return position + currentFileIndex * LEN;
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
