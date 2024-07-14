package cn.ipman.mq.server.store;

import cn.ipman.mq.metadata.model.Message;
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

/**
 * 消息存储类，用于存储和检索消息。
 * 使用MappedByteBuffer来映射文件到内存，以提高读写效率。
 */
public class MessageStore {

    String topic;
    public static final int LEN = 1024 * 10;  // 10KB 每个文件的大小
    MappedByteBuffer mappedByteBuffer = null;
    FileChannel channel = null;
    @Getter
    int currentFileIndex = 0;
    int currentOffset = 0;
    Map<Integer, MappedByteBuffer> fileBuffers = new HashMap<>();
    public final static String STORE_DIR = "storage/";
    public final static String STORE_FILE_FORMAT = ".dat";

    /**
     * 构造函数，初始化消息存储时需要指定主题。
     *
     * @param topic 消息主题，用于消息的分类存储。
     */
    public MessageStore(String topic) {
        this.topic = topic;
    }

    /**
     * 初始化消息存储，包括创建存储目录、加载已有消息文件等。
     *
     * @throws IOException 如果操作文件时发生错误。
     */
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

    /**
     * 根据当前offset和entry计算下一个offset。
     * 用于消息消费时确定下一个要消费的消息位置。
     *
     * @param offset 当前offset。
     * @param entry  消息索引条目。
     * @return 下一个offset。
     */
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


    /**
     * 加载指定文件到内存映射缓冲区。
     *
     * @param fileIndex     文件索引。
     * @param lastFileIndex 最后一个文件索引，用于判断是否需要初始化文件段信息。
     * @throws IOException 如果操作文件时发生错误。
     */
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
        if (fileIndex != lastFileIndex) {
            int maxOffset = (readOnlyBuffer.position() - 10) + fileIndex * LEN;
            Indexer.addFileSegments(this.topic, fileIndex, maxOffset);
            System.out.println("init load file topic/index/maxOffset => " + this.topic
                    + fileIndex + "/" + maxOffset);
        }
        readOnlyBuffer.clear();
    }

    /**
     * 打开（创建并映射）指定文件索引的文件。
     * 用于存储新消息或当需要访问新文件时。
     *
     * @param fileIndex 文件索引。
     * @throws IOException 如果操作文件时发生错误。
     */
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

    /**
     * 存储消息到消息存储。
     * 如果当前缓冲区满，则创建新文件继续存储。
     *
     * @param message 待存储的消息。
     * @return 消息的存储位置。
     */
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

    /**
     * 获取当前topic最新offset
     *
     * @return 当前topic最新offset。
     */
    public int pos() {
        return currentOffset + currentFileIndex * LEN;
    }

    /**
     * 根据offset读取消息。
     *
     * @param offset 消息的存储位置。
     * @return 读取到的消息对象。
     */
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

    /**
     * 获取存储的主题下所有消息的总数。
     *
     * @return 消息总数。
     */
    public int total() {
        return Indexer.getEntries(topic) != null ? Indexer.getEntries(topic).size() : 0;
    }
}