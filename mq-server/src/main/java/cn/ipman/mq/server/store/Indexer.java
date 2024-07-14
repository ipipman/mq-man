package cn.ipman.mq.server.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 索引器类，用于存储和检索消息的偏移量和长度信息。
 * 提供了根据主题和偏移量添加入口、根据主题获取所有入口、根据主题和偏移量获取特定入口的方法。
 *
 * @Author IpMan
 * @Date 2024/7/6 20:37
 */
public class Indexer {

    // 存储主题与多个入口（偏移量、长度）的映射，用于快速检索。
    static MultiValueMap<String, Entry> indexers = new LinkedMultiValueMap<>();
    // 存储主题、偏移量与入口的映射，用于快速通过偏移量定位入口。
    @Getter
    static Map<String, Entry> mappings = new ConcurrentHashMap<>();
    // 存储主题与多个文件段的映射，每个文件段包含文件索引和最大偏移量。
    static MultiValueMap<String, FileSegment> fileSegments = new LinkedMultiValueMap<>();
    // 偏移量占位符，用于构造唯一的键值对，方便检索。
    public final static String OFFSET_PLACEHOLDER = "||__offset_key__||";

    /**
     * 入口类，包含消息在文件中的偏移量、长度和文件索引。
     */
    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;
        int length;
        // 文件索引，用于定位消息在哪个文件中。
        int fileIndex;
    }

    /**
     * 文件段类，包含文件索引和该文件段的最大偏移量。
     */
    @AllArgsConstructor
    @Data
    public static class FileSegment {
        int fileIndex;
        int maxOffset;
    }

    /**
     * 根据主题和偏移量生成唯一的键值，用于存储和检索入口。
     *
     * @param topic 消息主题。
     * @param offset 消息偏移量。
     * @return 唯一的键值。
     */
    public static String getOffsetKey(String topic, int offset) {
        return topic + OFFSET_PLACEHOLDER + offset;
    }

    /**
     * 添加入口到索引器中，同时在映射中记录偏移量到入口的映射。
     *
     * @param topic 消息主题。
     * @param offset 消息偏移量。
     * @param length 消息长度。
     * @param fileIndex 消息所在的文件索引。
     */
    public static void addEntry(String topic, int offset, int length, int fileIndex) {
        Entry entry = new Entry(offset, length, fileIndex);
        indexers.add(topic, entry);
        mappings.put(getOffsetKey(topic, offset), entry);
    }

    /**
     * 添加文件段到索引器中。
     *
     * @param topic 消息主题。
     * @param fileIndex 文件索引。
     * @param maxPosition 该文件段的最大偏移量。
     */
    public static void addFileSegments(String topic, int fileIndex, int maxPosition) {
        fileSegments.add(topic, new FileSegment(fileIndex, maxPosition));
    }

    /**
     * 根据主题和文件索引获取对应的文件段。
     *
     * @param topic 消息主题。
     * @param fileIndex 文件索引。
     * @return 对应的文件段，如果不存在则返回null。
     */
    public static FileSegment getFileSegment(String topic, int fileIndex) {
        if (fileSegments == null || !fileSegments.containsKey(topic)){
            return null;
        }
        for (FileSegment fileSegment : fileSegments.get(topic)) {
            if (fileSegment.getFileIndex() == fileIndex) {
                return fileSegment;
            }
        }
        return null;
    }

    /**
     * 根据主题获取所有入口。
     *
     * @param topic 消息主题。
     * @return 主题对应的入口列表。
     */
    public static List<Entry> getEntries(String topic) {
        return indexers.get(topic);
    }

    /**
     * 根据主题和偏移量获取特定的入口。
     *
     * @param topic 消息主题。
     * @param offset 消息偏移量。
     * @return 对应的入口，如果不存在则返回null。
     */
    public static Entry getEntry(String topic, int offset) {
        return mappings.get(getOffsetKey(topic, offset));
    }

}