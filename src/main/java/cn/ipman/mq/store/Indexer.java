package cn.ipman.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
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

    static MultiValueMap<String, Entry> indexers = new LinkedMultiValueMap<>();
    @Getter
    static Map<String, Entry> mappings = new ConcurrentHashMap<>();
    public final static String OFFSET_PLACEHOLDER = "||__offset_key__||";

    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;
        int length;
        int fileIndex;  // 新增：文件索引
    }

    public static String getOffsetKey(String topic, int offset) {
        return topic + OFFSET_PLACEHOLDER + offset;
    }

    public static void addEntry(String topic, int offset, int length, int fileIndex) {
        Entry entry = new Entry(offset, length, fileIndex);
        indexers.add(topic, entry);
        mappings.put(getOffsetKey(topic, offset), entry);
    }

    public static List<Entry> getEntries(String topic) {
        return indexers.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        return mappings.get(getOffsetKey(topic, offset));
    }

}
