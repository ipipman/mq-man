package cn.ipman.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * entity indexer. 文件索引
 *
 * @Author IpMan
 * @Date 2024/7/6 20:37
 */
public class Indexer {

    // 全局的
    static MultiValueMap<String, Entry> indexers = new LinkedMultiValueMap<>();
    static Map<Integer, Entry> mappings = new HashMap<>(); // 根据offset索引映射

    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;  // 偏移量
        int length;  // 消息的长度
    }

    public static void addEntry(String topic, int offset, int length) {
        // 按topic创建, 一个topic创建一次
        Entry entry = new Entry(offset, length);
        indexers.add(topic, entry);
        mappings.put(offset, entry);
    }

    public static List<Entry> getEntries(String topic) {
        return indexers.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        return mappings.get(offset);
    }

}
