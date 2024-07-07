package cn.ipman.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引器类，用于存储和检索消息的偏移量和长度信息。
 * 提供了根据主题和偏移量添加入口、根据主题获取所有入口、根据主题和偏移量获取特定入口的方法。
 *
 * @Author IpMan
 * @Date 2024/7/6 20:37
 */
public class Indexer {

    // 存储主题与入口（偏移量和长度）的映射，使用LinkedMultiValueMap保持插入顺序
    static MultiValueMap<String, Entry> indexers = new LinkedMultiValueMap<>();

    // 根据偏移量键映射到具体的入口，提高检索效率
    static Map<String, Entry> mappings = new HashMap<>(); // 根据offset索引映射

    // 偏移量的占位符，用于构造唯一的偏移量键
    public final static String OFFSET_PLACEHOLDER = "||__offset_key__||";

    /**
     * 入口类，包含消息的偏移量和长度信息。
     */
    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;  // 偏移量
        int length;  // 消息的长度
    }

    /**
     * 根据主题和偏移量构造偏移量键。
     *
     * @param topic  消息主题
     * @param offset 消息偏移量
     * @return 构造的偏移量键
     */
    public static String getOffsetKey(String topic, int offset) {
        return topic + OFFSET_PLACEHOLDER + offset;
    }


    /**
     * 添加入口到索引器中。
     *
     * @param topic  消息主题
     * @param offset 消息偏移量
     * @param length 消息长度
     */
    public static void addEntry(String topic, int offset, int length) {
        // 按topic创建, 一个topic创建一次
        Entry entry = new Entry(offset, length);
        indexers.add(topic, entry);
        // 在topic下, 按offset添加入口
        System.out.println("add offset entry , key = " + getOffsetKey(topic, offset));
        mappings.put(getOffsetKey(topic, offset), entry);
    }

    /**
     * 根据主题获取所有入口。
     *
     * @param topic 消息主题
     * @return 主题对应的入口列表
     */
    public static List<Entry> getEntries(String topic) {
        return indexers.get(topic);
    }

    /**
     * 根据主题和偏移量获取特定的入口。
     *
     * @param topic  消息主题
     * @param offset 消息偏移量
     * @return 对应的入口，如果不存在则返回null
     */
    public static Entry getEntry(String topic, int offset) {
        System.out.println("get offset entry , key = " + getOffsetKey(topic, offset));
        return mappings.get(getOffsetKey(topic, offset));
    }

}
