package cn.ipman.mq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息代理类，负责管理消息队列并提供生产者与消费者创建方法。
 * 实现了基于主题的消息代理功能，允许创建及查找消息队列。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class IMBroker {

    /**
     * 使用并发哈希映射存储主题名到消息队列的映射，确保线程安全及高并发访问。
     */
    Map<String, IMQueue> mqMapping = new ConcurrentHashMap<>(64);

    /**
     * 根据主题查找已存在的消息队列。
     *
     * @param topic 主题名称
     * @return 对应的消息队列，若不存在则返回null
     */
    public IMQueue find(String topic) {
        return mqMapping.get(topic);
    }

    /**
     * 为指定主题创建新的消息队列，如该队列尚不存在。
     *
     * @param topic 主题名称
     * @return 新创建的或已存在的消息队列
     */
    public IMQueue createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new IMQueue(topic));
    }

    /**
     * 创建一个新的生产者实例。
     *
     * @return 新的IMProducer实例
     */
    public IMProducer createProducer() {
        return new IMProducer(this);
    }

    /**
     * 创建一个新的消费者实例并订阅指定主题。
     *
     * @param topic 订阅的主题
     * @return 新的IMConsumer实例
     */
    public IMConsumer<?> createConsumer(String topic) {
        IMConsumer<?> consumer = new IMConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

}
