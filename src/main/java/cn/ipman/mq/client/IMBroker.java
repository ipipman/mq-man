package cn.ipman.mq.client;

import cn.ipman.mq.model.IMMessage;

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

    public static String brokerUrl = "localhost:8765/mq";

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

    public boolean send(String topic, IMMessage<?> message) {
    }

    public <T> IMMessage<T> receive(String topic, String id) {
    }

    public void subscribe(String topic, String id) {
    }
}
