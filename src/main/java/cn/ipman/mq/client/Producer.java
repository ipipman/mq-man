package cn.ipman.mq.client;

import cn.ipman.mq.model.Message;

/**
 * 消息队列生产者类。
 * 该类负责生产消息并发送到指定的主题中。
 * 它通过消息代理实例来实现消息的实际发送。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:41
 */
public class Producer {

    /**
     * 消息代理实例。
     * 用于发送消息到指定的主题。
     */
    Broker broker;

    /**
     * 构造方法，初始化消息队列生产者。
     *
     * @param broker 消息代理实例，用于实际发送消息。
     */
    public Producer(Broker broker) {
        this.broker = broker;
    }

    /**
     * 发送消息到指定的主题。
     *
     * @param topic 消息的主题，指定消息的目的地。
     * @param message 待发送的消息对象。
     * @return 发送是否成功的布尔值。
     */
    public boolean send(String topic, Message<?> message) {
        return broker.send(topic, message);
    }
}
