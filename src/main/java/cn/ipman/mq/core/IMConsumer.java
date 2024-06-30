package cn.ipman.mq.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息消费者接口。
 * 用于订阅并消费特定主题的消息。
 *
 * @param <T> 消息体的类型。
 * @Author IpMan
 * @Date 2024/6/29 19:55
 */
public class IMConsumer<T> {

    static AtomicInteger CID = new AtomicInteger(0);

    private String id;

    /**
     * 消息中间件代理对象。
     * 用于与消息中间件进行交互。
     */
    IMBroker broker;

    /**
     * 订阅的主题。
     */
    String topic;

    /**
     * 消息队列对象。
     * 用于具体的消息消费操作。
     */
    IMQueue queue;


    /**
     * 构造函数，初始化消息消费者。
     *
     * @param broker 消息中间件代理对象。
     */
    public IMConsumer(IMBroker broker) {
        this.broker = broker;
        this.id = "CID" + CID.getAndIncrement();
    }

    /**
     * 订阅指定的主题。
     * 通过代理对象找到对应的主题消息队列。
     *
     * @param topic 要订阅的主题。
     * @throws RuntimeException 如果主题不存在，则抛出运行时异常。
     */
    public void subscribe(String topic) {
        this.topic = topic;
        this.queue = broker.find(topic);
        if (queue == null) throw new RuntimeException("topic not found");
    }

    /**
     * 从消息队列中轮询消息。
     * 如果在指定超时时间内没有消息可用，则返回null。
     *
     * @param timeout 超时时间，单位为毫秒。
     * @return 消息对象，如果队列为空则返回null。
     */
    public IMMessage<T> poll(long timeout) {
        return queue.poll(timeout);
    }

    /**
     * 注册消息监听器。
     * 当有新消息到达时，监听器将被调用。
     *
     * @param listener 消息监听器。
     */
    public void listen(IMListener<T> listener) {
        queue.addListen(listener);
    }

}
