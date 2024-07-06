package cn.ipman.mq.client;

import cn.ipman.mq.model.IMMessage;

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
        broker.subscribe(topic, this.id);
    }


    public IMMessage<T> receive(String topic) {
        return broker.receive(topic, this.id);
    }

    /**
     * 注册消息监听器。
     * 当有新消息到达时，监听器将被调用。
     *
     * @param listener 消息监听器。
     */
    public void listen(IMListener<T> listener) {

    }

}
