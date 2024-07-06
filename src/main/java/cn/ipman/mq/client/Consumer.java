package cn.ipman.mq.client;

import cn.ipman.mq.model.Message;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息消费者接口。
 * 用于订阅并消费特定主题的消息。
 *
 * @param <T> 消息体的类型。
 * @Author IpMan
 * @Date 2024/6/29 19:55
 */
public class Consumer<T> {

    static AtomicInteger CID = new AtomicInteger(0);

    String id;

    /**
     * 消息中间件代理对象。
     * 用于与消息中间件进行交互。
     */
    Broker broker;

    /**
     * 构造函数，初始化消息消费者。
     *
     * @param broker 消息中间件代理对象。
     */
    public Consumer(Broker broker) {
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
        broker.subscribe(topic, this.id);
    }

    public void unSubscribe(String topic) {
        broker.unSubscribe(topic, this.id);
    }

    @SuppressWarnings("all")
    public Message<T> receive(String topic) {
        Message<T> receive = broker.receive(topic, this.id);
        return receive;
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, this.id, offset);
    }

    public boolean ack(String topic, Message<?> message) {
        // 从header里获取offset, 实际是在send时存到了Entry里
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }


    public Listener<?> listener;

    /**
     * 注册消息监听器。
     * 当有新消息到达时，监听器将被调用。
     *
     * @param listener 消息监听器。
     */
    public void listen(String topic, Listener<?> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

}
