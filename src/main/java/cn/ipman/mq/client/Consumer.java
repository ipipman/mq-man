package cn.ipman.mq.client;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Statistical;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息消费者接口。负责订阅特定主题的消息，并提供消息的接收与确认机制。
 * <p>
 * 通过与Broker交互，实现消息的订阅、取消订阅、接收消息以及消息消费确认。
 * 同时，支持注册监听器以异步方式处理接收到的消息。
 *
 * @param <T> 消息体的类型。
 * @Author IpMan
 * @Date 2024/6/29 19:55
 */
public class Consumer<T> {

    /**
     * 使用AtomicInteger来全局唯一标识消费者
     */
    static AtomicInteger CID = new AtomicInteger(0);

    /**
     * 消费者唯一标识
     */
    String id;

    /**
     * 与之交互的Broker实例
     */
    Broker broker;

    /**
     * Consumer构造函数。
     * <p>
     * 在构造时自动分配消费者ID，并通过传入的Broker实例完成初始化。
     *
     * @param broker 与消费者交互的Broker实例。
     */
    public Consumer(Broker broker) {
        this.broker = broker;
        this.id = "CID" + CID.getAndIncrement();
    }

    /**
     * 订阅指定主题的消息。
     * <p>
     * 通过调用Broker的subscribe方法，使得消费者可以开始接收指定主题的消息。
     *
     * @param topic 需要订阅的消息主题。
     */
    public void subscribe(String topic) {
        broker.subscribe(topic, this.id);
    }


    /**
     * 取消订阅指定主题的消息。
     * <p>
     * 通过调用Broker的unSubscribe方法，使得消费者不再接收指定主题的消息。
     *
     * @param topic 需要取消订阅的消息主题。
     */
    public void unSubscribe(String topic) {
        broker.unSubscribe(topic, this.id);
    }

    /**
     * 接收指定主题的最新消息。
     * <p>
     * 通过调用Broker的receive方法，消费者可以从指定主题中获取最新的消息。
     * 注意：此方法为同步方法，会阻塞直到有新消息可用或超时。
     *
     * @param topic 指定的消息主题。
     * @return 返回接收到的最新消息。
     */
    @SuppressWarnings("all")
    public Message<T> receive(String topic) {
        Message<T> receive = broker.receive(topic, this.id);
        return receive;
    }

    /**
     * 确认消息消费。
     * <p>
     * 消费者通过调用此方法，确认已处理特定偏移量的消息，以便Broker可以安全地删除这些消息。
     *
     * @param topic  消息所属的主题。
     * @param offset 已处理消息的偏移量。
     * @return 返回确认操作是否成功的布尔值。
     */
    public boolean ack(String topic, int offset) {
        return broker.ack(topic, this.id, offset);
    }

    /**
     * 基于消息对象确认消费。
     * <p>
     * 通过消息头中的偏移量字段，确认已处理特定消息，实现消息消费的确认。
     *
     * @param topic  消息所属的主题。
     * @param message 已处理的消息对象。
     * @return 返回确认操作是否成功的布尔值。
     */
    public boolean ack(String topic, Message<?> message) {
        // 从header里获取offset, 实际是在send时存到了Entry里
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    /**
     * 消息监听器，用于异步处理接收到的消息
     */
    public Listener<?> listener;

    /**
     * 注册消息监听器。
     * <p>
     * 通过注册监听器，消费者可以以异步方式处理接收到的消息。
     *
     * @param topic     监听的消息主题。
     * @param listener 用于处理消息的监听器实例。
     */
    public void listen(String topic, Listener<?> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    /**
     * 获取指定主题的消息统计信息。
     * <p>
     * 通过调用Broker的statistical方法，消费者可以获取指定主题的消息收发统计信息。
     *
     * @param topic 指定的消息主题。
     * @return 返回包含消息统计信息的对象。
     */
    public Statistical statistical(String topic) {
        return broker.statistical(topic, this.id);
    }

}
