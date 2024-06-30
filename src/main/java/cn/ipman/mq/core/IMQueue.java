package cn.ipman.mq.core;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * 实现了消息队列（MQ）的基础功能，针对特定主题进行消息的发送和接收。
 * <p>
 * MQ for topic
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class IMQueue {

    // 消息主题
    String topic;
    // 消息队列, 使用LinkedBlockingQueue作为消息队列的实现，它是一个无界队列，具有线程安全的特性
    private final LinkedBlockingQueue<IMMessage<?>> queue = new LinkedBlockingQueue<>();
    // 消息监听器,保存所有消息监听器，用于消息的推送
    List<IMListener<?>> listeners = new ArrayList<>();


    /**
     * 对于无界队列（如LinkedBlockingQueue，除非指定了容量），因为总是有空间可以添加元素，所以offer方法总会成功并返回true。
     *
     * @param topic 消息主题
     */
    public IMQueue(String topic) {
        this.topic = topic;
    }

    /**
     * 发送消息到消息队列。如果队列接受消息，则返回true。
     * 同时，立即向所有监听器推送该消息。
     *
     * @param message 要发送的消息对象。
     * @return 如果消息成功添加到队列，则返回true；否则返回false。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean send(IMMessage<?> message) {
        boolean offered = queue.offer(message);

        // 发送时, 通知订阅这. 这里简化了, 正常应该是推送给客户端
        listeners.forEach(listener -> listener.onMessage((IMMessage) message));
        return offered;
    }

    /**
     * 从消息队列中获取一条消息。如果在指定的超时时间内有消息可用，则返回该消息。
     *
     * @param timeout 等待消息的超时时间，单位为毫秒。
     * @param <T>     消息的类型。
     * @return 成功获取到的消息对象，如果队列为空或超时，则返回null。
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <T> IMMessage<T> poll(long timeout) {
        return (IMMessage<T>) queue.poll(timeout, TimeUnit.MILLISECONDS);
    }


    /**
     * 添加消息监听器，用于接收和处理特定类型的消息。
     *
     * @param listener 要添加的消息监听器。
     * @param <T>      监听器处理的消息类型。
     */
    public <T> void addListen(IMListener<T> listener) {
        listeners.add(listener);
    }
}
