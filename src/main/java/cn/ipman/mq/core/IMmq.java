package cn.ipman.mq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MQ for topic
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class IMmq {

    String topic;
    // 消息队列
    private final LinkedBlockingQueue<IMMessage<?>> queue = new LinkedBlockingQueue<>();
    // 消息监听器
    List<IMListener<?>> listeners = new ArrayList<>();

    /**
     * 对于无界队列（如LinkedBlockingQueue，除非指定了容量），因为总是有空间可以添加元素，所以offer方法总会成功并返回true。
     *
     * @param topic 消息主题
     */
    public IMmq(String topic) {
        this.topic = topic;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean send(IMMessage<?> message) {
        boolean offered = queue.offer(message);

        // 简化下, 正常应该是推送给客户端
        listeners.forEach(listener -> listener.onMessage((IMMessage) message));
        return offered;
    }

    /**
     * 拉模式获取消息
     *
     * @param timeout timeout：表示等待的时间长度，单位是毫秒。在这个时间内，如果队列中有元素可取，则此方法将立即返回
     * @param <T>     消息pojo
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <T> IMMessage<T> poll(long timeout) {
        return (IMMessage<T>) queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListen(IMListener<T> listener) {
        listeners.add(listener);
    }
}
