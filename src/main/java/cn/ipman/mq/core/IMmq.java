package cn.ipman.mq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

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

    public IMmq(String topic) {
        this.topic = topic;
    }

    public boolean send(IMMessage<?> message) {
        return queue.offer(message);
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
}
