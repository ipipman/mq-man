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
@AllArgsConstructor
public class IMMq {

    private String topic;
    private LinkedBlockingQueue<IMMessage> queue = new LinkedBlockingQueue<>();

    public IMMq(String topic) {
        this.topic = topic;
    }

    public boolean send(IMMessage message) {
        return queue.offer(message);
    }

    /**
     * 拉模式获取消息
     */
    @SneakyThrows
    public <T> IMMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }
}
