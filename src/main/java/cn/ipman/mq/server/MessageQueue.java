package cn.ipman.mq.server;

import cn.ipman.mq.core.IMMessage;

/**
 * queues.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:21
 */
public class MessageQueue {

    private String topic;
    private IMMessage<?>[] queue = new IMMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(IMMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public IMMessage<?> receive(int idx) {
        if (idx <= index)  return queue[idx]; // 按位置拿数据
        return null;
    }


}
