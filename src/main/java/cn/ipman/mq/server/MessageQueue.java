package cn.ipman.mq.server;

import cn.ipman.mq.model.IMMessage;

/**
 * queues.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:21
 */
public class MessageQueue {

    String topic;
    IMMessage<?>[] queue = new IMMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(IMMessage<?> message) {
        if (index >= queue.length) { // 写满了~
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public IMMessage<?> receive(int idx) {
        if (idx <= index) return queue[idx]; // 按位置拿数据
        return null;
    }
    
}
