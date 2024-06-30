package cn.ipman.mq.server;

import cn.ipman.mq.model.IMMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * queues.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:21
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    private static final String TEST_TOPIC = "cn.ipman.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue("cn.ipman.test"));
    }

    // 记录客户端订阅
    Map<String, MessageSubscription> subscriptions = new HashMap<>();
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

    public void subscribe(MessageSubscription subscription) {
        // 添加订阅关系
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static int send(String topic, String consumerId, IMMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }


    public static IMMessage<?> receive(String topic, String consumerId, int idx) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.receive(idx);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId" + topic + "/" + consumerId);
    }

    // 使用此方法，需要手动调用ack, 更新订阅关系的offset
    public static IMMessage<?> receive(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int idx = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.receive(idx);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId" + topic + "/" + consumerId);
    }
}
