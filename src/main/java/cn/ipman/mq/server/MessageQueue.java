package cn.ipman.mq.server;

import cn.ipman.mq.model.IMMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * queues.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:21
 */
public class MessageQueue {

    // 所有队列, topic为key, Queue为值, 多个consumer可以订阅同一个topic
    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "cn.ipman.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("im.order", new MessageQueue("im.order"));
    }

    // 记录客户端订阅关系, 记录consumerID的消费关系,如消费哪些topic, 以及在topic的消费位置offset
    Map<String, MessageSubscription> subscriptions = new HashMap<>();

    String topic;
    IMMessage<?>[] queue = new IMMessage[1024 * 10];  // 用来存储message
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

    private void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> sub: subscription = " + subscription);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: subscription = " + subscription);
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, IMMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println(" ===>> send: topic/message = " + topic + "/" + message);
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
            IMMessage<?> receive = messageQueue.receive(idx + 1); // 拿到的消息应该是, 在offset基础上+1
            System.out.println(" ===>> receive: topic/cid/idx = " + topic + "/" + consumerId + "/" + idx);
            System.out.println(" ===>> receive: message = " + receive);
            return receive;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId" + topic + "/" + consumerId);
    }


    // 如果消费消息,不传消费message的index时,需要手动ack确定并修改consumer的offset
    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            if (offset > subscription.getOffset() && offset <= messageQueue.index) {
                System.out.println(" ===>> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId" + topic + "/" + consumerId);
    }


}
