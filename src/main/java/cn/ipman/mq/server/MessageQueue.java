package cn.ipman.mq.server;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Subscription;
import cn.ipman.mq.store.Indexer;
import cn.ipman.mq.store.MessageStore;

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

    // 所有队列, topic为key, Queue为值, 多个consumer可以订阅同一个topic
    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "cn.ipman.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("im.order", new MessageQueue("im.order"));
    }

    // 记录客户端订阅关系, 记录consumerID的消费关系,如消费哪些topic, 以及在topic的消费位置offset
    Map<String, Subscription> subscriptions = new HashMap<>();

    String topic;

    MessageStore store;

    public MessageQueue(String topic) {
        this.topic = topic;
        store = new MessageStore(topic);
        store.init();
    }

    public static List<Message<?>> batchReceive(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {

            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                if (entry == null) return null;
                nextOffset = offset + entry.getLength();
            }

            List<Message<?>> result = new ArrayList<>();
            Message<?> receive = messageQueue.receive(nextOffset);
            // 如果能拿到数据, 并且不超过batch size的限制时
            while (receive != null) {
                result.add(receive);
                if (result.size() >= size) break;

                offset = Integer.parseInt(receive.getHeaders().get("X-offset"));
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                if (entry == null) break;
                nextOffset = offset + entry.getLength();
                receive = messageQueue.receive(nextOffset);
            }
            System.out.println(" ===>> batch: topic/cid/size = " + topic + "/" + consumerId + "/" + offset + "/" + result.size());
            System.out.println(" ===>> batch: last message = " + receive);
            return result;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public int send(Message<String> message) {
        int offset = store.pos(); // 文件的位置
        message.getHeaders().put("X-offset", String.valueOf(offset)); // 设置偏移量
        store.write(message);
        return offset;
    }

    public Message<?> receive(int offset) {
        return store.read(offset);
    }

    public void subscribe(Subscription subscription) {
        // 添加订阅关系
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    private void unsubscribe(Subscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(Subscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> sub: subscription = " + subscription);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(Subscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: subscription = " + subscription);
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, Message<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println(" ===>> send: topic/message = " + topic + "/" + message);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static Message<?> receive(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.receive(offset);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    // 使用此方法，需要手动调用ack, 更新订阅关系的offset
    public static Message<?> receive(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {

            // 这个offset来源于客户端ack
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                if (entry == null) return null;
                nextOffset = offset + entry.getLength();
            }

            // 拿到偏移量,再获取数据
            Message<?> receive = messageQueue.receive(nextOffset); // 拿到的消息应该是, 在offset基础上+1
            System.out.println(" ===>> receive: topic/cid/idx = " + topic + "/" + consumerId + "/" + offset);
            System.out.println(" ===>> receive: message = " + receive);
            return receive;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }


    // 如果消费消息,不传消费message的index时,需要手动ack确定并修改consumer的offset
    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        if (messageQueue.subscriptions.containsKey(consumerId)) {
            Subscription subscription = messageQueue.subscriptions.get(consumerId);

            if (offset > subscription.getOffset() && offset < MessageStore.LEN) {
                System.out.println(" ===>> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }


}
