package cn.ipman.mq.server;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Statistical;
import cn.ipman.mq.model.Subscription;
import cn.ipman.mq.store.Indexer;
import cn.ipman.mq.store.MessageStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息队列管理类，负责消息的发送和接收，以及订阅关系的管理。
 */
public class MessageQueue {

    // 所有消息队列的映射，主题为键，消息队列为值。
    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "cn.ipman.test";

    static {
        // 初始化测试主题和订单主题的消息队列。
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("im.order", new MessageQueue("im.order"));
    }

    // 订阅关系映射，消费者ID为键，订阅信息为值
    Map<String, Subscription> subscriptions = new HashMap<>();

    // 消息队列的主题
    String topic;

    // 消息存储实例，每个主题有一个独立的消息存储
    MessageStore store; // 每个topic都有自己的store, 每个topic都有自己的indexer

    /**
     * 创建一个新的消息队列实例。
     *
     * @param topic 消息队列的主题。
     */
    public MessageQueue(String topic) {
        this.topic = topic;
        store = new MessageStore(topic);
        store.init();
    }

    /**
     * 批量接收消息。
     *
     * @param topic    消息的主题。
     * @param consumerId 消费者的ID。
     * @param size     批量接收的消息数量。
     * @return 消息列表。
     */
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


    /**
     * 统计消息队列的状态。
     *
     * @param topic    消息的主题。
     * @param consumerId 消费者的ID。
     * @return 消息队列的统计信息。
     */
    public static Statistical stat(String topic, String consumerId) {
        MessageQueue queue = queues.get(topic);
        Subscription subscription = queue.subscriptions.get(consumerId);
        return new Statistical(subscription, queue.store.total(), queue.store.pos());
    }

    /**
     * 发送消息。
     *
     * @param message 要发送的消息。
     * @return 消息的偏移量。
     */
    public int send(Message<String> message) {
        // 获取当前存储位置作为消息的偏移量
        int offset = store.pos();
        // 将偏移量记录在消息头中
        message.getHeaders().put("X-offset", String.valueOf(offset));
        // 写入消息到存储
        store.write(message);
        return offset;
    }

    /**
     * 根据偏移量接收消息。
     *
     * @param offset 消息的偏移量。
     * @return 消息对象。
     */
    public Message<?> receive(int offset) {
        return store.read(offset);
    }

    /**
     * 订阅消息。
     *
     * @param subscription 订阅信息。
     */
    public void subscribe(Subscription subscription) {
        // 添加订阅关系
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    /**
     * 取消订阅。
     *
     * @param subscription 订阅信息。
     */
    private void unsubscribe(Subscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    /**
     * 订阅消息的公共接口。
     *
     * @param subscription 订阅信息。
     */
    public static void sub(Subscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> sub: subscription = " + subscription);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    /**
     * 取消订阅消息的公共接口。
     *
     * @param subscription 订阅信息。
     */
    public static void unsub(Subscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: subscription = " + subscription);
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    /**
     * 发送消息的公共接口。
     *
     * @param topic    消息的主题。
     * @param message  要发送的消息。
     * @return 消息的偏移量。
     */
    public static int send(String topic, Message<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println(" ===>> send: topic/message = " + topic + "/" + message);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    /**
     * 根据偏移量接收消息的公共接口。
     *
     * @param topic    消息的主题。
     * @param consumerId 消费者的ID。
     * @param offset   消息的偏移量。
     * @return 消息对象。
     */
    public static Message<?> receive(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.receive(offset);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    /**
     * 接收消息，不传入偏移量。
     *
     * @param topic    消息的主题。
     * @param consumerId 消费者的ID。
     * @return 消息对象。
     */
    public static Message<?> receive(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {

            // 这个offset来源于客户端ack
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                System.out.println(" ===>> receive: start = " + topic + "/" + consumerId + "/" + offset);
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                if (entry == null) return null;
                nextOffset = offset + entry.getLength();
            }

            // 拿到偏移量,再获取数据
            Message<?> receive = messageQueue.receive(nextOffset);
            System.out.println(" ===>> receive: topic/cid/idx = " + topic + "/" + consumerId + "/" + offset);
            System.out.println(" ===>> receive: message = " + receive);
            return receive;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }


    /**
     * 确认消息消费并更新消费者偏移量。
     *
     * @param topic    消息的主题。
     * @param consumerId 消费者的ID。
     * @param offset   消息的偏移量。
     * @return 更新后的偏移量。
     */
    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        if (messageQueue.subscriptions.containsKey(consumerId)) {
            Subscription subscription = messageQueue.subscriptions.get(consumerId);

            // 检查偏移量是否有效并更新订阅的偏移量
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
