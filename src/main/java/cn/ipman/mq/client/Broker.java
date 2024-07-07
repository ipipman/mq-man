package cn.ipman.mq.client;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Result;
import cn.ipman.mq.model.Statistical;
import cn.ipman.mq.utils.HttpUtils;
import cn.ipman.mq.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * 消息代理类，负责管理消息队列并提供生产者与消费者创建方法。
 * 实现了基于主题的消息代理功能，允许创建及查找消息队列。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class Broker {

    /**
     * 默认的消息代理实例。
     */
    @Getter
    public static Broker Default = new Broker();

    /**
     * 消息代理服务的URL。
     */
    public static String brokerUrl = "http://localhost:8765/mq";

    /**
     * 所有消费者的集合。
     */
    final MultiValueMap<String, Consumer<?>> consumers = new LinkedMultiValueMap<>();

    /**
     * 添加消费者到指定主题。
     *
     * @param topic     主题。
     * @param consumer  消费者。
     */
    public void addConsumer(String topic, Consumer<?> consumer) {
        consumers.add(topic, consumer);
    }

    static {
        init();
    }

    /**
     * 初始化消息代理，启动定时任务轮询消费者以处理消息。
     */
    public static void init() {
        // 定时轮询消息队列，并调用监听器处理消息
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, Consumer<?>> consumers = getDefault().consumers;
            // 遍历所有topic下的消费者, 分别取server端获取数据, 并调用监听器处理消息
            consumers.forEach((topic, c) -> c.forEach(consumer -> {
                Message<?> receive = consumer.receive(topic);
                if (receive == null) return;
                try {
                    // 通知监听器处理消息
                    consumer.listener.onMessage(receive);
                    consumer.ack(topic, receive);
                } catch (Exception e) {
                    //todo
                }
            }));
        }, 100, 100);
    }


    /**
     * 创建生产者。
     *
     * @return 生产者实例。
     */
    public Producer createProducer() {
        return new Producer(this);
    }

    /**
     * 创建消费者。
     *
     * @param topic 消费的主题。
     * @return 消费者实例。
     */
    public Consumer<?> createConsumer(String topic) {
        Consumer<?> consumer = new Consumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

    public Consumer<?> createConsumer(String topic, int customCid) {
        Consumer<?> consumer = new Consumer<>(this, customCid);
        consumer.subscribe(topic);
        return consumer;
    }



    /**
     * 发送消息到指定主题。
     *
     * @param topic  消息主题。
     * @param message 消息对象。
     * @return 发送是否成功。
     */
    public boolean send(String topic, Message<?> message) {
        System.out.println(" ==>> send topic/message: " + topic + "/" + message);
        System.out.println(JSON.toJSONString(message));
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> send result: " + result);
        return result.getCode() == 1;
    }

    /**
     * 订阅指定主题。
     *
     * @param topic     主题。
     * @param consumerId 消费者ID。
     */
    public void subscribe(String topic, String consumerId) {
        System.out.println(" ==>> subscribe topic/consumerID: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> subscribe result: " + result);
    }

    /**
     * 接收指定主题的消息。
     *
     * @param topic     主题。
     * @param consumerId 消费者ID。
     * @return 消息对象。
     */
    @SuppressWarnings("unchecked")
    public <T> Message<T> receive(String topic, String consumerId) {
        System.out.println(" ==>> receive topic/cid: " + topic + "/" + consumerId);
        Result<Message<String>> result = HttpUtils.httpGet(brokerUrl + "/receive?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<Message<String>>>() {
                });
        System.out.println(" ==>> receive result: " + result);
        return (Message<T>) result.getData();
    }

    /**
     * 取消订阅指定主题。
     *
     * @param topic     主题。
     * @param consumerId 消费者ID。
     */
    public void unSubscribe(String topic, String consumerId) {
        System.out.println(" ==>> unSubscribe topic/cid: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> unSubscribe result: " + result);
    }

    /**
     * 确认消息消费。
     *
     * @param topic     主题。
     * @param consumerId 消费者ID。
     * @param offset    消息偏移量。
     * @return 确认是否成功。
     */
    public boolean ack(String topic, String consumerId, int offset) {
        System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + consumerId + "/" + offset);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/ack?t=" + topic + "&cid=" + consumerId + "&offset=" + offset,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> ack result: " + result);
        return result.getCode() == 1;
    }


    /**
     * 获取指定主题和消费者ID的统计信息。
     *
     * @param topic     主题。
     * @param consumerId 消费者ID。
     * @return 统计信息对象。
     */
    public Statistical statistical(String topic, String consumerId) {
        System.out.println(" ==>> statistical topic/cid: " + topic + "/" + consumerId);
        Result<Statistical> result = HttpUtils.httpGet(
                brokerUrl + "/stat?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<Statistical>>() {
                });
        System.out.println(" ==>> statistical result: " + result);
        return result.getData();
    }
}
