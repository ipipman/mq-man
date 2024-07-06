package cn.ipman.mq.client;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Result;
import cn.ipman.mq.utils.HttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

/**
 * 消息代理类，负责管理消息队列并提供生产者与消费者创建方法。
 * 实现了基于主题的消息代理功能，允许创建及查找消息队列。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class IMBroker {

    public static String brokerUrl = "http://localhost:8765/mq";

    /**
     * 创建一个新的生产者实例。
     *
     * @return 新的IMProducer实例
     */
    public IMProducer createProducer() {
        return new IMProducer(this);
    }

    /**
     * 创建一个新的消费者实例并订阅指定主题。
     *
     * @param topic 订阅的主题
     * @return 新的IMConsumer实例
     */
    public IMConsumer<?> createConsumer(String topic) {
        IMConsumer<?> consumer = new IMConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

    public boolean send(String topic, Message<?> message) {
        System.out.println(" ==>> send topic/message: " + topic + "/" + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> send result: " + result);
        return result.getCode() == 1;
    }

    public void subscribe(String topic, String consumerId) {
        System.out.println(" ==>> subscribe topic/consumerID: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> subscribe result: " + result);
    }

    @SuppressWarnings("unchecked")
    public <T> Message<T> receive(String topic, String consumerId) {
        System.out.println(" ==>> receive topic/cid: " + topic + "/" + consumerId);
        Result<Message<String>> result = HttpUtils.httpGet(brokerUrl + "/receive?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<Message<String>>>() {
                });
        System.out.println(" ==>> receive result: " + result);
        return (Message<T>) result.getData();
    }

    public void unSubscribe(String topic, String consumerId) {
        System.out.println(" ==>> unSubscribe topic/cid: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> unSubscribe result: " + result);
    }


    public boolean ack(String topic, String consumerId, int offset) {
        System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + consumerId + "/" + offset);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/ack?t=" + topic + "&cid=" + consumerId + "&offset=" + offset,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ==>> ack result: " + result);
        return result.getCode() == 1;
    }

}
