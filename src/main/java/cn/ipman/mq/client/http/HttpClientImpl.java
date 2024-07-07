package cn.ipman.mq.client.http;

import cn.ipman.mq.client.ClientService;
import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.HttpResult;
import cn.ipman.mq.model.Statistical;
import cn.ipman.mq.utils.HttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class HttpClientImpl implements ClientService {

    String brokerUrl = null;

    public HttpClientImpl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * 发送消息到指定主题。
     *
     * @param topic  消息主题。
     * @param message 消息对象。
     * @return 发送是否成功。
     */
    @Override
    public Boolean send(String topic, Message<?> message) {
        System.out.println(" ==>> send topic/message: " + topic + "/" + message);
        System.out.println(JSON.toJSONString(message));
        HttpResult<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<HttpResult<String>>() {
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
    @Override
    public void subscribe(String topic, String consumerId) {
        System.out.println(" ==>> subscribe topic/consumerID: " + topic + "/" + consumerId);
        HttpResult<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<HttpResult<String>>() {
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
    @Override
    public <T> Message<T> receive(String topic, String consumerId) {
        System.out.println(" ==>> receive topic/cid: " + topic + "/" + consumerId);
        HttpResult<Message<String>> result = HttpUtils.httpGet(brokerUrl + "/receive?t=" + topic + "&cid=" + consumerId,
                new TypeReference<HttpResult<Message<String>>>() {
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
    @Override
    public void unSubscribe(String topic, String consumerId) {
        System.out.println(" ==>> unSubscribe topic/cid: " + topic + "/" + consumerId);
        HttpResult<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<HttpResult<String>>() {
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
    @Override
    public Boolean ack(String topic, String consumerId, int offset) {
        System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + consumerId + "/" + offset);
        HttpResult<String> result = HttpUtils.httpGet(
                brokerUrl + "/ack?t=" + topic + "&cid=" + consumerId + "&offset=" + offset,
                new TypeReference<HttpResult<String>>() {
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
    @Override
    public Statistical statistical(String topic, String consumerId) {
        System.out.println(" ==>> statistical topic/cid: " + topic + "/" + consumerId);
        HttpResult<Statistical> result = HttpUtils.httpGet(
                brokerUrl + "/stat?t=" + topic + "&cid=" + consumerId,
                new TypeReference<HttpResult<Statistical>>() {
                });
        System.out.println(" ==>> statistical result: " + result);
        return result.getData();
    }
}
