package cn.ipman.mq.client.client;


import cn.ipman.mq.metadata.model.Message;
import cn.ipman.mq.metadata.model.Statistical;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public interface ClientService {

    /**
     * 发送消息到指定主题。
     *
     * @param topic   消息主题。
     * @param message 消息对象。
     * @return 发送是否成功。
     */
    Boolean send(String topic, Message<?> message);


    /**
     * 订阅指定主题。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     */
    void subscribe(String topic, String consumerId);

    /**
     * 接收指定主题的消息。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @return 消息对象。
     */
    <T> Message<T> receive(String topic, String consumerId);

    /**
     * 取消订阅指定主题。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     */
    void unSubscribe(String topic, String consumerId);

    /**
     * 确认消息消费。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @param offset     消息偏移量。
     * @return 确认是否成功。
     */
    Boolean ack(String topic, String consumerId, int offset);

    /**
     * 获取指定主题和消费者ID的统计信息。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @return 统计信息对象。
     */
    Statistical statistical(String topic, String consumerId);
}
