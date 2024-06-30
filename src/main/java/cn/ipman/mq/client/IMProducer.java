package cn.ipman.mq.client;

import cn.ipman.mq.model.IMMessage;

/**
 * 消息队列生产者类。
 * 用于通过消息代理向指定主题发送消息。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:41
 */
public class IMProducer {

    /**
     * 消息代理实例。
     * 用于定位和与消息队列通信。
     */
    IMBroker broker;

    /**
     * 构造方法。
     *
     * @param broker 消息代理实例。
     */
    public IMProducer(IMBroker broker) {
        this.broker = broker;
    }

    /**
     * 向指定主题发送消息。
     *
     * @param topic 主题名称。消息将被发送到此主题。
     * @param message 要发送的消息对象。
     * @return 如果消息发送成功则返回true；否则返回false。
     * @throws RuntimeException 如果找不到指定的主题，则抛出运行时异常。
     */
    public boolean send(String topic, IMMessage<?> message) {
        // 通过消息代理按主题定位消息队列
        IMQueue queue = broker.find(topic);
        if (queue == null) throw new RuntimeException("topic not found");
        // 向找到的消息队列发送消息，并返回发送操作的结果
        return queue.send(message);
    }
}
