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


    public boolean send(String topic, IMMessage<?> message) {
        return broker.send(topic, message);
    }
}
