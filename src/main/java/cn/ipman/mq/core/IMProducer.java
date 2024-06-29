package cn.ipman.mq.core;

/**
 * message queue producer.
 *
 * @Author IpMan
 * @Date 2024/6/29 19:41
 */
public class IMProducer {

    IMBroker broker;

    public IMProducer(IMBroker broker) {
        this.broker = broker;
    }

    public boolean send(String topic, IMMessage<?> message) {
        IMmq mq = broker.find(topic);
        if (mq == null) throw new RuntimeException("topic not found");
        return mq.send(message);
    }
}
