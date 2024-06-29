package cn.ipman.mq.core;

/**
 * message consumer.
 * 1种是推
 * 1种是拉取
 *
 * @Author IpMan
 * @Date 2024/6/29 19:55
 */
public class IMConsumer<T> {

    IMBroker broker;
    String topic;
    IMMq mq;

    public IMConsumer(IMBroker broker){
        this.broker = broker;
        mq = broker.find(topic);
        if (mq == null) throw new RuntimeException("topic not found");

    }

    public IMMessage poll(long timeout){
        return mq.poll(timeout);
    }


}
