package cn.ipman.mq.broker;


import java.util.Arrays;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class MQListenerContainerFactory {

    MQBroker broker;

    public MQListenerContainerFactory(MQBroker broker) {
        this.broker = broker;
    }

    public void registryListener(MQListenerEndpoint endpoint) {
        Arrays.stream(endpoint.getTopic()).forEach(topic -> {
            // created and subscribe
            MQConsumer<?> consumer = broker.createConsumer(topic);
            endpoint.getConsumerMap().putIfAbsent(topic, consumer);
            // 添加订阅者
            consumer.addListen(topic, message -> {
                try {
                    // 业务处理
                    endpoint.getMethod().invoke(endpoint.getBean(), message);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
        });
    }

}
