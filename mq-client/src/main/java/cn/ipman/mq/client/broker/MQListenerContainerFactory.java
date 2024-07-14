package cn.ipman.mq.client.broker;


import java.util.Arrays;

/**
 * MQ监听器容器工厂类，负责根据提供的MQBroker实例配置和管理MQ监听器。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class MQListenerContainerFactory {

    // MQBroker实例，用于创建消费者和管理订阅
    MQBroker broker;

    /**
     * 构造函数，初始化MQ监听器容器工厂。
     *
     * @param broker MQBroker实例
     */
    public MQListenerContainerFactory(MQBroker broker) {
        this.broker = broker;
    }

    /**
     * 注册MQ监听器，为指定的主题创建消费者并订阅消息。
     *
     * @param endpoint MQ监听器端点，包含监听器的配置信息，如主题、处理方法和绑定的Bean。
     */
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
