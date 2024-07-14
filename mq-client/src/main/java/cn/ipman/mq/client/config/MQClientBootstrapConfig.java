package cn.ipman.mq.client.config;

import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQListenerContainerFactory;
import cn.ipman.mq.client.broker.MQProducer;
import cn.ipman.mq.client.client.ClientService;
import cn.ipman.mq.client.client.netty.NettyClientImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * MQ客户端配置类，用于配置客户端服务、Broker、Producer和监听器容器工厂。
 *
 * @Author IpMan
 * @Date 2024/7/14 08:47
 */
@Configuration
@Import({MQConfigProperties.class})
public class MQClientBootstrapConfig {

    /**
     * MQ配置属性对象，用于获取配置信息。
     */
    @Autowired
    private MQConfigProperties mqConfigProperties;

    /**
     * 配置客户端服务。
     *
     * @return 客户端服务实例。
     */
    @Bean
    public ClientService clientService() {
        // 使用NettyClientImpl实现客户端服务，并传入MQ配置属性中的连接信息。
        return new NettyClientImpl(
                mqConfigProperties.getHost(),
                mqConfigProperties.getPort(),
                mqConfigProperties.getPoolMaxTotal(),
                mqConfigProperties.getPoolMaxIdle(),
                mqConfigProperties.getPoolMinIdle()
        );
    }

    /**
     * 配置MQBroker实例。
     * 使用initMethod指定初始化方法为init，确保Broker在使用前正确初始化。
     *
     * @param clientService 客户端服务实例，用于Broker内部通信。
     * @return MQBroker实例。
     */
    @Bean(initMethod = "init")
    public MQBroker brokerFactory(@Autowired ClientService clientService) {
        // 使用客户端服务创建MQBroker实例。
        return new MQBroker(clientService);
    }

    /**
     * 配置MQProducer实例。
     *
     * @param broker MQBroker实例，用于创建Producer。
     * @return MQProducer实例。
     */
    @Bean
    public MQProducer producerFactory(@Autowired MQBroker broker) {
        // 使用Broker创建Producer。
        return broker.createProducer();
    }

    /**
     * 配置MQ监听器容器工厂。
     *
     * @param broker MQBroker实例，用于监听消息。
     * @return MQ监听器容器工厂实例。
     */
    @Bean
    public MQListenerContainerFactory listenerContainerFactory(@Autowired MQBroker broker) {
        // 使用Broker创建监听器容器工厂。
        return new MQListenerContainerFactory(broker);
    }

}
