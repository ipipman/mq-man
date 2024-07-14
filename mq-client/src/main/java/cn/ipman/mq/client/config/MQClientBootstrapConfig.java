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
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/14 08:47
 */
@Configuration
@Import({MQConfigProperties.class})
public class MQClientBootstrapConfig {

    @Autowired
    private MQConfigProperties mqConfigProperties;

    @Bean
    public ClientService clientService() {
        return new NettyClientImpl(
                mqConfigProperties.getHost(),
                mqConfigProperties.getPort(),
                mqConfigProperties.getPoolMaxTotal(),
                mqConfigProperties.getPoolMaxIdle(),
                mqConfigProperties.getPoolMinIdle()
        );
    }

    @Bean(initMethod = "init")
    public MQBroker brokerFactory(@Autowired ClientService clientService) {
        return new MQBroker(clientService);
    }

    @Bean
    public MQProducer producerFactory(@Autowired MQBroker broker) {
        return broker.createProducer();
    }

    @Bean
    public MQListenerContainerFactory listenerContainerFactory(@Autowired MQBroker broker) {
        return new MQListenerContainerFactory(broker);
    }

}
