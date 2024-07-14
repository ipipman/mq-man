package cn.ipman.mq.client.config;

import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQListenerContainerFactory;
import cn.ipman.mq.client.broker.MQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/14 08:47
 */
@Configuration
public class MQClientConfig {

    @Bean
    public MQBroker brokerFactory() {
        return MQBroker.getDefault();
    }

    @Bean
    public MQProducer producerFactory(@Autowired MQBroker broker){
        return broker.createProducer();
    }

    @Bean
    public MQListenerContainerFactory listenerContainerFactory(@Autowired MQBroker broker) {
        return new MQListenerContainerFactory(broker);
    }

}