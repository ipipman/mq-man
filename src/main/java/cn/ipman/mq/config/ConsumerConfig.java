package cn.ipman.mq.config;

import cn.ipman.mq.broker.MQBroker;
import cn.ipman.mq.broker.MQListenerContainerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Configuration
public class ConsumerConfig {

    @Bean
    public MQListenerContainerFactory listenerContainerFactory(@Autowired MQBroker broker) {
        return new MQListenerContainerFactory(broker);
    }

}
