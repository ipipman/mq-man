package cn.ipman.mq.client.config;


import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQProducer;
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
public class ProducerConfig {

    @Bean
    public MQProducer producerFactory(@Autowired MQBroker broker){
        return broker.createProducer();
    }

}
