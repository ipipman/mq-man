package cn.ipman.mq.client.spring.demo;

import cn.ipman.mq.client.annotation.EnableMqMan;
import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQConsumer;
import cn.ipman.mq.client.broker.MQProducer;
import cn.ipman.mq.metadata.data.Order;
import cn.ipman.mq.metadata.model.Message;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableMqMan // 开启MQ
public class MqClientSpringDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqClientSpringDemoApplication.class, args);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public ApplicationRunner runner(@Autowired ApplicationContext context) {
        return x -> {
            int ids = 0;
            String topic = "im.order";

            MQBroker broker = context.getBean(MQBroker.class);
            // 通过broker创建producer和consumer
            MQProducer producer = broker.createProducer();
            // consumer-1
            MQConsumer<?> consumer1 = broker.createConsumer(topic, 1);

            // ------------ 生产、消费 ------------------
            for (int i = 0; i < 200; i++) {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                System.out.println("send ok => " + order);
            }

            for (int i = 0; i < 10; i++) {
                Message<Order> message = (Message<Order>) consumer1.receive(topic);
                System.out.println("poll ok => " + message); // 做业务处理...
                consumer1.ack(topic, message);
            }

            System.out.println("===>>===>>===>>===>>===>> ");
        };
    }
}
