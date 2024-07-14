package cn.ipman.mq.client.spring.demo;

import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQConsumer;
import cn.ipman.mq.client.broker.MQProducer;
import cn.ipman.mq.metadata.data.Order;
import cn.ipman.mq.metadata.data.User;
import cn.ipman.mq.metadata.model.Message;
import cn.ipman.mq.metadata.model.Statistical;
import cn.ipman.mq.server.MqServerApplication;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest(classes = {MqClientSpringDemoApplication.class},
        properties = {"mq.client.host=127.0.0.1", "mq.client.port=8766"})
class MqClientSpringDemoApplicationTests {

    static ApplicationContext context1;

    @BeforeAll
    static void init() {
        System.out.println(" ====================================== ");
        System.out.println(" ====================================== ");
        System.out.println(" ======    MQ Netty Server 8766     === ");
        System.out.println(" ====================================== ");
        System.out.println(" ====================================== ");
        context1 = SpringApplication.run(MqServerApplication.class,
                "--server.port=9766",
                "--mq.server.port=8766",
                "--mq.server.boss.threads=1",
                "--mq.server.worker.threads=4"
        );
    }

    @Autowired
    MQBroker broker;

    @SneakyThrows
    @Test
    @SuppressWarnings("unchecked")
    void contextLoads() {
        int ids = 0;
        String topic = "cn.ipman.test";

        // 通过broker创建producer和consumer
        MQProducer producer = broker.createProducer();
        // consumer-1
        MQConsumer<?> consumer1 = broker.createConsumer(topic, 1);

        System.out.println("------------------------>");
        Statistical stat = consumer1.statistical(topic);
        System.out.println("===>> Statistical ===>>" + stat);
        System.out.println("------------------------>");

        // ------------ 生产、消费 ------------------
        for (int i = 0; i < 2; i++) {
            User user = new User(ids, "item" + ids);
            producer.send(topic, new Message<>(ids++, JSON.toJSONString(user), null));
            System.out.println("send ok => " + user);
        }

        for (int i = 0; i < 2; i++) {
            Message<User> message = (Message<User>) consumer1.receive(topic);
            System.out.println("poll ok => " + message); // 做业务处理...
            if (message == null) break;
            consumer1.ack(topic, message);
        }

        System.out.println("------------------------>");
        Statistical stat1 = consumer1.statistical(topic);
        System.out.println("===>> Statistical ===>>" + stat1);
        System.out.println("------------------------>");

        //Thread.sleep(3_000);
    }


    @AfterAll
    static void destroy() {
        System.out.println(" ===========     close spring context     ======= ");
        SpringApplication.exit(context1, () -> 1);
    }

}
