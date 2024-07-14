package cn.ipman.mq.client.spring.demo.sample;

import cn.ipman.mq.client.broker.MQBroker;
import cn.ipman.mq.client.broker.MQConsumer;
import cn.ipman.mq.client.broker.MQProducer;
import cn.ipman.mq.metadata.demo.Order;
import cn.ipman.mq.metadata.model.Message;
import cn.ipman.mq.metadata.model.Statistical;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:06
 */
public class ConsumerDemo1 {

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int ids = 0;

        // 创建broker, 绑定topic
        String topic = "im.order";
        MQBroker broker = MQBroker.getDefault();

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
        System.out.println("===>>===>>===>>===>>===>> " );

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') { // 退出
                consumer1.unSubscribe(topic);
                System.exit(0);
            }
            if (c == 'p') { // 生产
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') { // 消费
                Message<Order> message = (Message<Order>) consumer1.receive(topic);
                if (message == null) continue;
                System.out.println("poll ok => " + message.getBody());
                consumer1.ack(topic, message);
            }
            if (c == 's') {
                Statistical stat = consumer1.statistical(topic);
                System.out.println(stat);
            }
            if (c == 'a') { // 生产10个
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                }
                System.out.println("send 10 orders... ");
            }
        }
    }
}
