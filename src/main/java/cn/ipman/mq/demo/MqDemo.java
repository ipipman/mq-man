package cn.ipman.mq.demo;

import cn.ipman.mq.client.IMBroker;
import cn.ipman.mq.client.IMConsumer;
import cn.ipman.mq.model.Message;
import cn.ipman.mq.client.IMProducer;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:06
 */
public class MqDemo {

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int ids = 0;

        // 创建broker, 绑定topic
        String topic = "im.order";
        IMBroker broker = new IMBroker();

        // 通过broker创建producer和consumer
        IMProducer producer = broker.createProducer();


//        IMConsumer<?> consumer = broker.createConsumer(topic);
//
//        // consumer定义topic
//        consumer.subscribe(topic);
//
//        // 测试listen监听topic
//        consumer.listen(message -> {
//            System.out.println("onMessage => " + message);
//        });


        IMConsumer<?> consumer1 = broker.createConsumer(topic);
        consumer1.subscribe(topic);

        // ------------ 生产、消费 ------------------
        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
            System.out.println("send ok => " + order);
        }

        for (int i = 0; i < 10; i++) {
            Message<Order> message = (Message<Order>) consumer1.receive(topic);
            System.out.println("poll ok => " + message); // 做业务处理...
            consumer1.ack(topic, message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') { // 退出
                consumer1.unSubscribe(topic);
                break;
            }
            if (c == 'p') { // 生产
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') { // 消费
                Message<Order> message = (Message<Order>) consumer1.receive(topic);
                System.out.println("poll ok => " + message.getBody());
                consumer1.ack(topic, message);
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
