package cn.ipman.mq.demo;

import cn.ipman.mq.broker.MQBroker;
import cn.ipman.mq.broker.MQConsumer;
import cn.ipman.mq.broker.MQProducer;
import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:06
 */
public class ConsumerDemo2 {

    static int count = 0;

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int ids = 0;

        // 创建broker, 绑定topic
        String topic = "im.order";
        MQBroker broker = MQBroker.getDefault();

        // 通过broker创建producer和consumer
        MQProducer producer = broker.createProducer();

        // consumer-0
        MQConsumer<?> consumer = broker.createConsumer(topic, 3);
        // 测试listen监听topic


        consumer.addListen(topic, message -> {
            System.out.println("listener onMessage => " + message);
            System.out.println(count++);
        });



//        // ------------ 生产、消费 ------------------
//        for (int i = 0; i < 10; i++) {
//            Order order = new Order(ids, "item" + ids, 100 * ids);
//            producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
//            System.out.println("send ok => " + order);
//        }
    }
}
