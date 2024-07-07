package cn.ipman.mq.demo;

import cn.ipman.mq.client.Broker;
import cn.ipman.mq.client.Consumer;
import cn.ipman.mq.client.Producer;
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

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int ids = 0;

        // 创建broker, 绑定topic
        String topic = "im.order";
        Broker broker = Broker.getDefault();

        // 通过broker创建producer和consumer
        Producer producer = broker.createProducer();

        // consumer-0
        Consumer<?> consumer = broker.createConsumer(topic, 2);
        // 测试listen监听topic
        consumer.listen(topic, message -> {
            System.out.println("listener onMessage => " + message);
        });

        // ------------ 生产、消费 ------------------
        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
            System.out.println("send ok => " + order);
        }
    }
}
