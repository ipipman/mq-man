package cn.ipman.mq.client.broker;

import cn.ipman.mq.client.client.ClientService;
import cn.ipman.mq.client.client.netty.NettyClientImpl;
import cn.ipman.mq.metadata.model.Message;
import cn.ipman.mq.metadata.model.Statistical;
import cn.ipman.mq.metadata.utils.ThreadUtils;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * 消息代理类，负责管理消息队列并提供生产者与消费者创建方法。
 * 实现了基于主题的消息代理功能，允许创建及查找消息队列。
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class MQBroker {

    /**
     * 默认的消息代理实例。
     */
    @Getter
    public static MQBroker Default = new MQBroker();

    public ClientService clientService;

    public MQBroker() {
        this.clientService = new NettyClientImpl(
                "127.0.0.1", 8765, 10, 5, 2);
    }

    public MQBroker(ClientService clientService) {
        this.clientService = clientService;
    }


    /**
     * 所有消费者的集合。
     */
    final MultiValueMap<String, MQConsumer<?>> consumers = new LinkedMultiValueMap<>();

    /**
     * 添加消费者到指定主题。
     *
     * @param topic    主题。
     * @param consumer 消费者。
     */
    public void addConsumer(String topic, MQConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }


    /**
     * 初始化消息代理，启动定时任务轮询消费者以处理消息。
     */
    public void init() {
        // 定时轮询消息队列，并调用监听器处理消息
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            // 遍历所有topic下的消费者, 分别取server端获取数据, 并调用监听器处理消息
            consumers.forEach((topic, c) -> c.forEach(consumer -> {
                // 消费数据
                Message<?> receive = consumer.receive(topic);
                if (receive == null) return;
                try {
                    // 通知监听器处理消息
                    consumer.listener.onMessage(receive);
                    consumer.ack(topic, receive);
                } catch (Exception e) {
                    //todo retry
                }
            }));
        }, 100, 100);
    }


    /**
     * 创建生产者。
     *
     * @return 生产者实例。
     */
    public MQProducer createProducer() {
        return new MQProducer(this);
    }

    /**
     * 创建消费者。
     *
     * @param topic 消费的主题。
     * @return 消费者实例。
     */
    @SuppressWarnings("unused")
    public MQConsumer<?> createConsumer(String topic) {
        MQConsumer<?> consumer = new MQConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }


    /**
     * 创建一个消息消费者，用于订阅指定的主题。
     *
     * @param topic 消息的主题，指定消费者将订阅的消息类型。
     * @param customCid 客户端自定义的消费者组ID，用于标识一组消费者实例，同组内的消费者将共享消息的消费。
     * @return 返回创建的消费者实例，该实例已配置好订阅的主题和消费者组ID。
     */
    public MQConsumer<?> createConsumer(String topic, int customCid) {
        MQConsumer<?> consumer = new MQConsumer<>(this, customCid);
        consumer.subscribe(topic);
        return consumer;
    }


    /**
     * 发送消息到指定主题。
     *
     * @param topic   消息主题。
     * @param message 消息对象。
     * @return 发送是否成功。
     */
    public boolean send(String topic, Message<?> message) {
        return clientService.send(topic, message);
    }

    /**
     * 订阅指定主题。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     */
    public void subscribe(String topic, String consumerId) {
        clientService.subscribe(topic, consumerId);
    }

    /**
     * 接收指定主题的消息。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @return 消息对象。
     */
    public <T> Message<T> receive(String topic, String consumerId) {
        return clientService.receive(topic, consumerId);
    }

    /**
     * 取消订阅指定主题。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     */
    public void unSubscribe(String topic, String consumerId) {
        clientService.unSubscribe(topic, consumerId);
    }

    /**
     * 确认消息消费。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @param offset     消息偏移量。
     * @return 确认是否成功。
     */
    public boolean ack(String topic, String consumerId, int offset) {
        return clientService.ack(topic, consumerId, offset);
    }


    /**
     * 获取指定主题和消费者ID的统计信息。
     *
     * @param topic      主题。
     * @param consumerId 消费者ID。
     * @return 统计信息对象。
     */
    public Statistical statistical(String topic, String consumerId) {
        return clientService.statistical(topic, consumerId);
    }
}
