package cn.ipman.mq.client.broker;

import lombok.Data;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * MQ监听器端点类，用于封装MQ消息监听的相关信息。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Data
public class MQListenerEndpoint {

    /**
     * 监听方法，当有消息到达时，会调用此方法进行处理。
     */
    private Method method;

    /**
     * 监听方法所属的bean对象，即调用监听方法的实际对象。
     */
    private Object bean;

    /**
     * 监听的topic列表，一个监听器可以监听多个topic的消息。
     */
    private String[] topic;

    /**
     * MQ消费者映射，用于存储不同类型的MQ消费者。
     * Key为消费者标识，Value为具体的MQ消费者对象。
     */
    private Map<String, MQConsumer<?>> consumerMap = new HashMap<>();

}
