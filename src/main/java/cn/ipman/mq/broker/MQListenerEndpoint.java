package cn.ipman.mq.broker;

import lombok.Data;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Data
public class MQListenerEndpoint {

    private Method method;
    private Object bean;
    private String[] topic;
    private Map<String, MQConsumer<?>> consumerMap = new HashMap<>();

}
