package cn.ipman.mq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics.
 *
 * @Author IpMan
 * @Date 2024/6/29 19:44
 */
public class IMBroker {

    Map<String, IMmq> mqMapping = new ConcurrentHashMap<>(64);

    public IMmq find(String topic) {
        return mqMapping.get(topic);
    }

    public IMmq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new IMmq(topic));
    }

}
