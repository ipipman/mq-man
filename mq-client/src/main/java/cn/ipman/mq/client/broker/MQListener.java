package cn.ipman.mq.client.broker;

import cn.ipman.mq.metadata.model.Message;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@FunctionalInterface
public interface MQListener<T> {

    /**
     * 当收到消息时调用此方法。
     * <p>
     * 该方法是监听器的核心方法，用于处理接收到的消息。当消息队列中有新消息到达时，将会调用监听器实例的此方法。
     * 实现此方法的监听器需要根据接收到的消息内容执行相应的业务逻辑。
     */
    void onMessage(Message<?> message);
}
