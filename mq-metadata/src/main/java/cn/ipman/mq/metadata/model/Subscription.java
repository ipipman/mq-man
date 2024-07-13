package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message Subscription class.
 * 用于表示消息订阅关系，包含订阅的主题、消费者ID以及消费者的消费位点。
 *
 * @Author IpMan
 * @Date 2024/6/30 20:29
 */
@Data
@AllArgsConstructor
public class Subscription {

    /**
     * 订阅的主题。
     * 主题是消息的分类，消费者根据主题订阅感兴趣的消息。
     */
    private String topic;

    /**
     * 消费者的ID。
     * 用于唯一标识一个消费者，消费者通过消费者ID获取和保存自己的消费位点。
     */
    private String consumerId;

    /**
     * 消费者的消费位点。
     * 消费位点表示消费者已经消费到的消息的偏移量，初始值为-1，表示尚未开始消费。
     * 消费者根据消费位点从消息队列中拉取新消息。
     */
    private int offset = -1;


}
