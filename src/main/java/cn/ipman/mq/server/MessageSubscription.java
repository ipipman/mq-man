package cn.ipman.mq.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;

/**
 * Message Subscription.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:29
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;  // 每个 consumer 都有自己的消费位置


}
