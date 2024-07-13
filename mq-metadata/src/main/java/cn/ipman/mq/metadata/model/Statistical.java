package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 统计信息类，用于记录和传递消息统计相关的信息。
 * 包括订阅信息、消息总数和当前消息位置等。
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Statistical {

    /**
     * 订阅信息，包含消费者的订阅关系和当前位置等。
     * 例如，消费者ID和消费位点。
     */
    private Subscription subscription;

    /**
     * 总消息数量，表示该主题或队列中的消息总数。
     */
    private int total;

    /**
     * 当前消息位置，表示最新写入的消息在文件或队列中的位置。
     * 用于跟踪和管理消息的消费进度。
     */
    private int position;

}
