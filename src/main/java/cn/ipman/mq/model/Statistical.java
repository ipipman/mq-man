package cn.ipman.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/7 18:55
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Statistical {

    private Subscription subscription; // 当前消费者在topic中的消费关系,如:consumerId, offset
    private int total;      // topic 种消息的数量
    private int position;   // topic 文件的最新写入的位置

}
