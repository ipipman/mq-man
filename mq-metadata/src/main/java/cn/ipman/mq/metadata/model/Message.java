package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 即时通讯消息模型类，用于封装消息数据。
 * 采用Lombok注解简化了构造方法、getter和setter的生成，提高了代码的可读性和简洁性。
 * 该类支持泛型，使得消息体可以是任意类型，增加了类的通用性和灵活性。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message<T> {

    // 使用AtomicLong来保证消息ID的线程安全自增
    // 消息ID
    static AtomicLong MID = new AtomicLong(0);

    /**
     * 消息的唯一标识符，由类静态方法nextId生成，保证全局唯一。
     */
    private long id;

    /**
     * 消息体，存储消息的实际内容，可以是任意类型。
     */
    private T body;

    /**
     * 消息头，用于存储额外的消息元数据或属性。
     */
    private Map<String, String> headers = new HashMap<>();

    //private String topic;
    //private Map<String, String> properties; // 业务属性

    /**
     * 获取下一个消息ID，用于生成新消息的唯一标识符。
     *
     * @return 下一个消息ID
     */
    public static long nextId() {
        return MID.getAndIncrement();
    }

    /**
     * 创建并返回一个新的消息对象，使用给定的消息体和消息头初始化。
     *
     * @param body    消息体
     * @param headers 消息头
     * @return 新创建的消息对象
     */
    public static Message<?> createMessage(String body, Map<String, String> headers) {
        return new Message<>(nextId(), body, headers);
    }

}
