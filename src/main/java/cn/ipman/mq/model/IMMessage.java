package cn.ipman.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 即时通讯（IM）消息模型类，用于消息传输的数据载体。
 * 本类采用Lombok注解自动生成getter、setter、构造器等模板代码，简化类的定义。
 *
 * @param <T> 消息体类型，支持泛型以增强类的灵活性和实用性。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IMMessage<T> {


    static AtomicLong MID = new AtomicLong(0);

    /**
     * 消息唯一标识符，用于唯一识别每条消息。
     */
    private long id;

    /**
     * 消息体，用于存储消息的实际内容。消息体类型为泛型，支持灵活的内容类型。
     */
    private T body;

    /**
     * 消息头，用于存储关于消息的额外系统信息。
     * 例如，它可以用来存储系统属性如消息版本号,
     */
    private Map<String, String> headers;

    //private String topic;
    //private Map<String, String> properties; // 业务属性

    public static long getId() {
        return MID.getAndIncrement();
    }

    public static IMMessage<String> createMessage(String body, Map<String, String> headers) {
        return new IMMessage<>(getId(), body, headers);
    }

}
