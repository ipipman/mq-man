package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Result for MQServer.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:36
 */
@AllArgsConstructor
@Data
public class HttpResult<T> {

    private int code;
    private T data;

    public static HttpResult<String> ok() {
        return new HttpResult<>(1, "OK");
    }

    public static HttpResult<Message<?>> msg(String message) {
        return new HttpResult<>(1, Message.createMessage(message, null));
    }

    public static HttpResult<Message<?>> msg(Message<?> message) {
        return new HttpResult<>(1, message);
    }

    public static HttpResult<List<Message<?>>> msg(List<Message<?>> messages) {
        return new HttpResult<>(1, messages);
    }

    public static HttpResult<String> ok(String msg) {
        return new HttpResult<>(1, msg);
    }

    public static HttpResult<Statistical> stat(Statistical statistical) {
        return new HttpResult<>(1, statistical);
    }
}
