package cn.ipman.mq.model;

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
public class Result<T> {

    private int code;
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<Message<?>> msg(String message) {
        return new Result<>(1, Message.createMessage(message, null));
    }

    public static Result<Message<?>> msg(Message<?> message) {
        return new Result<>(1, message);
    }

    public static Result<List<Message<?>>> msg(List<Message<?>> messages) {
        return new Result<>(1, messages);
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<Statistical> stat(Statistical statistical) {
        return new Result<>(1, statistical);
    }
}
