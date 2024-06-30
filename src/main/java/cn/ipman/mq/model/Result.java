package cn.ipman.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

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

    public static Result<IMMessage<?>> msg(String message) {
        return new Result<>(1, IMMessage.createMessage(message, null));
    }

    public static Result<IMMessage<?>> msg(IMMessage<?> message) {
        return new Result<>(1, message);
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }
}
